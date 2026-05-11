"""
Implementations of the presersvation task framework that are specific to the NIST Public Data Repository 
and how it handles preservation.
"""

import os, re, time, logging, shutil
from logging import Logger
from pathlib import Path
from collections import OrderedDict
from typing import List, Mapping
from copy import deepcopy

from .. import framework as fw
from nistoar.pdr.preserve.bagit import BagBuilder, BagWriteError, BagItException
from nistoar.base.config import ConfigurationException, merge_config
import nistoar.pdr.preserve.bagit.utils as bagutils
from nistoar.pdr.preserve import PreservationStateError, system as preserve_system
from nistoar.pdr.preserve.bagit.utils import parse_bag_name, find_latest_head_bag
import nistoar.id.versions as verutils
from nistoar.pdr.ingest import RMMIngestClient, DOIMintingClient, NotValidForIngest
from nistoar.pdr.distrib import (BagDistribClient, RESTServiceClient,
                                 DistribResourceNotFound, DistribServiceException)
from ..validate import *
from ..serialize import *

DEF_MBAG_VERSION = bagutils.DEF_MBAG_VERSION

class PDRBagFinalization(fw.AIPFinalization):
    """
    An implementation of the AIP finalization step that applies some last-minute tweaks to the submitted 
    AIP bag before validation and serialization.  It also extracts key metadata products that will be 
    part of the final publishing step.  

    This implementation supports the following configuration parameters:

    ``allow_replace``  
           (bool) allow an AIP to replace a previously preserved AIP with the same version number
           (default: False).  Use with caution.
    ``repo_access``  
           a configuration dictionary that describe access points to the public side of the 
           repository.  This is used to interrogate details of previous publications of a
           dataset.  
    ``bag_builder``  
           a configuration dictionary for configuring the 
           :py:class:`~nistoar.pdr.preserve.bagit.builder.BagBuilder` instance that will be 
           used to make the final updates.
    ``ingest`` 
           (dict) a configuration dictionary for the ingest functions that require preparation.
           recognized keys include ``rmm`` and ``doi`` (see more detail below).

    The ``ingest`` configuration dictionary is also used by the PDRPublication class; the parameters 
    used here include:

    ``doi`` 
           a configuration dictionary for configuring the DOI minting client used to stage the 
           DataCite record
    ``rmm``
           a configuration dictionary for configuring the ingest client used to stage the 
           NERDm record
    """

    def __init__(self, config=None):
        """
        instantiate the finalization step

        :param dict config:  the configuration for this step; if not provided, defaults will apply.
        """
        if config is None:
            config = {}
        self.cfg = config

        if not self.cfg.get("repo_access", {}).get("distrib_service", {}).get("service_endpoint"):
            raise ConfigurationException("Missing required configuration: "+
                                         "repo_access.distrib_service.service_endpoint")

        icfg = self.cfg.get('ingest', {})
        self._ingester = None
        ingcfg = icfg.get('rmm')
        if ingcfg and ingcfg.get('service_endpoint'):
            self._ingester = RMMIngestClient(ingcfg)

        self._doiminter = None
        dmcfg = icfg.get('doi')
        if dmcfg and dmcfg.get('datacite_api'):
            self._doiminter = DOIMintingClient(dmcfg)


    def apply(self, statemgr: fw.PreservationStateManager):
        """
        apply the finalization step.  This implementation will give the original bag a new name.
        """
        log = statemgr.log.getChild("finalization")

        aipid = statemgr.aipid
        bagdir = Path(statemgr.get_sip())
        if bagdir is None:
            raise fw.AIPFinalizationException("Initial AIP is not set via state manager", aipid)
        if not bagdir.is_dir():
            raise fw.AIPFinalizationException(f"Initial AIP is not an existing directory: {bagdir}",
                                              aipid)

        statemgr.record_progress("Finalizing the AIP bag")

        # start by determining the sequence number; if this fails, we shouldn't go on
        repo = RepositoryAccess(self.cfg.get('repo_access'), log)
        lastver = None
        lastseq = -1
        try:
            latestbag = repo.latest_headbag(aipid)
            if latestbag:
                parts = bagutils.parse_bag_name(latestbag)
                lastver = verutils.OARVersion(parts[1])
                lastseq = int(parts[3])
        except ValueError as ex:
            raise fw.AIPFinalizationException("Unexpected error interpreting previously published AIP "+
                                              f"name: {str(latestbag)}: {str(ex)}", aipid)
        # except repoaccess error

        bldr = BagBuilder(str(bagdir.parents[0]), str(bagdir.name), self.cfg.get("bag_builder"),
                          statemgr.aipid, log)
        try:
            bldr.ensure_bagdir()
            bldr.record("Beginning preservation")

            # check version.  New version should already be set in bag.
            newver = bldr.bag.nerd_metadata_for("", True).get("version")
            if not newver:
                raise fw.AIPFinalizationException(f"{aipid}: Version not set in AIP")
            newver = verutils.OARVersion(newver)
            if newver <= lastver:
                if self.cfg.get("allow_replace", False):
                    if newver < lastver:
                        raise fw.AIPFinalizationException("f{aipid}: {newver}: Can't replace "+
                                                          f"version earlier than last version ({lastver})")
                    log.warning("Preservation set to replace previous published version: %s", newver)
                else:
                    raise fw.AIPFinalizationException(
                        f"{aipid}: {newver} AIP version already published; won't replace!"
                    )
                        

            # check sequence number
            newseq = lastseq + 1

            # rename the bag based on version/seq. #
            newname = self.form_bag_name(aipid, newseq, str(newver))
            newdir = os.path.join(bldr._pdir, newname)
            if os.path.exists(newdir):
                raise fw.AIPFinalizationException(f"{aipid}: Unable to rename input bag: "+
                                                  "destination name already exists!", aipid)
        except BagItException as ex:
            raise fw.AIPFinalizationException(f"{aipid}: Problem accessing input bag, {bagdir}: " +
                                              str(ex)) from ex

        if newname != bagdir.name:
            try:
                bldr.done()
                bagdir.rename(newdir)
                statemgr.set_finalized_aip(newdir)
            except OSError as ex:
                raise fw.AIPFinalizationException(f"{aipid}: Unable to rename {bagdir} to {newdir}: " +
                                                  str(ex)) from ex
            bagdir = Path(newdir)
            bldr = BagBuilder(str(bagdir.parents[0]), str(bagdir.name), self.cfg.get("bag_builder"),
                              statemgr.aipid, log)
            bldr.ensure_bagdir()

        # now finalize on newly renamed bag
        try:
            bldr.finalize_bag(self.cfg.get("bag_builder", {}).get("finalize", {}))
            bldr.done()
        except BagItException as ex:
            raise AIPFinalizationException("Failed to finalize AIP bag: "+str(ex)) from ex

        # stage NERDm record for publishing
        nerdm = bldr.bag.nerdm_record()
        statemgr.set_state_property("nerdm:version", nerdm.get("version", "0"))
        if self._ingester:
            try:
                self._ingester.stage(nerdm, aipid)
            except Exception as ex:
                msg = f
                log.exception("Failure staging NERDm record for %s for ingest: %s", aipid, str(ex))
        else:
            log.warning("Ingester client not configured: archived records will not get loaded to repo")

        # stage DataCite record for DOI minting/updating
        if not self._doiminter:
            log.warning("DOI minting client not configured: archived records will not get submitted "+
                        "to DataCite")
        if 'doi' not in nerdm:
            log.warning("No DOI assigned to aip=%s; skipping Datacite submission", aipid)
        else:
            try:
                self._doiminter.stage(nerdm, name=aipid)
            except Exception as ex:
                log.exception("Failure staging DataCite record for %s for DOI minting/updating: %s",
                              aipid, str(ex))

        statemgr.mark_completed(statemgr.FINALIZED, "AIP bag finalization completed")


    def revert(self, statemgr: fw.PreservationStateManager):
        log = statemgr.log.getChild("finalization").getChild("revert")
        didstuff = False
        finalized = statemgr.get_finalized_aip()
        if finalized and os.path.exists(finalized):
            statemgr.set_finalized_aip(None)
            try:
                orig = statemgr.get_sip()
                if orig and os.path.exists(orig):
                    log.warning("Original AIP (unexpectedly) exists; deleting previously finalized version")
                    if os.path.is_dir(finalized):
                        shutil.rmtree(finalized)
                    else:
                        log.warning("Finalized AIP is unexpectedly a file; removing anyway")
                        os.unlink(finalized)

                else:
                    os.rename(finalized, orig)

                didstuff = True
                
            except OSError as ex:
                raise AIPFinalizationException("Failed to revert finalized bag back to original: "+str(ex)) \
                    from ex

        if self._ingester and self._ingester.is_staged(statemgr.aipid):
            self._ingester.clear(statemgr.aipid)
            didstuff = True
        if self._doiminter and self._doiminter.is_staged(statemgr.aipid):
            self._doiminter.clear(statemgr.aipid)
            didstuff = True

        return didstuff
            
                

    # apply:
    #  *  determine sequence number
    #  *  finalize log, provenance info
    #  *  finalize baginfo.txt
    #  *  rename bag
    #  *  stage nerdm record
    #  *  stage datacite record

    # revert:
    #  *  unstage nerdm, datacite records
    #  *  rename bag back to original name

    # cleanup:
    #  *  ?

    def form_bag_name(self, aipid, bagseq=0, aipver="1.0"):
        """
        return the name to use for the working bag directory.  According to the
        NIST BagIt Profile, preservation bag names will follow the format
        AIPID.AIPVER.mbagMBVER-SEQ

        :param str  aipid:   the AIP identifier for the dataset
        :param int  bagseq:  the multibag sequence number to assign (default: 0)
        :param str  aipver:  the dataset's release version string.  (default: 1.0)
        """
        fmt = self.cfg.get('bag_name_format')
        bver = self.cfg.get('mbag_version', DEF_MBAG_VERSION)
        return bagutils.form_bag_name(aipid, bagseq, aipver, bver, namefmt=fmt)

class PDR1AIPArchiving(fw.AIPArchiving):
    """
    An implementation of the AIP archiving interface appropriate for data in the PDR.  

    In this implementation, the AIP files are moved from a staging directory to a transfer directory.
    Once in place there, another independent process is expected replicate the data into long-term 
    storage in S3 which could take minutes or days to complete.  This implementation will wait until 
    all files appear in the final bucket via a sleep-poll loop.

    This implementation supports the following configuration parameters:

    ``store_dir``   
         (str) the storage transfer directory that files are copied to 
    ``public_bucket``
         (str) the S3 address representing the final storage bucket where the files should
         eventually arrive to be considered complete.  
    ``allow_overwrite``  
         (bool) If False (default) do not allow pre-existing files with the same names as
         any being transfered from that staging area to be replaced; if any such files are 
         found in the transfer directory, the preservation step will fail.  If True, such files
         will be replaced.  
    ``polling``
         (dict) a configuration dictionary that describes how the sleep-polling cycle evolves
         over time.
    """
    cksexts = ["sha256"]

    def __init__(self, config=None):
        """
        instantiate the archiving step
        :param dict config:  the configuration for this step; if not provided, defaults will apply.
        """
        if config is None:
            config = {}
        self.cfg = config
        self.storedir = self.cfg.get("store_dir")
        if not self.storedir:
            self.storedir = self.cfg.get("repo_access", {}).get('store_dir')
        if not self.storedir:
            raise ConfiguationException("Missing required config parameter: store_dir")
        self.storedir = Path(self.storedir)
        if not self.storedir.is_dir() or not os.access(self.storedir, os.W_OK):
            raise ConfiguationException("store_dir is not an existing directory with write permission: "+
                                        str(self.storedir))
        self.finalbucket = self.cfg.get("public_bucket")
        if not self.finalbucket:
            raise ConfiguationException("Missing required config parameter: public_bucket")

    def apply(self, statemgr: fw.PreservationStateManager):
        """
        apply the archiving step.  
        """
        log = statemgr.log.getChild("archiving")

        if not statemgr.steps_completed & statemgr.SUBMITTED:
            statemgr.record_progress("Archiving files to long-term storage")
            self.launch_migration(statemgr, log)
            statemgr.mark_completed(statemgr.SUBMITTED, "Files submitted to long-term storage")
        
        self.monitor_destination(statemgr, log)
        statemgr.mark_completed(statemgr.ARCHIVED)

    def launch_migration(self, statemgr: fw.PreservationStateManager, log: Logger):
        """
        start the AIP migration process.  In this implementation, the AIP files are copied to a 
        staging directory where an external file synchronization process takes over to replicate
        the files to multiple long-term storage locations.  When all files confirmed to be in the 
        migration staging directory, they are removed from serialization staging area.
        """
        aipfiles = statemgr.get_serialized_files()
        if not aipfiles:
            raise PreservationStateError("No AIP files appear to be ready for transfer!")

        # check for presence of aip files in store dir
        if not self.cfg.get("allow_overwrite"):
            found = [f for f in aipfiles if (self.storedir / os.path.basename(f)).exists()]
            if found:
                flst = "\n  "+found[0]
                if len(found) > 1:
                    flst += "...\n  "+found[-1]
                raise PreservationStateError("Archived target files already found in LT store; " +
                                             "won't overwrite:" + flst)

        # prepare for transfer
        cksfiles = [f for f in aipfiles if os.path.splitext(f)[-1].lstrip('.') in self.cksexts]
        serfiles = [f for f in aipfiles if os.path.splitext(f)[-1].lstrip('.') not in self.cksexts]
        if len(cksfiles) != len(serfiles):
            raise PreservationStateError("Serialized AIP file count != Checksum file count; "
                                         "Are AIPs really ready for transfe?")

        cksdir = self._ensure_dir(self.storedir / f"_{os.path.basename(serfiles[0])}.ckstrx")
        serdir = self._ensure_dir(self.storedir / f"_{os.path.basename(serfiles[0])}.trx")
        statemgr.set_state_property("archiving:serialized_temp_store", str(serdir))
        statemgr.set_state_property("archiving:checksum_temp_store", str(cksdir))
 
        # downstream synchonization is triggeed by presence of SHA file, so transfe zip files first
        try:
            self._safe_copy(serfiles, serdir, statemgr)
            self._safe_copy(cksfiles, cksdir, statemgr, True)
        except Exception as ex:
            log.exception("Failure occured while copying data to store directory: %s", str(ex))
            log.info("Will roll back transfer")
            raise

        # all files successfully transfered; now move files out of temp locations
        statemgr.record_progress("Archiving files: finishing up")
        try: 
            for f in serfiles:
                f = os.path.basename(f)
                os.rename(serdir/f, self.storedir/f)
            for f in cksfiles:
                f = os.path.basename(f)
                os.rename(cksdir/f, self.storedir/f)
        except Exception as ex:
            raise fw.AIPArchivingException(f"Error while renaming archive files: {str(ex)}")

    def revert(self, statemgr: fw.PreservationStateManager) -> bool:
        """
        Clean up any transfer artifacts (from a previous attempt) in preparation of a new transfer 
        attempt.  Note that if the caller knows that a previous attempt was successful, the 
        configuration must be updated to allow for overwrites as those files will not be removed. 
        :return:  False if this step cannot be undone even partially
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        self._clean_tmp_dest_dirs(statemgr)
        statemgr.unmark_completed(statemgr.SUBMITTED|statemgr.ARCHIVED)
        return True

    def clean_up(self, statemgr: fw.PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  
        :raise PreservationException:  if an error occurred preventing the application of this step.
        """
        self._clean_tmp_dest_dirs(statemgr)

    def _clean_tmp_dest_dirs(self, statemgr: fw.PreservationStateManager):
        # clean up the temporary directories created to receive transfered files
        tmpdir = statemgr.get_state_property("archiving:checksum_temp_store")
        if tmpdir and os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)
        tmpdir = statemgr.get_state_property("archiving:serialized_temp_store")
        if tmpdir and os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)

    def _ensure_dir(self, dirp):
        if not dirp.exists():
            try:
                dirp.mkdir()
            except Exception as ex:
                raise PreservationStateError(f"Unable to create tmp dir in destination: {str(ex)}")
        elif not dirp.is_dir():
            raise PreservationStateError(f"Not a directory: {dirp}")
        return dirp

    def _safe_copy(self, srcfiles: List[str], destdir: Path, statemgr: fw.PreservationStateManager,
                   ischkfile: bool=False):
        # now copy each file
        if ischkfile:
            statemgr.record_progress("Archiving files to long-term storage: checksum files")
        try:
            for src in srcfiles:
                archfile = os.path.basename(src)
                if not ischkfile:
                    statemgr.record_progress(f"Archiving files to long-term storage: {archfile}")
                shutil.copy(src, destdir)
        except OSError as ex:
            statemgr.record_progress(f"Archiving files to long-term storage: failure detected on {archfile}")
            statemgr.log.error("%s: Copy failure detected on %s: %s", statemgr.aipid, archfile, str(ex))
            raise fw.AIPArchivingException(f"Archive copy failure on {archfile}: {str(ex)}",
                                           statemgr.aipid) from ex

    def monitor_destination(self, statemgr: fw.PreservationStateManager, log: Logger):
        """
        monitor PDR's public bucket by polling its contents and waiting for the presence of all 
        of the serialized AIP files.  
        """
        pcfg = self.cfg.get("polling", {})
        if not pcfg.get("wait_for_completion", True):
            return

        racfg = self.cfg.get('repo_access')
        if not racfg:
            raise ConfigurationException("Unable to monitor transfer without 'repo_access' config")
        if not racfg.get('store_dir'):
            racfg['store_dir'] = self.storedir
        repo = RepositoryAccess(racfg, log)

        statemgr.record_progress("Archiving files: waiting for arrival in public bucket")
        
        aipfiles = set(os.path.basename(f) for f in statemgr.get_serialized_files())
        if not aipfiles:
            log.warn("%s: No AIP files to wait for", statemgr.aipid)
            return

        cycletime = pcfg.get("cycle_time", 600)
        if not isinstance(cycletime, (int, float)):
            log.warn("config param polling.cycle_time not a number; defaulting to 10 min.")
            cycletime = 600
        faillim = pcfg.get("failure_limit", 0)
        if not isinstance(faillim, int):
            log.warn("config param polling.fail_limit not an integer; defaulting to -1")
            faillim = -1

        found = []
        fails = 0
        waittime = cycletime
        while aipfiles:
            try:
                found = self_publicaips(repo, aipfiles)
                aipfiles = aipfiles.difference(found)
            except Exception as ex:
                log.warn("Trouble polling for migrated files: %s", str(ex))
                fails += 1
                if faillim > 0 and fails > faillim:
                    raise fw.AIPArchivingException(f"Too many polling errors (last one: {str(ex)}")

            if aipfiles and found:
                statemgr.record_progress("Archiving files: waiting for arrival of %d file%s" %
                                         len(aipfiles), "s" if len(aipfiles) > 0 else "")

            # If it looks like we're getting close to being done, reduce the waittime
            if len(aipfiles) == 1:
                waittime = waittime / 2
                if waittime < 2:
                    waittime = cycletime

            if aipfiles:
                time.sleep(waittime)

        if not aipfiles:
            statemgr.record_progress("All files successfully archived.")
        
    def _publicaips(self, repo, aipfiles):
        idvers = set()
        aips = set(aipfiles)
        found = set()
        for aip in aips:
            if aip in found:
                continue
            try:
                id, ver = parse_bag_name(aip)[0:2]
                ver = re.sub(r'_', '.', ver)
            except ValueError:
                # non-complient name; look for it explicitly
                log.warning("Waiting on AIP file with non-standard name: %s", aip)
                if self._aip_available(repo, aip):
                    found.add(aip)
            else:
                # find all AIPs matching id, ver
                if (id, ver) not in idvers:
                    matched = set(a for a in self._findaipsfor(repo, id, ver) if a in aips)
                    found.update(matched)
                idvers.add((id, ver))

        return found

    def _findaipsfor(self, repo, id, ver):
        return [a['name'] for a in repo.available_aips_for(id, ver) if a.get('name')]

    def _aip_available(self, repo, aipfile):
        return repo.aip_available(aipfile)
                    

class PDRPublication(fw.AIPPublication):
    """
    An implementation of the AIP publication interface appropriate for fully public data being published
    into the PDR.  

    This step can carry out the following specific functions:
      * migrating the dataset's products into the distribution system's cache
      * ingesting the dataset's NERDm record into the RMM database
      * delivering the DataCite DOI metadata to DataCite
      * notifying people that the dataset is fully published and ready for access

    This implementaion supports the following configuraiton parameters:

    ``ingest``
            (dict) _required_. a configuration dictionary for the ingest functions that require 
            preparation.  Recognized keys include ``rmm`` and ``doi`` (see more detail below).
    ``data_cache``
            (dict) _optional_. a dictionary for configuring data caching capabilities; see below 
            for the keys that will be recognized in this dictionary.
    ``notifier``
            (dict) _optional_. the configuration for the notification system that alerts humans 
            (by email) about the successes and failures from the preservation system.  See 
            :py:mod:`nistoar.pdr.notify` for details of this configuration.  The ``alert`` 
            types used by this preservation step are described below.  
 
    The ``ingest`` configuration dictionary is also used by the :py:class:`PDRFinalization` class; 
    the parameters used here include:

    ``doi``
            a configuration dictionary for configuring the DOI minting client used to submit the 
            DataCite record; see the :py:class:`~nistoar.pdr.ingest.dc.client.DOIMintingClient` class
            for the supported sub-parameters.  In addition, a sub-paramter called ``fail_on_incomplete``,
            described below, is supported.
    ``rmm``
            a configuration dictionary for configuring the ingest client used to submit the 
            NERDm record; see the :py:class:`~nistoar.pdr.ingest.rmm.client.IngestClient` class
            for the supported sub-parameters.  In addition, a sub-paramter called ``fail_on_incomplete``,
            described below, is supported.

    The ``ingest`` sub-sections both can also include this following sub-parameter:
    ``fail_on_incomplete``  
            If ``False`` (default), if the ingest attempt fails, processing will continue
            (after logging the error and, if configured, a notification sent); if ``True``,
            a failure results in an 
            :py:class:`~nistoar.pdr.preserve.task.framework.AIPPublicationException` is 
            raised, causing processing to stop.  

    The following parameters from the ``data_cache`` dictionary are used to configure a 
    :py:class:`~nistoar.pdr.distrib.cachectl.CacheCtlClient` that will be used to migrate
    the new dataset's products into the distribution system's cache.  If not provided,
    data caching will not be done by this step

    ``service_endpoint``:
            (str) _required_.  the base URL for the cache manager Web API used to migrate
            the data.
    ``auth_key``:
            (str) _required_.  the secret authorization key to use when accessing the API

    The ``notifier`` configures the :py:mod:`notification system<nistoar.pdr.notify>` used by 
    this presrevation step.  (It may be shared by other preservation steps.)  This step uses
    the following alert types:

    ``preserve.failure``
            a notification that announces that preservation of dataset failed and requires 
            attention by an administrator.
    ``preserve.success``
            a notification that announces that preservation of a dataset successfully completed.
    ``ingest.failure``
            a notification that announces that there was a failure specifically ingesting the 
            dataset's NERDm metadata into the RMM.  Generally, this means that the dataset's 
            landing page will not be available.
    ``doi.failure``
            a notification that announces that there was a failure specifically sending the DOI
            metadata to DataCite and establishing the public availability of the DOI.  
    ``cache.failure``
            a notification that announces that there was a failure specifically caching the dataset
            into the distribution system.  This type failure is not critical and thus does not prevent 
            the issuance of a ``preserve.success`` alert.  
    """

    def __init__(self, config=None, notifier=None):
        """
        instantiate the publication step
        :param dict config:  the configuration for this step; if not provided, defaults will apply.
        """
        if config is None:
            config = {}
        self.cfg = config
        self._notifier = notifier

        # client for requesting that data be cached
        scfg = self.cfg.get('store')
        # if not scfg:
        #     raise ConfigurationException("Missing required configparameter: store")
        # self._storer = PDRStoreClient(scfg)

        setuplog = preserve_system.getSysLogger().getChild('PDRPublication')

        icfg = self.cfg.get('ingest', {})
        self._ingester = None
        self._ingcfg = icfg.get('rmm')
        if self._ingcfg and self._ingcfg.get('service_endpoint'):
            self._ingester = RMMIngestClient(self._ingcfg)
        if not self._ingester:
            raise ConfigurationException("ingest.rmm not configured")

        self._doiminter = None
        self._dmcfg = icfg.get('doi')
        if self._dmcfg and self._dmcfg.get('datacite_api'):
            self._doiminter = DOIMintingClient(self._dmcfg)
        if not self._doiminter:
            setuplog.warning("DOI Minter not configured (missing doi parameter): won't create DOI")

        self._cachecli = None
        dccfg = self.cfg.get('data_cache')
        if dccfg and dccfg.get('service_endpoint'):
            if not dcfg.get('auth_key'):
                setuplog.warnging("Missing data_cache.auth_key; no authorization key will be used")
            self._cachecli = CacheCtlClient(dccfg['service_endpoint'], dccfg.get('auth_key'))
        if not self._cachecli:
            setuplog.warning("Cache client not configured (missing data_cache parameter)")

        if not self._notifier and self.cfg.get('notifier'):
            self._notifier = NotificationService(self.cfg['notifier'])
        if not self._notifier:
            setuplog.warning("Notification service not configured (missing notifier parameter)")

        self._done = set()
            

    def apply(self, statemgr: fw.PreservationStateManager):
        """
        apply the final publication step.  This implementation assumes details specific to the NIST
        PDR system as well as the use of ingest staging done during the finalization step (as with 
        :py:class:`PDRBagFinalization`).  
        """
        log = statemgr.log.getChild("publication")
        aipid = statemgr.aipid
        version = statemgr.get_state_property("nerdm:version", "?")
        statemgr.record_progress("Releasing dataset to the PDR")

        # TODO: submit for caching
        if self._cachecli:
            statemgr.record_progress("Triggering migration of data to distribution system")
            try:
                self._cachecli.ensure_cached(aipid)
                self._note_reverted(statemgr, "cache_data")

            except Exception as ex:
                msg = f"Failure while caching data products: {str(ex)}"
                log.exception(msg)
                log.info("Cache API service endpoint: %s", self._cachecli.ep)
                self._note_failed(statemgr, "cache_data")

                if self._notifier:
                    self._notifier.alert("cache.failure", origin=self.name,
                                         summary=f"Distribution caching failure: {aipid}", 
                                         desc=msg, id=aipid, version=version)

        if self._ingester:
            # submit NERDm record to RMM ingest service
            statemgr.record_progress("Releasing metadata to the PDR")
            try:
                if not self._ingester.is_staged(aipid):
                    PreservationStateException(f"No staged RMM record found for AIP-ID={aipid}")
                self._ingester.submit(aipid)
                log.info("Submitted NERDm record to RMM")
                self._note_succeeded(statemgr, "rmm_ingest")

            except Exception as ex:
                msg = f"Failed to ingest record with name={aipid} into RMM: {str(ex)}"
                if self._ingcfg.get('fail_on_incomplete'):
                    raise fw.AIPPublicationException(msg)
                log.exception(msg)
                log.info("Ingest service endpoint: %s", self._ingester.endpoint)

                if isinstance(ex, NotValidForIngest):
                    self._note_failed(statemgr, "rmm_ingest")
                else:
                    self._note_reverted(statemgr, "rmm_ingest")

                if self._notifier:
                    self._notifier.alert("ingest.failure", origin=self.name,
                                         summary=f"NERDm ingest failure: {aipid}", 
                                         desc=msg, id=aipid, version=version)

        if self._doiminter:
            # submit the DOI metadata to DataCite
            statemgr.record_progress("Releasing DOI metadata to DataCite")
            try:
                if not self._doiminter.is_staged(aipid):
                    PreservationStateException(f"No staged DOI record found for AIP-ID={aipid}")
                self._doiminter.submit(aipid)
                log.info("Submitted DOI record to DataCite")
                self._note_succeeded(statemgr, "mint_doi")

            except Exception as ex:
                msg = f"Failed to submit DOI record with name={aipid} to DataCite: {str(ex)}"
                if self._dmcfg.get('fail_on_incomplete'):
                    raise fw.AIPPublicationException(msg)
                log.exception(msg)
                log.info("DOI minter service endpoint: %s", self._doiminter.dccli._ep)

                if isinstance(ex, DOIClientException):
                    self._note_failed(statemgr, "mint_doi")
                else:
                    self._note_reverted(statemgr, "mint_doi")

                if self._notifier:
                    self._notifier.alert("doi.failure", origin=self.name,
                                         summary="NERDm ingest failure: {aipid}", 
                                         desc=msg, id=aipid, version=version)

        statemgr.mark_completed(statemgr.PUBLISHED, "AIP is released")

    def revert(self, statemgr: fw.PreservationStateManager):
        log = statemgr.log.getChild("publication")
        revertinfo = statemgr.get_state_property("publication:revert", {})
        if revertinfo:
            log.info("reverting state from previous attempt")
        
        if self._doiminter and revertinfo.get("mint_doi"):
            resultdir = None
            if revertinfo["mint_doi"] == "succeeded":
                resultdir = self._doiminter._publishdir
            elif revertinfo["mint_doi"] == "failed":
                resultdir = self._doiminter._faildir
            elif revertinfo["mint_doi"] == "interrupted":
                resultdir = self._doiminter._inprogdir

            if resultdir:
                staged = os.path.join(self._doiminter._stagedir, statemgr.aipid+".json")
                handled = os.path.join(resultdir, statemgr.aipid+".json")
                if not os.path.exists(staged) and os.path.isfile(handled):
                    try:
                        shutil.copy(handled, staged)
                    except Exception as ex:
                        log.error("Failed to revert DOI record from %s: %s", handled, str(ex))

                del revertinfo["mint_doi"]

        if self._ingester and revertinfo.get("rmm_ingest"):
            resultdir = None
            if revertinfo["rmm_ingest"] == "succeeded":
                resultdir = self._doiminter._successdir
            elif revertinfo["rmm_ingest"] == "failed":
                resultdir = self._doiminter._faildir
            elif revertinfo["rmm_ingest"] == "interrupted":
                resultdir = self._doiminter._inprogdir

            if resultdir:
                staged = os.path.join(self._doiminter._stagedir, statemgr.aipid+".json")
                handled = os.path.join(resultdir, statemgr.aipid+".json")
                if not os.path.exists(staged) and os.path.isfile(handled):
                    try:
                        shutil.copy(handled, staged)
                    except Exception as ex:
                        log.error("Failed to revert NERDm record from %s: %s", handled, str(ex))

                del revertinfo["rmm_ingest"]

        # nothing to do for caching
        if revertinfo.get("cache_data"):
            del revertinfo["cache_data"]

        statemgr.set_state_property("publication:revert", revertinfo)
        return super().revert(statemgr)

    def _set_for_revert(self, statemgr, action, result):
        revertinfo = statemgr.get_state_property("publication:revert", {})
        revertinfo[action] = result
        statemgr.set_state_property("publication:revert", revertinfo)

    def _note_failed(self, statemgr, action):
        self._set_for_revert(statemgr, action, "failed")
    def _note_succeeded(self, statemgr, action):
        self._set_for_revert(statemgr, action, "succeeded")
    def _note_reverted(self, statemgr, action):
        self._set_for_revert(statemgr, action, "reverted")
            
    

class RepositoryAccess:
    """
    an interface to the repositories APIs as well as local caches to determine the existance of 
    previously published resources. 

    This class looks for the following configuration parameters:

    ``distrib_service`` 
         a description of the repository's distribution service, used for interrogating
         previously published datasets via their AIP bags.  The dictionary value
         requires only one sub-parameter, ``service_endpoint``, that gives the REST
         service's base URL.
    ``metadata_service``  
         a description of the repository's metadata service, used for interrogating
         previously published datasets via their NERDm records.  (This is used to 
         find publications that did not previously get published via the preservation
         service and which, therefore, has no associated bags.)  The dictionary value
         requires only one sub-parameter, ``service_endpoint``, that gives the REST
         service's base URL.
    ``headbag_cache`` 
         a local directory that is use for caching head bags that have gone through the
         preservation service.
    ``store_dir``
         a local directory providing the gateway to long-term storage of public AIP bags. 
    ``restricted_store_dir``
         a local directory providing the gateway to long-term storage of restricted-
         access AIP bags.
    """

    def __init__(self, config, log=None):
        self.cfg = config
        self.log = log
        if not self.log:
            self.log = logging.getLogger("quiet")
            self.log.setLevel(logging.CRITICAL+10)  # silent

        self.distrib = None
        ep = self.cfg.get("distrib_service",{}).get("service_endpoint")
        if ep:
            self.distrib = RESTServiceClient(ep)
        

    def latest_headbag(self, aipid, include_in_process=True):
        """
        return the name of the latest headbag available for the dataset with the given AIP identifier

        :param str aipid:                the identifier of AIP of interest
        :param bool include_in_process:  if True (default), this method will also consult local caches 
                                         for bags that may still be going through the preservation 
                                         process.  If False, the answer will return the bag that has 
                                         fully completed the preservation (and ingest) process.  
        :raises DistribServiceException:  when a failure occurs while consulting the distribution service
        """
        candidates = []
        if self.distrib:
            distrib = BagDistribClient(aipid, self.distrib)
            try:
                candidates.extend([ distrib.head_for_version() ])
            except DistribResourceNotFound as ex:
                pass

        elif not include_in_process:
            raise DistribServiceException("Unable to determine latest head bag: "
                                          "No distribution service configured")
        else:
            self.log.warning("headbag queries are less accurate without access to distribution service")

        if include_in_process:
            locales = (
                self.cfg.get('store_dir'),
                self.cfg.get('restricted_store_dir'),
                self.cfg.get('headbag_cache')
            )

            for loc in locales:
                if loc and os.path.isdir(loc):
                    bags = [f for f in os.listdir(loc)
                              if f.startswith(aipid+".") and bagutils.is_legal_bag_name(f)]
                    if bags:
                        candidates.append(bagutils.find_latest_head_bag(bags))

        if not candidates:
            return None

        return bagutils.find_latest_head_bag(candidates)

    def available_aips_for(self, aipid, version):
        """
        return a list of descriptions of AIP files that are available for the given AIP ID and version
        """
        if not self.distrib:
            raise ConfigurationException("No access to distribution service available "+
                                         "(distrib_service was not set)")

        distrib = BagDistribClient(aipid, self.distrib)
        try:
            return distrib.describe_for_version(version)
        except DistribResourceNotFound:
            return []

    def aip_available(self, aipfile):
        if not self.distrib:
            raise ConfigurationException("No access to distribution service available "+
                                         "(distrib_service was not set)")

        aipres = "/".join(['_aip', aipfile])
        return self.distrib.is_available(aipres)

class PDRPreservationCleanup(fw.AIPCleanup):
    """
    An implementation of the AIP clean-up step appropriate for the PDR preservation workflow.

    When applied, this clean-up step will, by default:
       * remove from the staging area all serialized bags and associated SHA files _except_ 
         for the head bag, 
       * remove all unserialized multi-bags from the work area, and
       * remove the input SIP bag.

    When the ``cancel`` argument is passed in as ``True``, this step will:
       * remove all serialized bags, including the head bag,
       * remove all unserializaed multi-bags, 
       * leave the input SIP bag intact, and 
       * update the state manager for starting preservation again from the beginning.
    """

    def __init__(self, config=None):
        if config is None:
            config = {}
        self.cfg = config
        
    def clean_serialized_bags(self, statemgr: fw.PreservationStateManager, 
                              cancel=False, log: Logger=None):
        staged = statemgr.get_serialized_files()
        if staged:
            head = None
            if not cancel:
                head = find_latest_head_bag(staged)
                staged.remove(head)  # don't delete the head bag
            if self.delete_files(staged, statemgr, "serialized bags", log):
                statemgr.set_serialized_files(None if cancel else [head])

    def clean_multibags(self, statemgr: fw.PreservationStateManager, cancel=False, log: Logger=None):
        if self.cfg.get("cleanup_unserialized_bags", True):
            self._rm_mb_working_dir(statemgr, log)
        
    def _rm_mb_working_dir(self, statemgr: fw.PreservationStateManager, log: Logger=None):
        workdir = Path(statemgr.get_working_dir()) / "multibag"
        if workdir.exists():
            if not log:
                log = statemgr.log.getChild("cleanup")
            if not workdir.is_dir():
                log.warning("Multibag working dir, %s, is not a directory", str(workdir))
            else:
                try: 
                    shutil.rmtree(workdir)
                except Exception as ex:
                    log.error("Trouble deleting multibag work directory: %s", str(ex))

    def clean_original_aip(self, statemgr: fw.PreservationStateManager, log: Logger=None):
        sipdir = statemgr.get_original_aip()
        if sipdir and os.path.exists(sipdir):
            if not log:
                log = statemgr.log.getChild("cleanup")
            if not os.path.isdir(sipdir):
                raise PreservationStateException("PDRPreservationCleanup: assumption is that input SIP "+
                                                 "is a directory is not True")
            else:
                try:
                    shutil.rmtree(sipdir)
                except Exception as ex:
                    log.error("Trouble deleting original SIP: %s", str(ex))
        

    def apply(self, statemgr: fw.PreservationStateManager, cancel=False, *kw):
        """
        Apply final clean-up chores.  See :py:class:`class documentation<AIPPreservationCleanup>` 
        for details.
        """
        log = statemgr.log.getChild("clean-up")
        if cancel:
            log.info("Preservation canceled: cleaning up for restart")
        else:
            log.info("Preservation completed successfully: cleaning up")

        disabled = self.cfg.get('disabled')
        if disabled is None or disabled is False:
            disabled = []
        elif not isinstance(disabled, (bool, list)):
            log.warning("Wrong type for config param 'disabled' (%s); will be ignored", type(disabled))
            disabled = []
            
        if self.cfg.get('disabled') is True:
            log.warning("Full clean-up is disabled; preservation artifacts will remain")
            return

        if 'multibag' not in disabled:
            self.clean_multibags(statemgr, cancel, log)
        else:
            log.info("Skipping multibag clean-up (disabled in config)")

        if 'serialized' not in disabled:
            self.clean_serialized_bags(statemgr, cancel, log)
        else:
            log.info("Skipping serialized bag clean-up (disabled in config)")

        if not cancel:
            if 'original' not in disabled:
                self.clean_original_aip(statemgr, log)
            else:
                log.info("Skipping original input SIP (disabled in config)")


class PDRPreservationTaskFactory(fw.PreservationTaskFactory):
    """
    a factory for creating a :py:class:`PreservationTask` injected with necessary 
    :py:class:PreservationStep instances appropriate for preserving SIPs submitted for 
    publication to the NIST PDR.  
    """
    def_supported_types = ["pdr", "def"]

    def __init__(self, config: Mapping=None):
        super(PDRPreservationTaskFactory, self).__init__(config)
        self._massage_config()

    def _massage_config(self):
        if self.cfg.get('repo_access'):
            for step in ['archive', 'finalize']:
                if not self.cfg.get(step, {}).get('repo_access'):
                    self.cfg.setdefault(step, {})
                    self.cfg[step]['repo_access'] = self.cfg['repo_access']

        for prop in ['store_dir', 'restricted_store_dir']:
            if self.cfg.get(prop):
                for step in ['finalize', 'archive']:
                    if not self.cfg.get(step, {}).get(prop):
                        self.cfg.setdefault(step, {})
                        self.cfg[step][prop] = self.cfg[prop]

    def _create_state_manager(self, aipid: str, config: Mapping,
                              logger: Logger, startover=False) -> fw.PreservationStateManager:
        if config.get('type', 'def') not in self.def_supported_types + ["json"]:
            raise ConfigurationException("Unsupported state manager type: "+str(config.get('type')))
        return JSONPreservationStateManager(config, aipid, clear_task=restart)
        
    def _create_finalizer(self, config: Mapping) -> fw.AIPFinalization:
        if config.get('type', 'def') not in self.def_supported_types:
            raise ConfigurationException("Unsupported finalize step type: "+str(config.get('type')))
        return PDRBagFinalization(config)

    def _create_validater(self, config: Mapping) -> fw.AIPValidation:
        if config.get('type', 'def') not in self.def_supported_types:
            raise ConfigurationException("Unsupported validate step type: "+str(config.get('type')))
        return NISTBagValidation(config)

    def _create_serializer(self, config: Mapping) -> fw.AIPSerialization:
        if config.get('type', 'def') not in self.def_supported_types:
            raise ConfigurationException("Unsupported validate step type: "+str(config.get('type')))
        return NISTBagSerialization(config)

    def _create_archiver(self, config: Mapping) -> fw.AIPArchiving:
        if config.get('type', 'def') not in self.def_supported_types:
            raise ConfigurationException("Unsupported validate step type: "+str(config.get('type')))
        return PDR1AIPArchiving(config)

    def _create_publisher(self, config: Mapping) -> fw.AIPPublication:
        if config.get('type', 'def') not in self.def_supported_types:
            raise ConfigurationException("Unsupported validate step type: "+str(config.get('type')))
        return PDRPublication(config)

    def _create_cleaner(self, config: Mapping) -> fw.AIPCleanup:
        tp = config.get('type', 'def')
        if tp == 'min':
            # a minimal cleaner is wanted; PreservationTask will create a minimal default
            return None
        if tp not in self.def_supported_types:
            raise ConfigurationException("Unsupported validate step type: "+str(tp))
        return PDRAIPCleanup(config)


        
