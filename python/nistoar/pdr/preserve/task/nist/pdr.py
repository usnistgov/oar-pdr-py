"""
Implementations of the presersvation task framework that are specific to the NIST Public Data Repository 
and how it handles preservation.
"""

import os, time, datetime, logging, json, shutil
from logging import Logger
from pathlib import Path
from collections import OrderedDict
from typing import List

from .. import framework as fw
from nistoar.pdr.preserve.bagit import BagBuilder, BagWriteError, BagItException
from nistoar.pdr.exceptions import ConfigurationException
import nistoar.pdr.preserve.bagit.utils as bagutils
from nistoar.pdr.preserve import PreservationStateError
import nistoar.id.versions as verutils
from nistoar.pdr.ingest import RMMIngestClient, DOIMintingClient
from nistoar.pdr.distrib import (BagDistribClient, RESTServiceClient,
                                 DistribResourceNotFound, DistribServiceException)

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
        bagdir = Path(statemgr.get_original_aip())
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
                orig = statemgr.get_original_aip()
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
        statemgr.record_progress("Archiving files: waiting for arrival in public bucket")
        
        aipfiles = [os.path.basename(f) for f in statemgr.get_serialized_files()]
        if not aipfiles:
            log.warn("%s: No AIP files to wait for", statemgr.aipid)
            return

        prefix = os.path.commonprefix(aipfiles)
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
                likefiles = self._findbyprefix(prefix)
                fails = 0
                for i in range(len(aipfiles)):
                    f = aipfiles.pop(0)
                    if f in likefiles:
                        found.append(f)
                    else:
                        aipfiles.append(f)
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
        
    def _findbyprefix(self, prefix: str, location: str = None):
        # Qery AWS S3 bucket for files starting with prefix
        return []
    

class PDRPublication(fw.AIPPublication):
    """
    An implementation of the AIP publication interface appropriate for fully public data being published
    into the PDR.  

    This implementaion supports the following configuraiton parameters:

    ``store``    
            (dict) the configuration for an AIPStoreClient that will deliver the serialized AIPs
            to long-term storage.
    ``ingest``
            (dict) a configuration dictionary for the ingest functions that require preparation.
            recognized keys include ``rmm`` and ``doi`` (see more detail below).

    The ``ingest`` configuration dictionary is also used by the PDRPublication class; the parameters 
    used here include:

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

    def apply(self, statemgr: fw.PreservationStateManager):
        """
        apply the final publication step.  This implementation assumes details specific to the NIST
        PDR system as well as the use of ingest staging done during the finalization step (as with 
        :py:class:`PDRBagFinalization`).  
        """
        log = statemgr.log.getChild("publication")
        aipid = statemgr.aipid
        version = statemgr.get_state_property("nerdm:version", "?")
        statemgr.record_progress("Releasing metadata to the PDR")

        # TODO: submit for caching

        if self._ingester:
            # submit NERDm record to RMM ingest service
            try:
                if not self._ingester.is_staged(aipid):
                    PreservationStateException(f"No staged RMM record found for AIP-ID={aipid}")
                self._ingester.submit(aipid)
                log.info("Submitted NERDm record to RMM")
            except Exception as ex:
                msg = f"Failed to ingest record with name={aipid} into RMM: {str(ex)}"
                if self._ingcfg.get('fail_on_incomplete'):
                    raise fw.AIPPublicationException(msg)
                log.exception(msg)
                log.info("Ingest service endpoint: %s", self._ingester.endpoint)

                if self._notifier:
                    self._notifier.alert("ingest.failure", origin=self.name,
                                         summary=f"NERDm ingest failure: {aipid}", 
                                         desc=msg, id=aipid, version=version)

        if self._doiminter:
            # submit the DOI metadata to DataCite
            try:
                if not self._doiminter.is_staged(aipid):
                    PreservationStateException(f"No staged DOI record found for AIP-ID={aipid}")
                self._doiminter.submit(aipid)
                log.info("Submitted DOI record to DataCite")
            except Exception as ex:
                msg = f"Failed to submit DOI record with name={aipid} to DataCite: {str(ex)}"
                if self._dmcfg.get('fail_on_incomplete'):
                    raise fw.AIPPublicationException(msg)
                log.exception(msg)
                log.info("DOI minter service endpoint: %s", self._doiminter.dccli._ep)

                if self._notifier:
                    self._notifier.alert("doi.failure", origin=self.name,
                                         summary="NERDm ingest failure: {aipid}", 
                                         desc=msg, id=aipid, version=version)

        statemgr.mark_completed(statemgr.PUBLISHED, "AIP is released")

    def revert(self, statemgr: fw.PreservationStateManager):
        return False
    

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
            locals = (
                self.cfg.get('store_dir'),
                self.cfg.get('restricted_store_dir'),
                self.cfg.get('headbag_cache')
            )

            for loc in locals:
                if loc and os.path.isdir(loc):
                    bags = [f for f in os.listdir(loc)
                              if f.startswith(aipid+".") and bagutils.is_legal_bag_name(f)]
                    if bags:
                        candidates.append(bagutils.find_latest_head_bag(bags))

        if not candidates:
            return None

        return bagutils.find_latest_head_bag(candidates)

                    
            

        
