"""
A common implementation of the AIPValidation preservation step interface that tests for NIST-compliant
preservation bags.
"""
import os, time, datetime, logging, json, shutil
from pathlib import Path
from typing import Union
from collections import OrderedDict

from . import framework as fw
from nistoar.pdr.preserve.bagit import NISTBag
from nistoar.pdr.preserve.bagit.multibag import MultibagSplitter
from nistoar.pdr.preserve.bagit.serialize import DefaultSerializer
from nistoar.pdr.exceptions import StateException
from nistoar.pdr.utils import checksum_of

class NISTBagSerialization(fw.AIPSerialization):
    """
    An implementation of the AIP serialization step.  This represents a default implementation in which
    a target bag may get split into multiple "Multi-bag" bags (if source exceeds size requirements) and 
    all bags are serialized into files and compressed.  In accordance with the preservation task 
    framework, it is assumed that the target bag has been finalized and validated.  

    This implementation supports the following configuration parameters:
    ``multibag``  
          (dict) a configuration that controls how AIP bags are split into multiple bags.  If not 
          provided, multi-bag splitting will not be applied.  Furthermore, if the original bag to
          be serialized has a size smaller than ``multibag.max_headbag_size`` (which defaults to 
          ``multibag.max_bag_size``), splitting will not be applied.  See the 
          :py:class:`~nistoar.pdr.preserve.bagit.multibag.MulibagSplitter` class for the definition
          of supported sub-parameters.
    ``format``
          (str) a label indicating the format of serialization that will be applied.  Supported values
          include "zip" (default) and "7z".
    ``hard_link_data``
          (bool) if True (default), unix hard links will be used when replicating data files
          in a bag (which can be quite large).  This does not apply to other metadata files in the 
          bag.  If False, then data files will be explicitly copied.  Of course, This only applies 
          when replicating a bag to a destination on the same filesystem as the source bag.
    ``cleanup_unserialized_bags``  
          if True (default) and the input bag was split before serialization, then the resulting 
          (unserialized) split bags will be removed after serialized into zip files.  
    """
    def __init__(self, config=None):
        """
        instantiate the serialization step
        :param dict config:  the configuration for this step; if not provided, defaults will apply.
        """
        super(NISTBagSerialization, self).__init__(config)
        self._ser = DefaultSerializer()

    def apply(self, statemgr: fw.PreservationStateManager, notifier: fw.NotificationService=None, **kw):
        """
        apply the serialization step.  
        """
        log = statemgr.log.getChild("serialization")
        statemgr.record_progress("Serializing")

        bagdir = statemgr.get_finalized_aip()
        if bagdir is None:
            raise fw.AIPSerializationException("Finalized bag is not set (rerun finalized?)",
                                               statemgr.aipid)
        srcbags = [ bagdir ]

        # Consider multibag splitting
        mbcfg = self.cfg.get('multibag', {})
        maxhbsz = mbcfg.get('max_headbag_size', mbcfg.get('max_bag_size'))
        if maxhbsz:
            log.info("Considering multibagging (max size: %d)", maxhbsz)
            mbcfg.setdefault('replace', True)
            mbspltr = MultibagSplitter(bagdir, mbcfg)

            if mbspltr.check(log.getChild("splitter")):
                # replicate in workspace
                destdir = Path(statemgr.get_working_dir()) / "multibag"
                destdir.mkdir(exist_ok=True)
                
                bigbagdir = self.replicate_bag(bagdir, destdir)
                mbspltr = MultibagSplitter(bigbagdir, mbcfg)

                srcbags = mbspltr.split(destdir, log.getChild("splitter"))

        else:
            log.warning("multibag splitting not configured")

        # serialize each bag
        stagedir = statemgr.get_stage_dir()
        outfiles = []
        info = []
        format = self.cfg.get('format', 'zip')
        for bagd in srcbags:
            bagfile = self._ser.serialize(bagd, stagedir, format)
            outfiles.append(bagfile)
            statemgr.set_serialized_files(outfiles)

            csumfile = bagfile + ".sha256"
            csum = checksum_of(bagfile)
            with open(csumfile, 'w') as fd:
                fd.write(csum)
                fd.write('\n')
            outfiles.append(csumfile)
            statemgr.set_serialized_files(outfiles)

            info.append({
                'name': os.path.basename(bagfile),
                'sha256': csum
            })
        
        if hasattr(statemgr, "annotate"):
            statemgr.annotate("bagfiles", info)

        statemgr.mark_completed(statemgr.SERIALIZED, "AIP bag serialization completed")

    def clean_up(self, statemgr: fw.PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  
        :raise PreservationException:  if an error occurred preventing the application of this step.
        """
        log = statemgr.log.getChild("serialization")

        if self.cfg.get("cleanup_unserialized_bags", True):
            self._rm_mb_working_dir(statemgr)

    def _rm_mb_working_dir(self, statemgr: fw.PreservationStateManager):
        workdir = Path(statemgr.get_working_dir()) / "multibag"
        if workdir.exists():
            if not workdir.is_dir():
                log.warning("Serialization working dir, %s, is not a directory", str(workdir))
            else:
                shutil.rmtree(workdir)

    def revert(self, statemgr: fw.PreservationStateManager) -> bool:
        """
        If possible, undo this preservation step.  If it cannot be (fully) undone by design, this 
        function will return without raising an exception.
        :return:  False if this step cannot be undone even partially
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        log = statemgr.log.getChild("serialization").getChild("revert")
        statemgr.unmark_completed(statemgr.SERIALIZED)
        self._rm_mb_working_dir(statemgr)

        serfiles = statemgr.get_serialized_files()
        if serfiles:
            for serfile in serfiles:
                if os.path.isfile(serfile):
                    try:
                        os.remove(serfile)
                    except OSError as ex:
                        log.warning("Unable to remove serialization file, %s: %s", serfile, str(ex))
        statemgr.set_serialized_files(None)
        return True

    def replicate_bag(self, srcbag: Union[str, Path], destdir: Union[str, Path],
                      hard_link_data=None, remove_existing=True) -> Path:
                    
        srcbag = Path(srcbag)
        destbag = Path(destdir) / srcbag.name
        if destbag.exists():
            if destbag.is_dir():
                shutil.rmtree(destbag)
            else:
                destbag.unlink()

        try:
            destbag.mkdir()
            for entry in srcbag.iterdir():
                if entry.name == "data":
                    # copy using hardlinks
                    self._replicate_with_hardlinks(entry, destbag)
                elif entry.is_dir():
                    # normal dir copy
                    shutil.copytree(entry, destbag / entry.name, True)
                elif entry.is_symlink():
                    # not expected; preserve the link
                    (destbag / entry.name).symlink_to(os.path.readlink(entry))
                else:
                    # normal file copy
                    shutil.copy(entry, destbag)

        except OSError as ex:
            if destbag.exists():
                shutil.rmtree(destbag)
            raise

        return destbag
        
    def _replicate_with_hardlinks(self, srcdir: Path, dest: Path):
        # if srcdir and dest are not in the same filesystem, just do a traditional copy
        if srcdir.stat().st_dev != dest.stat().st_dev:
            shutil.copytree(srcdir, dest)
            return

        # walk the source directory to replicate its
        parent = dest
        for root, dirs, files in os.walk(srcdir):
            parent = parent / os.path.basename(root)
            parent.mkdir(exist_ok=True)

            for f in files:
                srcf = os.path.join(root,f)
                destf = parent / f

                if os.path.isfile(srcf):
                    # creeate a hard link in the destination directory
                    if destf.is_dir():
                        shutil.rmtree(destf)
                    elif destf.exists():
                        destf.unlink()
                    os.link(srcf, destf)

                elif os.path.islink(srcf):
                    # not expected; preserve the link
                    os.symlink(os.path.readlink(srcf), destf)

                else:
                    # should not happen
                    shutil.copy(srcf, parent)

        
            
                        
                
            
