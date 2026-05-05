"""
This module provides an implementation of the 
:py:class:`~nistoar.pdr.preserve.task.framework.PreservationStateManager`.
"""
import json
from pathlib import Path
from collections import OrderedDict
from collections.abc import Mapping
from logging import Logger
from typing import List, Iterable

from .framework import PreservationStateManager, UNSTARTED_PROGRESS
from .. import PreservationStateError, PreservationException

from nistoar.pdr.utils.io import LockedFile
from nistoar.base.config import ConfigurationException
from nistoar.pdr.publish.service import status
from nistoar.pdr.utils.io import _PathTolerantJSONEncoder

class JSONPreservationStateManager(PreservationStateManager):
    """
    An implementation of the :py:class:`~nistoar.pdr.preserve.task.framework.PreservationStateManager`
    class in which the state is stored in a JSON files on disk.

    This implementation will look for the following properties in the configuration dictionary passed 
    to it:

    :persist_in:       a file or directory path where this manager can persist its state information 
                       (no default).  If the path points to an existing directory, the state will be
                       written to a file in that directory with a name of the form 
                       _aipid_``_state.json``.  Otherwise it will be interpreted as a file path; its
                       parent directory must exist.
    :stage_dir:        a directory path where serialized AIP files can be written prior to archiving.
    :working_dir:      a directory path where preservation task steps can write temporary data.  
    :keep_fresh:       if True (default), the persist state will be reloaded often (everytime information 
                       is asked for).  Set this to False if it is expected that this instance will be 
                       the only one updating this status.  
    """

    def __init__(self, config: Mapping=None, aipid: str=None, aiploc: str=None, logger: Logger=None,
                 stat: status.SIPStatus=None, clear_state: bool=False, persistin: Path=None):
        """
        Create the state manager
        :param dict    config:  the dictionary for configuring this instance
        :param str      aipid:  the ID of the AIP being preserved
        :param str     aiploc:  the location (as a file path or a URI) of the AIP
        :param Logger  logger:  the logger to use in this state manager
        :param SIPStatus stat:  an SIPStatus tracking the overall publishing status; if provided,
                                it will be updated messages about what's happening with preservation.
        :param bool clear_state:  if True, any previously persisted state will be over-written by 
                                an initial, pre-preservation state.  Otherwise (default), the 
                                currently persisted state will be loaded into memory.
        :param Path persistin:  the location to persist state information to; this overrides the 
                                value of ``persist_in`` in the configuration.  This location must be 
                                provided either via this argument or in the configuration.
        """
        if config is None:
            config = {}
        self._data = None
        self._keepfresh = config.get("keep_fresh", True)
        self._pubstat = stat

        # determine/process the cache file before calling the super-constructor.   If the AIP-ID
        # and the AIP location were not given as arguments, we might be able to get them from
        # the cache file, if it exists.
        if not persistin:
            persistin = config.get("persist_in")
        if not persistin:
            raise ConfigurationException("Missing configuration parameter: persist_in")
        if isinstance(persistin, str):
            persistin = Path(persistin)
        if not aipid:
            if persistin.is_file():
                self._load(persistin)
                try:
                    aipid = self._data["_aipid"]
                except KeyError as ex:
                    raise ValueError(f"{persistin}: file missing required property: {str(ex)}")
            else:
                raise ValueError(self.__class__.__name__ + "(): when aipid is not given, " +
                                 "persistin must point to an existing file")

        if persistin.is_dir():
            persistin = persistin / f"{aipid}_state.json"
        elif not persistin.parents[0].is_dir():
            raise PreservationStateError("Preservation state file's parent is not an existing directory: "
                                         + str(persistin.parents[0]))
        self._cachefile = persistin
        
        super(JSONPreservationStateManager, self).__init__(aipid, config, logger)

        if self._cachefile.exists() and clear_state:
            self._cachefile.unlink()

        # synchronize our cache
        if self._cachefile.exists():
            self._load()
            self._data["_aipid"] = self._aipid
            if aiploc:
                self._data["_orig_aip"] = aiploc
            if self.cfg.get("stage_dir"):
                self._data["_stage_dir"] = self.cfg.get("stage_dir")
            if self.cfg.get("working_dir"):
                self._data["_work_dir"] = self.cfg.get("working_dir")
            self._cache()
        else:
            self._init_state(_aipid=self._aipid, _orig_aip=aiploc,
                             _stage_dir=self.cfg.get("stage_dir"), _work_dir=self.cfg.get("working_dir"))

    def _init_state(self, **kw):
        self._data = OrderedDict(kw)
        self._data["_completed"] = self.UNSTARTED
        self._data["_message"] = UNSTARTED_PROGRESS
        self._cache()

    def _load(self, cachefile=None):
        if not cachefile:
            cachefile = self._cachefile
        try:
            with LockedFile(cachefile) as fd:
                self._data = json.load(fd)
        except FileNotFoundError as ex:
            msg = "Trouble loading preservation state for AIP=%s from %s: cache file disappeared" \
                  % (self.aipid, cachefile)
            raise PreservationStateError(msg) from ex
        except IOError as ex:
            raise PreservationStateError("Trouble loading preservation state for AIP=%s from %s: %s" %
                                         (self.aipid, cachefile, str(ex))) from ex
        except json.JSONDecodeError as ex:
            raise PreservationStateError("Trouble decoding JSON state for AIP=%s from %s: %s" %
                                         (self.aipid, cachefile, str(ex))) from ex

    def _cache(self):
        try:
            with LockedFile(self._cachefile, 'w') as fd:
                json.dump(self._data, fd, indent=2, cls=_PathTolerantJSONEncoder)
        except FileNotFoundError as ex:
            msg = "Trouble saving preservation state for AIP=%s to %s: directory not found: %s" \
                % (self.aipid, self._cachefile, str(ex))
            raise PreservationStateError(msg) from ex
        except IOError as ex:
            raise PreservationStateError("Trouble saving preservation state for AIP=%s to %s: %s" %
                                         (self.aipid, self._cachefile, str(ex))) from ex
        except TypeError as ex:
            raise PreservationStateError("Trouble encoding JSON state for AIP=%s: %s" %
                                         (self.aipid, str(ex))) from ex

    @property
    def message(self) -> str:
        """
        a message describing current progress in the preservation process.  This can be more 
        fine-grained than the label returned by :py:meth:`completed`.  
        """
        if self._keepfresh:
            self._load()
        return self._data.get("_message", "")

    @property
    def steps_completed(self) -> int:
        """
        a bit array that indicates the preservation steps that have been successfully applied
        """
        if self._keepfresh:
            self._load()
        return self._data.get("_completed", self.UNSTARTED)

    def mark_completed(self, step: int, message: str=None):
        """
        indicate that the given step has been completed.  This is intended to be called by a 
        :py:class:`PreservationStep` when it successfully completes.
        :param int step:  the :py:class:`PreservationCompleted` constant indicating the step that has 
                          been completed.  Multple steps can be so marked by OR-ing them together.  
        :param str message:  If provided, update the a progress message with this string
        """
        if step > self._all_steps:
            raise ValueError(f"mark_complete(): Unrecognized steps included in step code: {step}")
        self._data["_completed"] = self.steps_completed | step

        if message:
            self.record_progress(message)  # calls _cache()
        else:
            self._cache()

        if self._pubstat and step & self.PUBLISHED:
            self._pubstat.update(status.PUBLISHED)   # this will update its message

    def unmark_completed(self, step: int):
        """
        indicate that the given step is being reverted and thus should not be marked as completed.
        If it is so marked, it will be removed.  
        :param int step:  the :py:class:`PreservationCompleted` constant indicating the step that has 
                          been completed.  Multple steps can be so marked by OR-ing them together.  
        """
        if step > self._all_steps:
            raise ValueError(f"mark_complete(): Unrecognized steps included in step code: {step}")
        if self.steps_completed & step > 0:
            self._data["_completed"] = self.steps_completed & ~step
            self.record_progress("reverting step...")

    def get_original_aip(self) -> str:
        """
        return the original location of the submitted AIP.  Typically, the value represents a bag
        root directory; however, in general, it could be a URI interpreted in an implementation-specific
        way.  The AIP's existance at that location depends on the state of the preservation process; it 
        is not guaranteed to exist at this location at the time this function is called.
        """
        return self._data.get("_orig_aip")

    def get_finalized_aip(self) -> str:
        """
        return the location of the finalized AIP--i.e. the location of the AIP that is 
        has been (or will be) finalized prior to validation.  Typically, the value represents a bag
        root directory; however, in general, it could be a URI interpreted in an implementation-specific
        way.  The AIP's existance at that location depends on the state of the preservation process; it 
        is not guaranteed to exist at this location at the time this function is called.
        :return:  the location of the AIP after the finalization step has been applied, or None if it 
                  is not known, yet. 
        """
        if self._keepfresh:
            self._load()
        return self._data.get("_finalized_aip")

    def set_finalized_aip(self, loc: str):
        """
        set the location of the finalized AIP--i.e. the location of the AIP that is 
        has been (or will be) finalized prior to validation.  Typically, the value represents a bag
        root directory; however, in general, it could be a URI interpreted in an implementation-specific
        way.  The AIP's existance at that location depends on the state of the preservation process; it 
        is not required to exist at this location at the time this function is called.
        """
        if loc is not None and not isinstance(loc, str):
            raise TypeError("set_finalized_aip(): loc is not a str")
        self._data["_finalized_aip"] = loc
        self._cache()

    def get_stage_dir(self) -> str:
        """
        return the directory (or other URI-based location) where serialized AIP files will be staged to 
        during the serialization process.  
        """
        return self._data.get("_stage_dir")

    def set_serialized_files(self, aipfiles: Iterable[str]):
        """
        Set the list of files that were (or will be) created from serializing the AIP.

        This is typically called by a AIPSerialization implementation to report where it wrote (or 
        will write) its files.  The AIPArchiving step can then use :py:meth:`get_serialized_files`
        to get the list of files to archive.  This should be a complete list--not a partial one.
        The files are not required to exist at these locations at the time this function is called.  

        :param list aipfiles:  a list of paths (or URIs) pointing to all of the serialized AIP files
                               resulting from the serialization step.
        """
        if aipfiles is not None:
            if isinstance(aipfiles, str) or not isinstance(aipfiles, Iterable):
                raise TypeError("set_serialized_files(): aipfiles not an iterable (of str)")
            elif len(aipfiles) == 0:
                self.log.error("Trying to set an empty list of serialized file (use None, instead)")
                raise ValueError("set_serialized_files(): empty list provided")
            aipfiles = list(aipfiles)
            if any([not isinstance(f, str) for f in aipfiles]):
                raise TypeError("set_serialized_files(): aipfiles not a list of str")

        self._data["_serialized_files"] = aipfiles
        self._cache()

    def get_serialized_files(self) -> List[str]:
        """
        Return the list of files that were (or will be) created from serializing the AIP.  The files'
        existance at these locations depends on the state of the preservation process; they are not 
        guaranteed to all exist at the time it is called.

        This is typically called by a AIPArchiving implementation to get the list of files to archive.  

        :return:  a list of string paths (or URIs) pointing to the complete list of serialized AIP files
                  that resulted from the serialization step.
        """
        if self._keepfresh:
            self._load()
        out = self._data.get("_serialized_files")
        if out is not None:
            out = list(out)
        return out

    def get_state_property(self, name: str, default=None): 
        """
        get an arbitrary property describing some part of the state of the preservation process.  
        This allows two steps in the process (which need not be sequential) to coordinate their 
        behavior. 
        """
        if self._keepfresh:
            self._load()
        return self._data.get(name, default)

    def set_state_property(self, name: str, value):
        """
        set (and persist) an arbitrary property describing some part of the state of the preservation 
        process.  This allows two steps in the process (which need not be sequential) to coordinate 
        their behavior. 
        """
        try:
            json.dumps(value)
        except TypeError as ex:
            raise TypeError("set_state_property(): Not a JSON-supported type for %s: %s" %
                            (name, type(value)))
        self._data[name] = value
        self._cache()

    def record_progress(self, message: str):
        """
        Update the progress message
        """
        if message is None:
            message = ""
        self._data["_message"] = message
        self._cache()

        if self._pubstat:
            self._pubstat.record_progress(message)

    def annotate(self, name, val):
        """
        annotate the publication status with the given data.  This method does nothing if this object
        was not constructed with a SIPStatus instance
        :param str name:  the name to save the data value as.  This will be saved under the (visible) 
                          "user" data by default; if the name starts with "sys:", it will be saved 
                          under the "sys" data that is not visible to API users.  
        :param val:  a JSON-encodable data value 
        """
        if not self._pubstat:
            return
        annot_type = "user"
        if name.startswith("sys:"):
            annot_type = "sys"
            name = name[len("sys:"):]
        self._pubstat.data[annot_type][name] = val
        self._pubstat.cache()

    def get_working_dir(self) -> str:
        """
        return the path to a directory where presevation steps can write intermediated data or 
        custom logs.  (Steps should cleanup unneeded intermediate data during clean-up.)
        :return:  the path to the directory or None if one is not available.
                  :rtype: str
        """
        return self._data.get('_work_dir')
