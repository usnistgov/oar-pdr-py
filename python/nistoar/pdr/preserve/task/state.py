"""
This module provides an implementation of the 
:py:class:`~nistoar.pdr.preserve.task.framework.PreservationStateManager`.
"""
import json
from pathlib import Path
from collections import OrderedDict
from collections.abc import Mapping
from logging import Logger
from typing import List, Iterable, Union

from .framework import PreservationStateManager, UNSTARTED_PROGRESS
from .. import PreservationStateError, PreservationException

from nistoar.pdr.utils.io import LockedFile
from nistoar.base.config import ConfigurationException
from nistoar.pdr.publish.service import status
from nistoar.pdr.utils.io import _PathTolerantJSONEncoder

def _read_state_file(filename: Path, aipid: str=None) -> Mapping:
    if not aipid:
        aipid = '??'
    try:
        with LockedFile(filename) as fd:
            return json.load(fd)
    except FileNotFoundError as ex:
        msg = "Trouble loading preservation state for AIP=%s from %s: cache file not found" \
              % (aipid, filename)
        raise PreservationStateError(msg) from ex
    except IOError as ex:
        raise PreservationStateError("Trouble loading preservation state for AIP=%s from %s: %s" %
                                     (aipid, filename, str(ex))) from ex
    except json.JSONDecodeError as ex:
        raise PreservationStateError("Trouble decoding JSON state for AIP=%s from %s: %s" %
                                     (aipid, filename, str(ex))) from ex

def _write_state_file(state: Mapping, filename: Path, aipid: str):
    try:
        with LockedFile(filename, 'w') as fd:
            json.dump(state, fd, indent=2, cls=_PathTolerantJSONEncoder)
    except FileNotFoundError as ex:
        msg = "Trouble saving preservation state for AIP=%s to %s: directory not found: %s" \
            % (aipid, filename, str(ex))
        raise PreservationStateError(msg) from ex
    except IOError as ex:
        raise PreservationStateError("Trouble saving preservation state for AIP=%s to %s: %s" %
                                     (aipid, filename, str(ex))) from ex
    except TypeError as ex:
        raise PreservationStateError("Trouble encoding JSON state for AIP=%s: %s" %
                                     (aipid, str(ex))) from ex

class JSONPreservationStateManager(PreservationStateManager):
    """
    An implementation of the :py:class:`~nistoar.pdr.preserve.task.framework.PreservationStateManager`
    class in which the state is stored in a JSON files on disk.  The default location for this 
    persisted data is controlled by configuration.  

    This implementation will look for the following properties in the configuration dictionary passed 
    to it:

    ``persist_in`` 
          a file or directory path where this manager can persist its state information 
          (no default).  If the path points to an existing directory, the state will be
          written to a file in that directory with a name of the form _aipid_``_state.json``.  
          Otherwise it will be interpreted as a file path; its parent directory must exist.
    ``stage_dir``        
          a directory path where serialized AIP files can be written prior to archiving.
    ``working_dir``
          a directory path where preservation task steps can write temporary data.  
    ``keep_fresh``
          if True (default), the persisted state will be reloaded often (everytime information 
          is asked for).  Set this to False if it is expected that this instance will be 
          the only one updating this status.  
    """

    @classmethod
    def from_file(cls, statefile: Union[str,Path], logger: Logger=None, pubstat: status.SIPStatus=None, 
                  config: Mapping=None, persistin: Union[str,Path]=None):
        """
        create at state manager instance by loading the preservation state from a given file.  

        :param str|Path statefile:  the persisted preservation state file to load
        :param Logger      logger:  the logger to use in this state manager
        :param SIPStatus  pubstat:  an SIPStatus tracking the overall publishing status; if provided,
                                    it will be updated messages about what's happening with preservation.
        :param dict        config:  an optional configuration, used for filling in any configuration 
                                    information missing from ``statefile``.  
        :param Path     persistin:  the location to persist state information to.  Provide this if 
                                    ``statefile`` should be treated as read-only.  If not provided, 
                                    the input ``statefile`` will be updated as the preservation proceeds.  
        :raises ValueError:  if the state file is missing key data, namely the AIP-ID or the location 
                             of the SIP.  If this file was created by this class, this error should not
                             occur.  
        :raises IOError:  if ``statefile`` cannot be opened to read or if it is not possible to write 
                          to the state file (``persistin``, if provided; otherwise, ``statefile``).
        """
        if isinstance(statefile, str):
            statefile = Path(statefile)
        if not persistin:
            persistin = statefile
        return cls(_read_state_file(statefile), config, logger, pubstat, persistin=persistin)

    @classmethod
    def for_aip(cls, config: Mapping, aipid: str, aiploc: str, logger: Logger=None,
                pubstat: status.SIPStatus=None, clear_state: bool=False):
        """
        Create a state manager to process a particular AIP.  

        Normally, this factory method will look for a persisted state file in a location set by 
        the configuration, and its state will be loaded from there (so that preservation can proceed
        from where it left off).  If the file does not exist or the ``clear_state`` argument is True, 
        a new state file will be created and the returned instance will be set to start preservation 
        from the beginning.  

        :param dict        config:  an optional configuration, used for filling in any configuration 
                                    information missing from ``statefile``.  
        :param str          aipid:  the ID of the AIP to process
        :param str         aiploc:  the location of the input SIP/AIP.  Often, this is a file path, 
                                    but it could be a URL if the task finalization step supports it.
        :param Logger      logger:  the logger to use in this state manager
        :param SIPStatus  pubstat:  an SIPStatus tracking the overall publishing status; if provided,
                                    it will be updated messages about what's happening with preservation.
        :param bool   clear_state:  If True, any previously saved state will be forgotten and the state
                                    will be set to start preservation processing from the beginning.
        :raises ValueError:  if the state file exists but is missing key data.  If the file was created 
                             by this class, this error should not occur.  
        :raises IOError:  if it is not possible to write the persisted state file.
        """
        persistin = config.get("persist_in")
        if not persistin:
            raise ConfigurationException("Missing required config param: persist_in")
        persistin = Path(persistin)
        statefile = persistin / f"{aipid}_state.json" if persistin.is_dir() else persistin
        if not statefile.parent.is_dir():
            raise ConfigurationException(f"persist_in: {str(statefile.parent)}: directory does not exist")

        if not clear_state and statefile.exists():
            state = _read_state_file(statefile, aipid)
        else:
            # create a brand new state
            state = {
                "_aipid": aipid,
                "_orig_sip": aiploc
            }

        return cls(state, config, logger, pubstat, persistin=statefile)

    def __init__(self, statedata: Mapping, config: Mapping=None, logger: Logger=None,
                 pubstat: status.SIPStatus=None, persistin: Path=None):
        """
        Create the state manager. 

        Typically, this constructor is not called directly; rather, an instance is created via either
        of the factory methods, :py:meth:`from_file` or :py:meth:`for_aip`.  

        :param dict statedata:  the preservation state data 
        :param dict    config:  the dictionary for configuring this instance
        :param Logger  logger:  the logger to use in this state manager
        :param SIPStatus stat:  an SIPStatus tracking the overall publishing status; if provided,
                                it will be updated messages about what's happening with preservation.
        :param Path persistin:  the location to persist state information to; this overrides the 
                                value of ``persist_in`` in the configuration.  This location must be 
                                provided either via this argument or in the configuration.
        """
        if config is None:
            config = {}
        self.cfg = config
        self._data = statedata
        self._keepfresh = self.cfg.get("keep_fresh", True)
        self._pubstat = pubstat

        aipid = self._data.get("_aipid")
        if not aipid:
            raise ValueError("JSONPreservationStateManager: state data is missing _aipid")
        if not self._data.get("_orig_sip"):
            raise ValueError("JSONPreservationStateManager: state data is missing _orig_sip")

        super(JSONPreservationStateManager, self).__init__(aipid, logger)

        self._data.setdefault("_stage_dir", self.cfg.get("stage_dir"))
        self._data.setdefault("_work_dir", self.cfg.get("working_dir"))
        self._data.setdefault("_completed", self.UNSTARTED)
        self._data.setdefault("_message", UNSTARTED_PROGRESS)

        if not persistin:
            persistin = self.cfg.get("persist_in")
            if isinstance(persistin, str):
                persistin = Path(persistin)
        if not persistin:
            raise ConfigurationException("JSONPreservationStateManager: missing parameter, persist_in")
        if persistin.is_dir():
            persistin = persistin / f"{aipid}_state.json"
        elif not persistin.parents[0].is_dir():
            raise PreservationStateError("Preservation state file's parent is not an existing directory: "
                                         + str(persistin.parents[0]))
        self._cachefile = persistin
        self._cache()

    def _load(self, cachefile=None):
        if not cachefile:
            cachefile = self._cachefile
        self._data = _read_state_file(cachefile, self.aipid)

    def _cache(self):
        _write_state_file(self._data, self._cachefile, self.aipid)

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

    def get_sip(self) -> str:
        """
        return the original location of the submitted SIP.  Typically, the value represents a bag
        root directory; however, in general, it could be a URI interpreted in an implementation-specific
        way.  The AIP's existance at that location depends on the state of the preservation process; it 
        is not guaranteed to exist at this location at the time this function is called.
        """
        return self._data.get("_orig_sip")

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
                            (name, type(value))) from ex
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
