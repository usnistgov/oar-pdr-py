"""
This module provides the base classes that define a framework for managing a preservation task.
The Preservation Task Framework provides a model for how the preserving of products is organized.  
The process can be a long-running one which can experience various errors along the way.  This framework
allows one to execute the process in a variety ways--such as, synchronously via the command line or 
asynchronously via a web service.  If the process fails along the way, the framework can allow the process
to be resumed at the appropriate point after the failure conditions have been corrected without starting 
over.  

The preservation process accepts as its input a Submission Information Package (SIP); typically, this 
takes the form of a proto-BagIt bag (but this can depend on the implementation).  The process is defined 
by the following steps, each represented by a pluggable interface:
  1. :py:class:`AIPFinalization` -- transforming the input SIP (the proto-bag) into a complete AIP; this
     often adding required administrative artifacts and other tweaks to the content of the bag.  
  2. :py:class:`AIPValidation` -- running a series of validation checks to ensure that the AIP meets the 
     requirements for completeness.
  3. :py:class:`AIPSerialization` -- converting the AIP into one or more archivable files
  4. :py:class:`AIPArchiving` -- committing the serialized bag files to long-term storage
  5. :py:class:`AIPPublication` -- releasing the AIP to external systems, namely to a repository system 
     through which the AIP can be accessed.  
  6. :py:class:`AIPCleanup` -- removing any remaining preservation artifacts and possibly the input SIP.

These steps are managed via an instance of the :py:class:`PreservationTask` class, calling each step 
in sequence.  The state of a task--which steps have been completed and where to find intermediate 
products--is handled by a :py:class:`PreservationStateManager` instance which is passed into the step
when it is executed.  
"""
import logging, os
from collections.abc import Mapping
from abc import ABCMeta, abstractmethod, abstractproperty
from logging import Logger
from typing import List
from copy import deepcopy

from nistoar.pdr.preserve import PreservationException, system as preserve_system
from nistoar.base.config import merge_config
from nistoar.pdr.notify import NotificationService

UNSTARTED_PROGRESS = "waiting to start preservation"

class PreservationStepsAware:
    UNSTARTED  =  0     # preservation of the AIP has not yet been started
    STARTED    =  1     # preservation finalization has started
    FINALIZED  =  2     # the AIP has been finalized for preservation
    VALIDATED  =  4     # the AIP has been found to be valid and ready for preservation
    SERIALIZED =  8     # the AIP has been serialized
    SUBMITTED  = 16     # the serialized AIP files have been submitted for migration to long-term storage
    ARCHIVED   = 32     # the migration to long-term storage is complete
    PUBLISHED  = 64     # the AIP has been released to external services

    _last_step = PUBLISHED
    _all_steps = (_last_step << 1) - 1
    _step_label = {
        UNSTARTED  : "unstarted",
        STARTED    : "started",
        FINALIZED  : "finalized",
        VALIDATED  : "validated",
        SERIALIZED : "serialized",
        SUBMITTED  : "submitted to archive",
        ARCHIVED   : "archived",
        PUBLISHED  : "published"
    }

    def _last_step_in(cls, state):
        step = cls._last_step
        while step > 0 and step & state == 0:
            step >>= 1
        return step

    def _label_for_step(cls, state):
        return cls._step_label[cls._last_step_in(state)]

    def _all_steps_completed(cls, state):
        return (cls._all_steps & state) == cls._all_steps
        
class PreservationStateManager(PreservationStepsAware, metaclass=ABCMeta):
    """
    a class that tracks the state of the preservation process of an AIP.  It not only encapsulates 
    the AIP being preserved but also the mechanisms for persisting the state of the preservation,
    including how intermediate products are managed.  

    An instance of this class is used to coordinate the different steps in a :py:class:`PreservationTask`
    by passing information between steps.  Some of the information includes storage locations of 
    data or "directories" where data can be written to.  Typically, these locations are interpreted as 
    a filesystem path; however, an implemenation may express them as URIs (which must be properly formatted
    as a URI).  Such URI-based implementations will need to be paired with :py:class:`PreservationTask`
    implementations that can utilized such locations.

    .. seealso:: implementations :py:mod:`~nistoar.pdr.preserve.task.state`
    """

    def __init__(self, aipid: str, logger: Logger=None):
        """
        instantiate the state manager.  
        :param str      aipid:  the identifier of the AIP to preserve.  The ``config`` must 
                                indicate where this AIP is located.
        :param Logger  logger:  the logger to use during preservation.
        """
        if not logger:
            logger = preserve_system.getSysLogger().getChild(aipid)
        self._log = logger
        self._aipid = aipid

    @property
    def aipid(self) -> str:
        """
        the identifier for the AIP being preserved.
        """
        return self._aipid

    @property
    def log(self) -> Logger:
        """
        the Logger that can be used to record message from the preservation process
        """
        return self._log

    @log.setter
    def log(self, logger: Logger):
        self._log = logger

    @abstractproperty
    def message(self) -> str:
        """
        a message describing current progress in the preservation process.  This can be more 
        fine-grained than the label returned by :py:meth:`completed`.  
        """
        raise NotImplementedError()

    @abstractproperty
    def steps_completed(self) -> int:
        """
        a bit array that indicates the preservation steps that have been successfully applied
        """
        raise NotImplementedError()

    @property
    def completed(self) -> str:
        """
        a label indicating the latest completed stage of preservation
        """
        return self._label_for_step(self.steps_completed)

    @property
    def all_completed(self) -> bool:
        """
        True if all preservation steps have been completed
        """
        return self._all_steps_completed(self.steps_completed)

    @abstractmethod
    def mark_completed(self, step: int, message=None):
        """
        indicate that the given step has been completed.  This is intended to be called by a 
        :py:class:`PreservationStep` when it successfully completes.
        :param int step:  the :py:class:`PreservationCompleted` constant indicating the step that has 
                          been completed.  Multple steps can be so marked by OR-ing them together.  
        :param str message:  If provided, update the a progress message with this string
        """
        raise NotImplementedError()

    @abstractmethod
    def unmark_completed(self, step: int):
        """
        indicate that the given step is being reverted and thus should not be marked as completed.
        If it is so marked, it will be removed.  
        :param int step:  the :py:class:`PreservationCompleted` constant indicating the step that has 
                          been completed.  Multple steps can be so marked by OR-ing them together.  
        """
        raise NotImplementedError()

    @abstractmethod
    def _load(self):
        """
        load the state from its persistent stroage
        """
        raise NotImplementedError()

    @abstractmethod
    def _cache(self):
        """
        load the state from its persistent stroage
        """
        raise NotImplementedError()

    @abstractmethod
    def get_sip(self) -> str:
        """
        return the location of the submitted SIP.  Typically, the value represents a bag
        root directory; however, in general, it could be a URI interpreted in an implementation-specific
        way.  The SIP's existance at that location depends on the state of the preservation process; it 
        is not guaranteed to exist at this location at the time this function is called.
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def set_finalized_aip(self, loc):
        """
        set the location of the finalized AIP--i.e. the location of the AIP that is 
        has been (or will be) finalized prior to validation.  Typically, the value represents a bag
        root directory; however, in general, it could be a URI interpreted in an implementation-specific
        way.  The AIP's existance at that location depends on the state of the preservation process; it 
        is not required to exist at this location at the time this function is called.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_stage_dir(self) -> str:
        """
        return the directory (or other URI-based location) where serialized AIP files will be staged to 
        during the serialization process.  
        """
        raise NotImplementedError()

    @abstractmethod
    def set_serialized_files(self, aipfiles: List[str]):
        """
        Set the list of files that were (or will be) created from serializing the AIP.

        This is typically called by a AIPSerialization implementation to report where it wrote (or 
        will write) its files.  The AIPArchiving step can then use :py:meth:`get_serialized_files`
        to get the list of files to archive.  This should be a complete list--not a partial one.
        The files are not required to exist at these locations at the time this function is called.  

        :param list aipfiles:  a list of paths (or URIs) pointing to all of the serialized AIP files
                               resulting from the serialization step.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_serialized_files(self) -> List[str]:
        """
        Return the list of files that were (or will be) created from serializing the AIP.  The files'
        existance at these locations depends on the state of the preservation process; they are not 
        guaranteed to all exist at the time it is called.

        This is typically called by a AIPArchiving implementation to get the list of files to archive.  

        :return:  a list of string paths (or URIs) pointing to the complete list of serialized AIP files
                  that resulting from the serialization step.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_state_property(self, name: str, default=None): 
        """
        get an arbitrary property describing some part of the state of the preservation process.  
        This allows two steps in the process (which need not be sequential) to coordinate their 
        behavior. 
        """
        raise NotImplementedError()

    @abstractmethod
    def set_state_property(self, name: str, value):
        """
        set (and persist) an arbitrary property describing some part of the state of the preservation 
        process.  This allows two steps in the process (which need not be sequential) to coordinate 
        their behavior. 
        """
        raise NotImplementedError()

    @abstractmethod
    def record_progress(self, message: str):
        """
        Update the progress message
        """
        raise NotImplementedError()

    @abstractmethod
    def get_working_dir(self) -> str:
        """
        return the path to a directory where presevation steps can write intermediated data or 
        custom logs.  (Steps should cleanup unneeded intermediate data during clean-up.)
        :return:  the path to the directory or None if one is not available.
                  :rtype: str
        """
        raise NotImplementedError()


class PreservationStep(metaclass=ABCMeta):
    """
    an abstract interface for one step in the preservation process.  Specific steps are typically
    derived from this interface, from which particular implementations of those steps are derived.

    To apply a step, an instance is passed via its functions a PreservationStateManager instance
    which encapsulates the AIP being preserved as well as the state of its progress
    """
    def __init__(self, config: Mapping=None):
        """
        instantiate the validator step.
        :param dict config:  the configuration for this step; if not provided, defaults will apply.
        """
        if config is None:
            config = {}
        self.cfg = config
    
    def run(self, statemgr: PreservationStateManager, notifier: NotificationService=None,
            ignore_cleanup_error: bool=True, **kw):
        """
        Apply the preservation processing step.  This is normally done by first calling 
        :py:meth:`revert` to clean up from a previously (failed) attempt, then 
        :py:meth:`apply`, followed by (if :py:meth:`apply` is successful) :py:meth:`clean_up`.  
        Consequently, one should expect that the outcome from any previous runs of this step 
        will be undone or otherwise obliterated.

        :param PreservationStateManager statemgr:  the state manager that encapsulates the AIP 
                                       being preserved and the state of its progress.
        :param NotificationService notifier:  the notification service the step may use to 
                 send out alerts about issues needing attention (by a person).  This is passed to 
                 the :py:meth:`apply` method whose implementation may or may not choose to use it
                 (see :py:meth:`apply` for more details).
        :param bool ignore_cleanup_error:  if True (default), any errors that occur while calling 
                                       :py:meth:`clean_up` will be caught.  Regardless of this 
                                       value, the exception will be logged as a warning.  
        :param kw:  extra implementation-specific keyword arguments.  
        :raise PreservationException:  if an error occurred preventing the completion of this
                                       step.
        """
        self.revert(statemgr)
        self.apply(statemgr, notifier, **kw)
        try:
            self.clean_up(statemgr)
        except PreservationException as ex:
            self._report_cleanup_failure(statemgr, ex)
            if not ignore_cleanup_error:
                raise

    @abstractmethod
    def apply(self, stagemgr: PreservationStateManager, notifier: NotificationService=None, **kw):
        """
        Apply this preservation step to the target AIP.  

        One should expect that the outcome from any previous runs of this step will be overwritten.  
        The implementation may support extra custom arguments; generally, such arguments would be 
        provided when this function is called outside of the normal :py:class:`PreservationTask`
        workflow.

        :param PreservationStateManager stagemgr:  the state manager the step should use to get 
                 the current state of the preservation
        :param NotificationService      notifier:  the notification service the step may use to 
                 send out alerts about issues needing attention (by a person).  Any exception 
                 raised by this step will automatically result in a notification alert sent out 
                 by the executing :py:class:`PreservationTask`, so a step need only make use of 
                 this notifier for issues that need attention but should not stop processing (i.e.
                 does not raise an exception for).
        :param **kw:  additional implementation-specific arguments
        
        :raise PreservationException:  if an error occurred preventing the application of this step.
        """
        raise NotImplementedError()

    @abstractmethod
    def revert(self, statemgr: PreservationStateManager) -> bool:
        """
        If possible, undo this preservation step.  If it cannot be (fully) undone by design, this 
        function will return without raising an exception.
        :return:  False if this step cannot be undone even partially
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        raise NotImplementedError()

    @abstractmethod
    def clean_up(self, statemgr: PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  
        :raise PreservationException:  if an error occurred preventing the application of this step.
        """
        raise NotImplementedError()

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during preservation step clean-up (%s): %s",
                                 type(ex).__name__, str(ex))

    
class AIPFinalization(PreservationStep):
    """
    The interface for transforming the submitted SIP into a complete AIP.  last-minute tweaks to the 
    content of the AIP, but it also may involve extracting key information that will be needed in the
    :py:class:`AIPPublication` step. 

    This implementation can be instantiated and used to apply the null operation for finalization, 
    but it should also be used as a base class for specific finalization implementations.
    """

    def apply(self, stagemgr: PreservationStateManager, notifier: NotificationService=None, **kw):
        """
        Apply the finalization steps.  This implementation does nothing.
        :raise AIPFinalizationException:  if an error occurred preventing the finalization
        """
        pass

    def revert(self, statemgr: PreservationStateManager) -> bool:
        """
        If possible, undo the finalization step.  Since :py:meth:`apply` does nothing, this 
        implementation does nothing but return True.
        :return:  False if this step cannot be undone even partially.
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        statemgr.unmark_completed(statemgr.FINALIZATION)
        return True

    def clean_up(self, statemgr: PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  Since :py:meth:`apply` 
        does nothing, this implementation does nothing.  
        :raise PreservationException:  if an error occurred while trying to clean-up this step
        """
        pass

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during finalization clean-up (%s): %s",
                                 type(ex).__name__, str(ex))

class AIPValidation(PreservationStep):
    """
    An abstract interface for validating an AIP's readiness for serialization and subsequent 
    archiving.  Implementations encapsulate a set of requirements that the underlying AIP 
    must meet.  

    Note that the :py:meth:`apply` method should raise an 
    :py:class:`~nistoar.pdr.preserve.AIPValidationError` exception if the AIP does not meet 
    its validation requirements and :py:class:`AIPValidationException` if a failure occurs 
    while trying to apply the process itself.  
    """
    
    def revert(self, statemgr: PreservationStateManager) -> bool:
        """
        If possible, undo the finalization step.  As validation is 
        typically read-only, this default implementation does nothing but return True.
        :return:  False if this step cannot be undone even partially.
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        statemgr.unmark_completed(statemgr.VALIDATED)
        return True

    def clean_up(self, statemgr: PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  As validation is 
        typically read-only, this default implementation does nothing.  
        :raise PreservationException:  if an error occurred while trying to clean-up this step
        """
        pass

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during validation clean-up (%s): %s",
                                 type(ex).__name__, str(ex))

class AIPSerialization(PreservationStep):
    """
    an abstract interface for serializing an AIP into one or more archivable files.  Subclasses
    implement a particular strategy for the serialization.

    In addition to implementation-specific parameters, this abstract class supports the following
    general parameter:

    ``always_validate``
        (bool) _optional_.  If True (default), always run the validation step before running 
        this serialization step, regardless of whether the finalized bag successfully passed 
        validation before.  This sets the :py:attr:`always_validate` property and only applies 
        when this step is run within a :py:class:`PreservationTask`.  If False, this step will 
        only be applied if the step is marked as uncompleted.
    """

    @property
    def always_validate(self):
        """
        a flag indicating whether the validation step should always be run before this 
        serialization step. 

        This behavior only applies when this step is executed as part of a 
        :py:class:`PreservationTask` and generally becomes relevent when restarting the task 
        after a failure during the serialization step: after the failure conditions are rectified,
        this flag ensures that validation is re-run before serialization.  If this flag is False, 
        validation will only be applied if it is marked as uncompleted.

        This flag is set at construction time via the configuration parameter, 
        ``always_validate``.
        """
        return self.cfg.get('always_validate', True)

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during serialization clean-up (%s): %s",
                                 type(ex).__name__, str(ex))

class AIPArchiving(PreservationStep):
    """
    an abstract interface for migrating serialized AIP files to long-term storage.  
    """

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during archiving clean-up (%s): %s",
                                 type(ex).__name__, str(ex))


class AIPPublication(PreservationStep):
    """
    an abstract interface for releasing an AIP that has been archived to external systems, namely a
    repository system through which the AIP can be discovered and accessed.  
    """
    
    def revert(self, statemgr: PreservationStateManager) -> bool:
        """
        If possible, undo the publication step.  This implementation does nothing but return True.
        :return:  False if this step cannot be undone even partially.
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        statemgr.unmark_completed(statemgr.PUBLISHED)
        return True

    def clean_up(self, statemgr: PreservationStateManager):
        """
        Clean up any unneeded state that was created while executing this step.  
        This implementation does nothing.  
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        pass

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during publication clean-up (%s): %s",
                                 type(ex).__name__, str(ex))

class AIPCleanup(PreservationStep):
    """
    the final preservation step after publication that cleans up any remaining preservation 
    artifacts as well as possibly the input SIP.  

    This step is often different from the other steps in that generally it cannot be reverted.  
    Further, it should be implemented to be able to run multiple times without error whether 
    there are things to clean up or not.  In particular, the :py:class:`PreservationTask`'s 
    ``run()`` method will always run this clean-up step even if the publish step has already 
    been completed.  This step's :py:meth:`apply` method should support a ``cancel`` keyword 
    argument.  

    This default implementation will only delete the staged serialized bags, if they exist.
    """
    def apply(self, statemgr: PreservationStateManager, notifier: NotificationService=None,
              cancel=False, *kw):
        """
        Apply the final clean-up chores

        This default implementation will delete any serialized preservation bags created 
        from the input SIP from the staging area.  

        :param PreservationStateManager statemgr:  the state manager that can help guide the 
               final clean-up. 
        :param bool cancel:  If True, assume that the clean-up is due to a request to cancel 
               a previous preservation attempt such that the next time the preservation is 
               restarted, it should start from the very beginning.  This means that the input
               SIP should remain intact.  The preservation state should be reset accordingly.  
               If False (default), the implementation should feel free to delete the input SIP.  
        """
        staged = statemgr.get_serialized_files()
        if self.delete_files(staged, statemgr, "serialized bags"):
            statemgr.set_serialized_files(None)


    def delete_files(self, filelist: List[str], statemgr: PreservationStateManager,
                     what: str=None, log: Logger=None):
        """
        delete a list of files and complain about any problems

        :param list(str) filelist:  the list of file names to delete
        :param PreservationStateManager stagemgr:   the preservation state manager
        :param str what:  a phrase indicating what is in the list; used in log messages when 
                          files fail to delete (default: "files")
        """
        if not what:
            what = "files"

        failed = {}
        if filelist:
            for f in filelist:
                if os.path.exists(f):
                    try:
                        os.remove(f)
                    except Exception as ex:
                        failed[f] = str(ex)

        if failed:
            if not log:
                log = statemgr.log.getChild("clean-up")
            if len(failed) == len(filelist):
                msg = list(failed.values())[0]
                log.error("Unable to delete %s: (e.g.) %s", what, msg)

            elif len(failed) == 1:
                f = list(failed.items())[0]
                log.warning("Unable to delete a serialized bags: %s: %s", what, f[0], f[1])

            else:
                msgs = [ f"  {f[0]}: {f[1]}" for f in failed.items() ]
                log.error(f"Unable to delete some {what}:\n" + "\n".join(msgs))

        return not failed

        
    def revert(self, statemgr: PreservationStateManager) -> bool:
        """
        This implementation does nothing because this step generally cannot be reverted, and 
        so it returns True.
        """
        return False

    def clean_up(self, statemgr: PreservationStateManager):
        """
        This does nothing as this step should not create any artifacts to clean_up.
        This implementation does nothing.  
        :raise PreservationException:  if an error occurred while trying to undo the step
        """
        pass

    def _report_cleanup_failure(self, statemgr: PreservationStateManager, ex: Exception):
        if statemgr.log:
            statemgr.log.warning("Failure during publication clean-up (%s): %s",
                                 type(ex).__name__, str(ex))


class PreservationTask(PreservationStepsAware):
    """
    a class representing the task of preserving a specific AIP that encapsulates the steps in the 
    process.  Each step is pluggable via the constructor.  This task keeps track of which steps have 
    been completed.

    This class is designed to support different strategies for preserving different kinds of AIPs, 
    separating that from how a preservation process is executed and managed.  The strategy for a
    particular preservation task is provided via the :py:class:`PreservationStep` instances 
    injected via the constructor.  A :py:class`PreservationStateManager` instance is used to 
    coordinate the execution of those steps.  A ``PreservationTask`` is normally created via a 
    :py:class:`PreservationTaskFactory`.  The design also allows for PreservationTasks to be resumed 
    after failures (and the source of the failure has been fixed). 
    """
    def __init__(self, mgr: PreservationStateManager, finalizer: AIPFinalization,
                 validater: AIPValidation, serializer: AIPSerialization, archiver: AIPArchiving,
                 publisher: AIPPublication, cleaner: AIPCleanup=None, notifier: NotificationService=None):
        """
        initialize the task with its pluggable components.  Clients normally do not instantiate a 
        task directly, but rather call :py:meth:`PreservationTaskFactory.create_task` on a 
        :py:class:`PreservationTaskFactory`.  
        """
        self._statemgr = mgr
        self._finalizer = finalizer
        self._validater = validater
        self._serializer = serializer
        self._archiver = archiver
        self._publisher = publisher

        if not cleaner:
            cleaner = self._create_default_cleaner()
        self._cleaner = cleaner

        self._notifier = notifier

    def _create_default_cleaner(self):
        return AIPCleaner()
        
    @property
    def aipid(self):
        return self._statemgr.aipid

    def finalize(self) -> bool:
        """
        If it has not already been done, finalize the AIP in preparation for full preservation.

        This represents the first step in the preservation process.  If this step was already 
        completed, the function returns immediately.

        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously, or False because it was already completed.  
                  :rtype: bool
        :raises AIPFinalizationException:  if the process fails to complete this finalization step
                  successfully.  
        """
        if self.finalized():
            return False
        self._finalizer.run(self._statemgr, self._notifier)
        return True

    def finalized(self) -> bool:
        """
        return True if the AIP has been finalized and is ready to be serialized.
        """
        return bool(self._statemgr.steps_completed & self.FINALIZED)

    def validate(self, as_is: bool=True, always_apply=True) -> bool:
        """
        Validate that the AIP is ready for preservation.  This is the second step in the preservation
        process, coming after finalization.  

        :param bool as_is:  if False, ensure that the AIP has been finalized first; otherwise,
                  the validater will be forced to run on the AIP without regard to its state. 
        :param bool always_apply:  if True (default), always execute the validation prior to 
                  serialization; if False, validation will only be executed if it hasn't be 
                  previously marked as completing validation.  
        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously or becaues it was forced to by request via ``as_is``; otherwise, False 
                  is returned.
                  :rtype: bool
        :raises AIPValidationException:  if the process fails to complete this validation step
                  successfully.  
        :raises AIPValidationError:  if the AIP was found to be invalid or otherwise did not meet the 
                  requirements for preservation
        """
        if not as_is:
            if always_apply:
                self._statemgr.unmark_completed(self._statemgr.VALIDATED)
            elif self.validated():
                return False
            self.finalize()
        self._validater.run(self._statemgr, self._notifier)
        return True

    def validated(self) -> bool:
        """
        return True if the AIP is considered currently valid according to the configured preservation
        requirements.  

        Note that this task may configured to always run the validater regardless before
        serialization; if this is the case, this will always return False.
        """
        return bool(self._statemgr.steps_completed & self.VALIDATED)
            
    def serialize(self) -> bool:
        """
        Complete all preservation steps up through serialization.  If the AIP has already been 
        serialized completely, this function returns immediately.  

        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously, or False because it was already completed.  
                  :rtype: bool
        :raise AIPSerializationException:  if an error occurs during serialization
        """
        if self.serialized():
            return False
        self.validate(as_is=False, always_apply=self._serializer.always_validate)
        self._serializer.run(self._statemgr, self._notifier)
        return True

    def serialized(self) -> bool:
        """
        return True if the AIP has been completed serialized and is ready to be archived.
        """
        return bool(self._statemgr.steps_completed & self.SERIALIZED)

    def archive(self) -> bool:
        """
        Complete all preservation steps up through archiving.  

        Archiving--the process of committing the serialized AIP to long-term storage--is itself 
        inherently asynchronous, so this function will start that process.  If the AIP has already 
        been submitted for archiving, this function returns immediately.  

        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously, or False because it was already completed.  
                  :rtype: bool
        :raise AIPArchivingException:  if an error occurred will initiating the archiving
        """
        if self.submitted_to_archive():
            return False
        self.serialize()
        self._archiver.run(self._statemgr, self._notifier)
        return True

    def submitted_to_archive(self):
        """
        return True if this AIP has been serialized and submitted to long-term storage.  (The 
        transfer may still be underway.)
        """
        return self._statemgr.steps_completed & self.SUBMITTED > 0

    def archived(self):
        """
        return True if this AIP has been completed migrated to long-term storage.  
        """
#        return self._archiver.transfer_complete()
        return self._statemgr.steps_completed & self.ARCHIVED > 0

    def publish(self, ensure_archived=True):
        """
        Complete all preservation steps up through publishing.  If the AIP has already been published,
        this function returns immediately.  

        :param bool ensure_archived:  if True (default), make sure that the archiving process has 
                  been completed before publishing; if it is not, an Excpetion raised  If False, 
                  attempt to release the AIP while archiving is underway.
        :return:  True if it was necessary to execute the step because it had not be carried out 
                  previously, or False because it was already completed.  
                  :rtype: bool
        :raise AIPPublicationException:  if a fatal failure occurs while attempting to publish the AIP,
                  or if ``ensure_archived`` was True and archiving is not yet complete.
        """
        if self.published():
            return False
        self.archive()
        if ensure_archived and not self.archived():
            raise AIPPublicationException("Archiving has not completed", id)
        self._publisher.run(self._statemgr, self._notifier)
        return True

    def published(self) -> bool:
        """
        return True if the AIP has completed the publication step (and, thus, the entire process).
        """
        return bool(self._statemgr.steps_completed & self.PUBLISHED)

    def completion_clean_up(self, cancel: bool=False):
        """
        clean up the preservation input and any remaining artifacts after all preservations steps 
        have been completed.  

        This commonly means cleaning up the submitted SIP as well as any remaining serialized AIPs.  

        :param bool cancel: if False (default), an exception will be raised if all of the 
                            preservation have not been completed.  If True, it is assumed that 
                            this incomplete task is being canceled: remaining artifacts will be 
                            purged but the input SIP will not.  
        """
        # NOTE: Is it possible for the task to legitimately complete the publishing step
        # but not previous ones (presumably via admin override or otherwise manual handling)?
        # If so, requiring all_completed() may be problematic.
        if not cancel and not self._statemgr.all_completed:
            raise PreservationTaskException("Preservation has not completed", id)
        if self._cleaner:
            self._cleaner.run(self._statemgr, self._notifier, cancel)

    def run(self):
        """
        execute this preservation task to completion and clean-up.  

        This run by the presrevation service system to commence the preservation process. 

        :return:  False if the task has already be run to completion and there is nothing left 
                  to do; otherwise True is returned.
        """
        try:
            doing = "Starting"
            if self._statemgr.steps_completed > self._statemgr.UNSTARTED:
                doing = "Restarting"
            msg = f"{doing} preservation task"
            self._statemgr.mark_completed(self._statemgr.STARTED, msg)
            self._statemgr.log.info(msg)
            
            if not self._statemgr.all_completed:
                self.publish()

            if self._notifier:
                # celebrate!
                self._notifier.alert("preserv.success", "New DAP published: "+self._statemgr.aipid,
                                     id=self._statemgr.aipid,
                                     version=self._statemgr.get_state_property('version', '??'))

        except Exception as ex:
            if self._notifier:
                # admit failure
                if isinstance(ex, PreservationTaskFailure):
                    notify_as = ex.notify_as or "preserve.failure"
                    props = ex.props or {}
                    self._notifier.alert(notify_as, str(ex), ex.errors, ex.step,
                                         id=ex.aipid or self._statemgr.aipid, **props,
                                         version=self._statemgr.get_state_property('version', '??'))
                elif isinstance(ex, PreservationException):
                    self._notifier.alert("preserve.failure", str(ex), ex.errors,
                                         getattr(ex, 'step', None),
                                         id=ex.aipid or self._statemgr.aipid,
                                         version=self._statemgr.get_state_property('version', '??'))
                elif isinstance(ex, ConfigurationException):
                    self._notifier.alert("preserve.failure",
                                         "Preservation failed due to task configuration problem",
                                         [str(ex)], id=self._statemgr.aipid,
                                         version=self._statemgr.get_state_property('version', '??'))
                else:
                    self._notifier.alert("preserve.failure",
                                         "Preservation failed due to unexpected error",
                                         [str(ex)], id=self._statemgr.aipid,
                                         version=self._statemgr.get_state_property('version', '??'))
            raise
                
        try:
            self.completion_clean_up()
        except Exception as ex:
            if self._notifier:
                self._notifier.alert("cleanup.failure", "Problem during presrevation task clean-up",
                                     [str(ex)], id=self._statemgr.aipid)
            raise

    @property
    def completed(self):
        """
        True if this task has completed all its steps
        """
        return self._statemgr.all_completed

    @property
    def started(self):
        """
        True if this task has completed all its steps
        """
        return self._statemgr.steps_completed > self._statemgr.UNSTARTED

class PreservationTaskFactory(metaclass=ABCMeta):
    """
    an interface for creating a :py:class:`PreservationTask` injected with necessary 
    :py:class:PreservationStep instances as specified by a given configuration.

    The factory methods require a configuration dictionary that complies with an expected 
    base structure that includes the following properties:

    ``state_manager``
         properties that configure the :py:class:`PreservationStateManager` to use
         within the task.  
    ``finalize``:       
         properties that configure the :py:class:`AIPFinalizaiton` instance to use
    ``validate``       
         properties that configure the :py:class:`AIPValidation` instance to use
    ``serialize``
         properties that configure the :py:class:`AIPSerialization` instance to use
    ``archive``
         properties that configure the :py:class:`AIPArchiving` instance to use
    ``publish``
         properties that configure the :py:class:`AIPPublication` instance to use
    ``cleanup``
         properties that configure the :py:class:`AIPCleanup` instance to use

    Each of the above properties have dictionary values which can include the ``type`` property.
    This property identifies the class that should be instantiated to handle its part of the 
    preservation process.  The value expected--in particular, whether this it is a label that 
    maps to a class, is an explicit python class name, etc.--is implementation dependent.  (A
    factory implementation may limit which classes are supported.)  If the ``type`` property is not 
    provided, the factory may assume a default.  All other subproperties expected depends on the 
    component class implementation.

    Note that the factory takes responsibility for sharing subproperties across the top-level 
    dictionaries to ensure that the steps work together--e.g. that a step can find the outputs of 
    the previous step.  
    """
    def __init__(self, config: Mapping=None):
        """
        initialize the factory.  

        The given configuration will be treated as the default configuration.  It will be 
        merged with the configuration provided to the factory method, :py:meth:`create_task`,
        with the latter taking precedence.  
        """
        self.cfg = config

    def create_task(self, sipstate: PreservationStateManager,
                    logger: Logger=None, config: Mapping=None) -> PreservationTask:
        """
        create the fully configured :py:class:`PreservationTask` that can preserve the SIP 
        referenced in the given preservation state manager.  

        :param PreservationStateManager sipstate:  the :py:class:`PreservationStateManager`
                 instance for a particular SIP needing conversion to an AIP, preservation, 
                 and ingestion.  This state can be brand new for an SIP in "raw" form, or it
                 can be one reflecting a partially processed SIP that needs to be completed 
                 fully (say, after a previous failure).  
        :param Logger logger:  the logger that the resulting task should use for messages 
                 during processing.  If not provided, a default will be used (which may be 
                 one already attached to the manager instance, ``sipstate``). 
        :param dict config:  the configuration that controls construction of this task and
                 the behavior of the steps.  This configuration will be merged with the 
                 default configuration set at this factory's construction.  
        :return:  the configured :py:class:`PreservationTask`
        :raise ConfigurationException:  if the task cannot be created due to insufficient or 
                                incorrect configuration
        :raise PreservationException:  if any other failure occurs while assembling the task.
        """
        if config is None:
            config = {}
        config = merge_config(config, deepcopy(self.cfg))

        if logger:
            sipstate.log = logger
        elif not sipstate.log:
            sipstate.log = preserve_system.getSysLogger().getChild("task").getChild(sipstat.aipid)

        steps = []
        steps.append(self._create_finalizer(config.get('finalize')))
        steps.append(self._create_validater(config.get('validate')))
        steps.append(self._create_serializer(config.get('serialize')))
        steps.append(self._create_archiver(config.get('archive')))
        steps.append(self._create_publisher(config.get('publish')))
        steps.append(self._create_cleaner(config.get('cleanup')))
        
        notifier = None
        if config.get('notify'):
            nostifier = NotificationService(config['notify'])

        return PreservationTask(sipstate, *steps, notifier=notifier)

    @abstractmethod
    def _create_finalizer(self, config: Mapping) -> AIPFinalization:
        raise NotImplementedError()

    @abstractmethod
    def _create_validater(self, config: Mapping) -> AIPValidation:
        raise NotImplementedError()

    @abstractmethod
    def _create_serializer(self, config: Mapping) -> AIPSerialization:
        raise NotImplementedError()

    @abstractmethod
    def _create_archiver(self, config: Mapping) -> AIPArchiving:
        raise NotImplementedError()

    @abstractmethod
    def _create_publisher(self, config: Mapping) -> AIPPublication:
        raise NotImplementedError()
        
    def _create_cleaner(self, config: Mapping) -> AIPCleanup:
        # returning None will cause the minimal default implementation to be used
        return None
        

class PreservationTaskException(PreservationException):
    """
    a base class for exceptions that occur while executing a preservation task.
    """
    def __init__(self, msg: str=None, aipid: str=None, step: str=None, errors: List[str]=None,
                 cause=None):
        """
        create the exception, optionally list things that went wrong for the AIP
        :param str     msg:  a general message describing the failure
        :param str   aipid:  the ID of the AIP being processed
        :param str    step:  the name of the step in the preservation task where the 
                             exception occured.  While this is usually set in the subclass's
                             constructor, it is not requred to match a step defined in this moudule
        :param list errors:  a list of specific error messages indicating the multiple errors 
                             that occurred.
        """
        self.step = step or ""
        self.aipid = aipid
        if not msg:
            msg = f"Problem during preservation"
            if self.step:
                msg += f" {self.step}"
            if self.aipid:
                msg += f" for AIP={self.aipid}"
            msg = self._append_error_preview(msg, errors)
                
        super(PreservationTaskException, self).__init__(msg, errors, cause)

    def _append_error_preview(self, msg: str, errors: List[str]):
        if errors and isinstance(errors, (list, tuple)):
            msg += ": {errors[0]}"
            if len(errors) > 1:
                msg += " (and other errors)"
        else:
            msg += ": cause unknown"
        return msg


class AIPFinalizationException(PreservationTaskException):
    """
    an exception that occurs while attempting to apply the finalization step in a processing task
    """
    def __init__(self, msg: str=None, aipid: str=None, errors: List[str]=None, step: str=None):
        """
        create the exception, optionally list things that went wrong for the AIP
        :param str     msg:  a general message describing the failure
        :param str   aipid:  the ID of the AIP being processed
        :param list errors:  a list of specific error messages indicating the multiple errors 
                             that occurred.
        """
        if not step:
            step = "finalization"
        if not msg:
            msg = f"Failure while finalizing AIP"
            if aipid:
                msg += f"={aipid}"
            msg = self._append_error_preview(msg, errors)
        super(AIPFinalizationException, self).__init__(msg, aipid, step, errors)


class AIPValidationException(PreservationTaskException):
    """
    an exception that occurs while attempting to apply the finalization step in a processing task
    """
    def __init__(self, msg: str=None, aipid: str=None, errors: List[str]=None, step: str=None):
        """
        create the exception, optionally list things that went wrong for the AIP
        :param str     msg:  a general message describing the failure
        :param str   aipid:  the ID of the AIP being processed
        :param list errors:  a list of specific error messages indicating the multiple errors 
                             that occurred.
        """
        if not step:
            step = "validation"
        if not msg:
            msg = "Failure while validating AIP"
            if aipid:
                msg += f"={aipid}"
            msg = self._append_error_preview(msg, errors)
        super(AIPValidationException, self).__init__(msg, aipid, step, errors)


class AIPSerialzationException(PreservationTaskException):
    """
    an exception that occurs while attempting to apply the finalization step in a processing task
    """
    def __init__(self, msg: str=None, aipid: str=None, errors: List[str]=None, step: str=None):
        """
        create the exception, optionally list things that went wrong for the AIP
        :param str     msg:  a general message describing the failure
        :param str   aipid:  the ID of the AIP being processed
        :param list errors:  a list of specific error messages indicating the multiple errors 
                             that occurred.
        """
        if not step:
            step = "serialization"
        if not msg:
            msg = f"Failure while serializing AIP"
            if aipid:
                msg += f"={aipid}"
            msg = self._append_error_preview(msg, errors)
        super(AIPSerialzationException, self).__init__(msg, aipid, step, errors)


class AIPArchivingException(PreservationTaskException):
    """
    an exception that occurs while attempting to apply the finalization step in a processing task
    """
    def __init__(self, msg: str=None, aipid: str=None, errors: List[str]=None, step: str=None):
        """
        create the exception, optionally list things that went wrong for the AIP
        :param str     msg:  a general message describing the failure
        :param str   aipid:  the ID of the AIP being processed
        :param list errors:  a list of specific error messages indicating the multiple errors 
                             that occurred.
        """
        if not step:
            step = "archiving"
        if not msg:
            msg = f"Failure while archiving AIP"
            if aipid:
                msg += f"={aipid}"
            msg = self._append_error_preview(msg, errors)
        super(AIPArchivingException, self).__init__(msg, aipid, step, errors)


class AIPPublicationException(PreservationTaskException):
    """
    an exception that occurs while attempting to apply the publication step in a processing task
    """
    def __init__(self, msg: str=None, aipid: str=None, errors: List[str]=None, step: str=None):
        """
        create the exception, optionally list things that went wrong for the AIP
        :param str     msg:  a general message describing the failure
        :param str   aipid:  the ID of the AIP being processed
        :param list errors:  a list of specific error messages indicating the multiple errors 
                             that occurred.
        """
        if not step:
            step = "publication"
        if not msg:
            msg = f"Failure while publishing AIP"
            if aipid:
                msg += f"={aipid}"
            msg = self._append_error_preview(msg, errors)
        super(AIPPublicationException, self).__init__(msg, aipid, step, errors)

class IngestError(AIPPublicationException):
    """
    an exception that occurs while attempting to apply the publication step in a processing task
    """
    def __init__(self, aipid: str, msg: str=None, errors: List[str]=None):
        """
        create the exception, optionally list things that went wrong for the AIP
        :param str     msg:  a general message describing the failure
        :param str   aipid:  the ID of the AIP being processed
        :param list errors:  a list of specific error messages indicating the multiple errors 
                             that occurred.
        """
        if not msg:
            msg = "Failure ingesting AIP into repository"
            if aipid:
                msg += f": {aipid}"
            msg = self._append_error_preview(msg, errors)
        super(IngestError, self).__init__(msg, aipid, step, errors)

class DOISubmissionError(AIPPublicationException):
    """
    an exception that occurs while attempting to apply the publication step in a processing task
    """
    def __init__(self, aipid: str, msg: str=None, errors: List[str]=None, step: str=None):
        """
        create the exception, optionally list things that went wrong for the AIP
        :param str     msg:  a general message describing the failure
        :param str   aipid:  the ID of the AIP being processed
        :param list errors:  a list of specific error messages indicating the multiple errors 
                             that occurred.
        """
        if not msg:
            msg = "Failure submitting DOI metadata"
            if aipid:
                msg += f"for AIP={aipid}"
            msg = self._append_error_preview(msg, errors)
        super(DOISubmissionError, self).__init__(msg, aipid, step, errors)

class PreservationTaskFailure(PreservationTaskException):
    """
    an exception that indicates that :py:class:`PreservationTask` failed to complete.

    This exception is intended to be triggered in response to some other more specific 
    exception caused the task processing to stop.  It is meant to carry all the necessary 
    information for facilitating post-failure handling (like notifying an administrator).  
    """

    def __init__(self, aipid: str, message: str, errors: List[str], origin: str=None,
                 notify_type: str=None, cause=None, **kw):
        """
        initialize the exception.

        :param str       aipid:  the identifier for the AIP being operated on
        :param str     message:  a summary of the failure that stopped task processing
        :param list     errors:  a list of more specific error messages that occurred as 
                                 part of the failure
        :param str        step:  the name of the step implementation where the failure 
        :param str notify_type:  a label indicating the type of notification alert that 
                                 should result from this failure.  If None, a general-
                                 purpose type will be engaged.  
        :param Exception cause:  the original exception that cause the failure.  Note 
                                 that using the raise...from pattern will set this 
                                 automatically.
        """
        super(PreservationTaskFailure, self).__init__(message, aipid, step, errors, cause)
        self.notify_as = notify_type
        self.props = kw
        


