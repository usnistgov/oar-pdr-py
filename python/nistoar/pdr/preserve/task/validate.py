"""
A common implementation of the AIPValidation preservation step interface that tests for NIST-compliant
preservation bags.
"""
import os, time, datetime, logging, json
from pathlib import Path
from collections import OrderedDict

from . import framework as fw
from nistoar.pdr.preserve.bagit import NISTBag
from nistoar.pdr.preserve.bagit.validate import NISTBagValidator
from nistoar.pdr.preserve.bagit.validate.base import ValidationIssue, ValidationResults
from nistoar.pdr.preserve import AIPValidationError
from nistoar.pdr.preserve.datachecker import DataChecker
from nistoar.pdr.utils import LockedFile
from nistoar.pdr.exceptions import StateException

class NISTBagValidation(fw.AIPValidation):
    """
    An implementation of the AIP validation step that ensures that the AIP meets the requirements
    for a NIST preservation bag and is ready to serialized for long-term storage.  If it does not,
    the :py:meth:`apply` method raises an AIPValidationError exception.

    This impplementation applies the NIST bag validation suite provided in the 
    :py:mod:`nistoar.pdr.preserve.bagit.validate` module.  The strictness of the validation--that is,
    what level of test failure (error, warning, or recommendation) raises an exception--is controlled
    by the ``raise_on`` configuration parameter.  

    If the provided :py:class:`~nistoar.pdr.preserve.task.framework.PreservationStateManager` provides
    a working directory, this implementation will write a file called ``validation_results.json`` that 
    encodes the detailed results and timing, which can be useful for assessing failures.  

    This implementation supports the following configuration parameters:
    :trivial:  if True, only very minimal checks are done on the bag to allow for serialization.
               Set this to True if it is expected to be fully validated outside of the step (e.g. 
               beforehand).  The default is False; use with caution.
    :always_apply:  if True (default), this step will not communicate to the coordinating preservation
               task that validation was completed; this will cause validation to be re-applied before 
               serialization in the event that the task needs to be rerun.  
    :raise_on: a flag--one of "error", "warn", "rec"--that indicates which level of failed tests
               should result in raising a validation exception.  (default: "rec")
    :record_passed: if True, a listing of all applied tests that passed will be written to the 
               ``validation_results.json`` file.  Recording passed tests will also get triggered if the 
               state manager's log is enabled for debugging messages, 
    :check_data_files:  if True (default), make sure all the files described in NERDm metadata are 
               accessible:  they must exist either in this bag or in a previously preserved bag.  If 
               is set to True, ``data_checker`` must also be set to configure access to the repository
               and long-term storage. 
    :data_checker:  Used when ``check_data_files`` is True, this is a configuration dictionary that 
               configure the file checker.  See :py:class:`~nistoar.pdr.preserve.datachecker.DataChecker`
               for more info.
    :bagit:    a configuration dictionary that specifically configures validation against the base
               BagIt standard.  See :py:mod:`nistoar.pdr.preserve.bagit.validate.bagit` for more 
               info.  Note: to turn off checksum checks (which can take a long time for large 
               collections), set ``check_checksums`` to False within this dictionary. 
    :multibag: a configuration dictionary that specifically configures validation against the 
               Multibag BagIt profile.  See :py:mod:`nistoar.pdr.preserve.bagit.validate.multibag` 
               for more info.  
    :nist:     a configuration dictionary that specifically configures validation against the 
               NIST Preservation BagIt profile.  See :py:mod:`nistoar.pdr.preserve.bagit.validate.nist` 
               for more info.  
    """

    def apply(self, statemgr: fw.PreservationStateManager, as_is: bool=False):
        """
        apply the validation to the target AIP
        :param PreservationStateManager statemgr:  the state manager coordinating the preservation task
        :raise AIPValidationException:  if a failure occurs while trying to apply the validation process;
                                        this includes if it cannot get access to the finalized bag.
        :raise AIPValidationError:      if the finalized back fails to meet all requirements for a 
                                        preservation bag.
        """
        log = statemgr.log.getChild("validate")
        start = time.time()
        startd = datetime.datetime.fromtimestamp(start).isoformat(' ')
        log.debug("validating preservation bag, starting %s", startd)

        info = OrderedDict([
            ("step", "validation"),
            ("aipid", statemgr.aipid),
            ("start_date", startd)
        ])

        ERR = "error"
        WARN= "warn"
        REC = "rec"
        raiseon_words = [ ERR, WARN, REC ]
        
        raiseon = self.cfg.get('raise_on', WARN)
        if raiseon and raiseon not in raiseon_words:
            raise ConfigurationException(f"raise_on property not one of "+str(raiseon) + ": " + raiseon)

        bagdir = statemgr.get_finalized_aip()
        if bagdir is None and as_is:
            bagdir = statemgr.get_sip()
        if bagdir is None:
            raise fw.AIPValidationException("Finalized bag is not set (rerun finalized?)", statemgr.aipid)

        try: 
            bag = NISTBag(bagdir)
        except StateException as ex:
            raise fw.AIPValidationException(f"Finalized bag cannot be opened: {str(ex)}", statemgr.aipid) \
                from ex

        # TODO?  warn about no check_data_files?

        res = ValidationResults(bag.name)
        res.want = res.ALL
        if raiseon:
            res.want = ((raiseon == ERR)  and res.ERROR)  or \
                       ((raiseon == WARN) and res.PROB) or res.ALL

        if self.cfg.get('trivial'):
            # Just make sure we have something to serialize
            issue = ValidationIssue("PDR", "0", "1", res.WARN,
                                    "AIP should look like a BagIt bag (and multibag-ready)")
            res._warn(issue, os.path.is_file(os.path.join(bag.dir, "bag-info.txt")),
                      "Doesn't look like a real bag (missing bag-info.txt)")
            info['trivial'] = True
            info['bag_validate_duration'] = time.time() - start

        else:
            # Run our bag validators
            vld8r = NISTBagValidator(self.cfg)
            res = vld8r.validate(bag, results=res)

            # process the results
            issues = res.failed(res.want)
            if len(issues):
                log.warning("Bag Validation issues detected for AIP id="+statemgr.aipid)
                for iss in issues:
                    if iss.type == iss.ERROR:
                        log.error(iss.description)
                    elif iss.type == iss.WARN:
                        log.warning(iss.description)
                    else:
                        log.info(iss.description)

            mark = time.time()
            info['bag_validate_duration'] = mark - start

            dofilecheck = self.cfg.get('check_data_files', True)
            if dofilecheck and res.failed(res.ERROR):
                log.warn("Severe bag errors found; skipping data file checks")
                dofilecheck = False

            elif dofilecheck:
                # run the data checker
                chkr = DataChecker(bag, self.cfg.get('data_checker', {}),
                                   statemgr.log.getChild('data_checker'))

                missing = chkr.unindexed_files()
                issue = ValidationIssue("PDR", "0", "2.1", res.ERROR,
                                        "All data files listed in the NERDm metadata must appear in "
                                        "the multibag file index")
                if len(missing) > 0:
                    log.error("master bag for id=%s is missing the following "+
                              "files from the multibag file index:\n  %s",
                              self.name, "\n  ".join(missing))
                    issue.add_comment(f'{len(missing)} file{(len(missing)>0 and "s are") or " is"} missing:')
                res._err( issue, len(missing) == 0, missing)
                
                missing = chkr.unavailable_files(viadistrib=viadistrib)
                issue = ValidationIssue("PDR", "0", "2.2", res.ERROR,
                                        "All data files listed in the the multibag file index must be "
                                        "found in this or an available bag.")
                if len(missing) > 0:
                    log.error("unable to locate the following files described " +
                              "in master bag for id=%s:\n  %s",
                              self.name, "\n  ".join(missing))
                    issue.add_comment(f'{len(missing)} file{(len(missing)>0 and "s are") or " is"} missing:')
                res._err( issue, len(missing) == 0, missing)

                info['data_check_duration'] = time.time() - mark

        mark = time.time()
        info['duration'] = mark - start
        info['finish_date'] = datetime.datetime.fromtimestamp(mark).isoformat(' ')
        self._save_results(info, res, statemgr)

        if raiseon and res.count_failed(res.want) > 0:
            raise AIPValidationError("AIP Bag Validation errors detected",
                                     errors=[i.description for i in res.failed(res.PROB)])

        log.info(f"{statemgr.aipid}: bag validation completed without issue")
        statemgr.mark_completed(statemgr.VALIDATED, "Bag validation completed")

    def _save_results(self, info, res, statemgr):
        outdir = statemgr.get_working_dir()
        log = statemgr.log.getChild('validate')
        if not outdir:
            log.debug("Won't record validation results; no working directory available")
            return
        
        outfile = Path(outdir) / "validation_results.json"

        info['is_valid'] = res.ok()
        info['failed'] = []
        for issue in res.failed(res.ALL):
            info['failed'].append(issue.to_json_obj())
        if self.cfg.get("record_passed") or log.isEnabledFor(logging.DEBUG):
            info['passed'] = []
            for issue in res.passed(res.ALL):
                info['passed'].append(issue.to_json_obj())

        try:
            with LockedFile(outfile, 'w') as fd:
                json.dump(info, fd, indent=2)
        except Exception as ex:
            log.warning("Failed to record validation results: %s", str(ex))


        

                    
