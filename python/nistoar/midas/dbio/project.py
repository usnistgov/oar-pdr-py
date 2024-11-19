"""
a module providing a service for creating and manipulating MIDAS _projects_ stored in a DBIO
backend.  

A _project_ represents a draft description of a digital asset stored in the MIDAS database; it 
is represented by a _project record_ that is compliant with the MIDAS Common Database project
data model.  Different project types include DMP and Digital Asset Publication (DAP).  This 
module provides a base service class for manipulating such records.  It is intended to be 
subclassed to handle the creation of the different types of projects and conventions, policies,
and interaction models for manipulating them.
"""
import re
from logging import Logger, getLogger
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping, Sequence
from typing import List
from copy import deepcopy

import jsonpatch

from .base import (DBClient, DBClientFactory, ProjectRecord, ACLs, RecordStatus, ANONYMOUS,
                   AlreadyExists, NotAuthorized, ObjectNotFound, DBIORecordException,
                   InvalidUpdate, InvalidRecord)
from . import status
from .. import MIDASException, MIDASSystem
from nistoar.pdr.utils.prov import Agent, Action
from nistoar.id.versions import OARVersion
from nistoar.pdr import ARK_NAAN
from nistoar.base.config import ConfigurationException

_STATUS_ACTION_CREATE   = RecordStatus.CREATE_ACTION
_STATUS_ACTION_UPDATE   = RecordStatus.UPDATE_ACTION
_STATUS_ACTION_CLEAR    = "clear"
_STATUS_ACTION_FINALIZE = "finalize"
_STATUS_ACTION_SUBMIT   = "submit"

DEF_PUBLISHED_SUFFIX = "_published"

class ProjectService(MIDASSystem):
    """
    A base class for a service to create, access, or update a project.  This generic 
    base can be used as is or extended and overridden to specialize the business logic for updating 
    a particular type of project under particular conventions or policies.  The service is attached 
    to a particular user at construction time (as given by a :py:class:`~nistoar.pdr.utils.Agent`
    instance); thus, requests to this service are subject to internal Authorization checks.

    This base service supports three parameters, ``dbio``, ``default_perms``, and ``clients``.  The 
    optional ``dbio`` parameter will be passed to the :py:class:`~nistoar.midas.dbio.base.DBClientFactory`'s 
    ``create_client()`` function to create the :py:class:`~nistoar.midas.dbio.base.DBClient`. 

    The optional ``default_perms`` is an object that sets the ACLs for newly created project records.  
    Its optional properties name the permisson types that defaults are to be set for, including "read", 
    "write", "admin", and "delete" but can also include other (non-standard) category names.  Each 
    property is a list of user identifiers that the should be given the particular type of permission.
    Typically, only virtual group identifiers (like "grp0:public") make sense.

    The ``clients`` parameter is an object that places restrictions on the 
    creation of records based on which group the user is part of.  The keys of the object
    are user group names that are authorized to use this service, and whose values are themselves objects
    that restrict the requests by that user group; for example:

    .. code-block::

       "clients": {
           "midas": {
               "default_shoulder": "mdm1"
           },
           "default": {
               "default_shoulder": "mdm0"
           }
       }

    The special group name "default" will (if present) be applied to users whose group does not match
    any of the other names.  If not present the user will not be allowed to create new records.  

    This implementation only supports one parameter as part of the group configuration: ``default_shoulder``.
    This parameter gives the identifier shoulder that should be used the identifier for a new record 
    created under the user group.  Subclasses of this service class may support other parameters. 
    """
    STATUS_ACTION_CREATE   = _STATUS_ACTION_CREATE  
    STATUS_ACTION_UPDATE   = _STATUS_ACTION_UPDATE  
    STATUS_ACTION_CLEAR    = _STATUS_ACTION_CLEAR   
    STATUS_ACTION_FINALIZE = _STATUS_ACTION_FINALIZE
    STATUS_ACTION_SUBMIT   = _STATUS_ACTION_SUBMIT  

    def __init__(self, project_type: str, dbclient_factory: DBClientFactory, config: Mapping={},
                 who: Agent=None, log: Logger=None, _subsys=None, _subsysabbrev=None):
        """
        create the service
        :param str  project_type:  the project data type desired.  This name is usually used as the 
                                   name of the collection in the backend database.  Recognized values
                                   include ``dbio.DAP_PROJECTS`` and ``dbio.DMP_PROJECTS``
        :param DBClient dbclient:  the DBIO client instance to use to access and save project records
        :param dict       config:  the handler configuration tuned for the current type of project
        :param who         Agent:  the representation of the user that is requesting access
        :param Logger        log:  the logger to use for log messages
        """
        if not _subsys:
            _subsys = "DBIO Project Service"
        if not _subsysabbrev:
            _subsysabbrev = "DBIO"
        super(ProjectService, self).__init__(_subsys, _subsysabbrev)

        # set configuration, check values
        self.cfg = config
        for param in "clients default_perms".split():
            if not isinstance(self.cfg.get(param,{}), Mapping):
                raise ConfigurationException("%s: value is not a object as required: %s" %
                                             (param, type(self.cfg.get(param))))
        for param,val in self.cfg.get('clients',{}).items():
            if not isinstance(val, Mapping):
                raise ConfigurationException("clients.%s: value is not a object as required: %s" %
                                             (param, repr(val)))
        for param,val in self.cfg.get('default_perms',{}).items():
            if not isinstance(val, list) or not all(isinstance(p, str) for p in val):
                raise ConfigurationException(
                    "default_perms.%s: value is not a list of strings as required: %s" %
                    (param, repr(val))
                )
        
        if not who:
            who = Agent("dbio.project", Agent.USER, Agent.ANONYMOUS, Agent.PUBLIC)
        self.who = who
        if not log:
            log = getLogger(self.system_abbrev).getChild(self.subsystem_abbrev).getChild(project_type)
        self.log = log

        user = who.actor if who else None
        self.dbcli = dbclient_factory.create_client(project_type, self.cfg.get("dbio", {}), user)
        if not self.dbcli.people_service:
            self.log.warning("No people service available for %s service", project_type)

    @property
    def user(self) -> Agent:
        """
        the Agent instance representing the user that this service acts on behalf of.
        """
        return self.who

    def create_record(self, name, data=None, meta=None) -> ProjectRecord:
        """
        create a new project record with the given name.  An ID will be assigned to the new record.
        :param str  name:  the mnuemonic name to assign to the record.  This name cannot match that
                           of any other record owned by the user. 
        :param dict data:  the initial data content to assign to the new record.  
        :param dict meta:  the initial metadata to assign to the new record.  
        :raises NotAuthorized:  if the authenticated user is not authorized to create a record
        :raises AlreadyExists:  if a record owned by the user already exists with the given name
        """
        shoulder = self._get_id_shoulder(self.who)
        if self.dbcli.user_id == ANONYMOUS:
            self.log.warning("A new record requested for an anonymous user")
        prec = self.dbcli.create_record(name, shoulder)
        self._set_default_perms(prec.acls)

        if meta:
            meta = self._moderate_metadata(meta, shoulder)
            if prec.meta:
                self._merge_into(meta, prec.meta)
            else:
                prec.meta = meta
        elif not prec.meta:
            prec.meta = self._new_metadata_for(shoulder)
        prec.data = self._new_data_for(prec.id, prec.meta)
        prec.status.act(self.STATUS_ACTION_CREATE, "draft created")
        if data:
            self.update_data(prec.id, data, message=None, _prec=prec)  # this will call prec.save()
        else:
            prec.save()

        self._record_action(Action(Action.CREATE, prec.id, self.who, prec.status.message))
        self.log.info("Created %s record %s (%s) for %s", self.dbcli.project, prec.id, prec.name, self.who)
        return prec

    def _set_default_perms(self, acls: ACLs):
        defs = self.cfg.get("default_perms", {})
        for perm in defs:
            acls.grant_perm_to(perm, *defs[perm])

    def delete_record(self, id) -> ProjectRecord:
        """
        delete the draft record.  This may leave a stub record in place if, for example, the record 
        has been published previously.  
        """
        # TODO:  handling previously published records
        raise NotImplementedError()

    def _get_id_shoulder(self, user: Agent):
        """
        return an ID shoulder that is appropriate for the given user agent
        :param Agent user:  the user agent that is creating a record, requiring a shoulder
        :raises NotAuthorized: if an uathorized shoulder appropriate for the user cannot be determined.
        """
        out = None
        client_ctl = self.cfg.get('clients', {}).get(user.agent_class)
        if client_ctl is None:
            client_ctl = self.cfg.get('clients', {}).get("default")
        if client_ctl is None:
            self.log.debug("Unrecognized client group, %s", user.agent_class)
            raise NotAuthorized(user.actor, "create record",
                                "Client group, %s, not recognized" % user.agent_class)

        out = client_ctl.get('default_shoulder')
        if not out:
            self.log.info("No default ID shoulder configured for client group, %s", user.agent_class)
            raise NotAuthorized(user.actor, "create record",
                                "No default shoulder defined for client group, "+user.agent_class)
        return out

    def get_record(self, id) -> ProjectRecord:
        """
        fetch the project record having the given identifier
        :raises ObjectNotFound:  if a record with that ID does not exist
        :raises NotAuthorized:   if the record exists but the current user is not authorized to read it.
        """
        return self.dbcli.get_record_for(id)

    def get_status(self, id) -> RecordStatus:
        """
        For the record with the given identifier, return the status object that indicates the current 
        state of the record and the last action applied to it.  
        :raises ObjectNotFound:  if a record with that ID does not exist
        :raises NotAuthorized:   if the record exists but the current user is not authorized to read it.
        """
        return self.get_record(id).status

    def get_data(self, id, part=None):
        """
        return a data content from the record with the given ID
        :param str   id:  the record's identifier
        :param str path:  a path to the portion of the data to get.  This is the same as the ``datapath``
                          given to the handler constructor.  This will be an empty string if the full
                          data object is requested.
        :raises ObjectNotFound:  if no record with the given ID exists or the ``part`` parameter points to 
                          a non-existent part of the data content.  
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                          given by ``id``.  
        :raises PartNotAccessible:  if access to the part of the data specified by ``part`` is not allowed.
        """
        prec = self.dbcli.get_record_for(id)  # may raise ObjectNotFound
        if not part:
            return prec.data
        return self._extract_data_part(prec.data, part)

    def _extract_data_part(self, data, part):
        if not part:
            return data
        steps = part.split('/')
        out = data
        while steps:
            prop = steps.pop(0)
            if prop not in out:
                raise ObjectNotFound(id, part)
            out = out[prop]

        return out

    def _record_action(self, act: Action):
        # this is tolerant of recording errors
        try:
            self.dbcli.record_action(act)
        except Exception as ex:
            self.log.error("Failed to record provenance action for %s (%s: %s): %s",
                           act.subject, act.type, act.message, str(ex))

    def _try_save(self, prec):
        # this is tolerant of recording errors
        try:
            prec.save()
        except Exception as ex:
            self.log.error("Failed to save project record, %s: %s", prec.id, str(ex))

    def update_data(self, id, newdata, part=None, message="", _prec=None):
        """
        merge the given data into the currently save data content for the record with the given identifier.
        :param str      id:  the identifier for the record whose data should be updated.
        :param str|dict|list newdata:  the data to save as the new content.  
        :param str    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             the given ``newdata`` is a value that should be set to the property pointed 
                             to by ``part``.  
        :param str message:  an optional message that will be recorded as an explanation of the update.
        :raises ObjectNotFound:  if no record with the given ID exists or the ``part`` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by ``id``.  
        :raises PartNotAccessible:  if replacement of the part of the data specified by `part` is not allowed.
        :raises InvalidUpdate:  if the provided ``newdata`` represents an illegal or forbidden update or 
                             would otherwise result in invalid data content.
        """
        set_action = False
        if not _prec:
            set_action = True  # setting the last action will NOT be the caller's responsibility
            _prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        olddata = None

        if _prec.status.state not in [status.EDIT, status.READY]:
            raise NotEditable(id)

        if not part:
            # updating data as a whole: merge given data into previously saved data
            olddata = deepcopy(_prec.data)
            self._merge_into(newdata, _prec.data)

        else:
            # updating just a part of the data
            steps = part.split('/')
            data = _prec.data
            while steps:
                prop = steps.pop(0)
                if prop not in data or data[prop] is None:
                    if not steps:
                        data[prop] = newdata
                    else:
                        data[prop] = {}
                elif not steps:
                    olddata = data[prop]
                    if isinstance(data[prop], Mapping) and isinstance(newdata, Mapping):
                        self._merge_into(newdata, data[prop])
                    else:
                        data[prop] = newdata
                elif not isinstance(data[prop], Mapping):
                    raise PartNotAccessible(id, part,
                                            "%s: data property, %s, is not in an updatable state")
                data = data[prop]

        data = _prec.data
        if message is None:
            message = "draft updated"
        
        # prep the provenance record
        obj = self._jsondiff(olddata, newdata)  # used in provenance record below
        tgt = _prec.id
        if part:
            # if patching a specific part, record it as a subaction
            provact = Action(Action.PATCH, tgt, self.who, message)
            tgt += "#data.%s" % part
            provact.add_subaction(Action(Action.PATCH, tgt, self.who, "updating data."+part, obj))
        else:
            provact = Action(Action.PATCH, tgt, self.who, _prec.status.message, obj)

        # ensure the replacing data is sufficiently complete and valid and then save it
        # If it is invalid, InvalidUpdate is raised.
        try:
            data = self._save_data(data, _prec, message, set_action and _STATUS_ACTION_UPDATE)

        except InvalidUpdate as ex:
            provact.message = "Failed to save update due to invalid data: " + ex.format_errors()
            raise
            
        except Exception as ex:
            self.log.error("Failed to save update for project, %s: %s", _prec.id, str(ex))
            provact.message = "Failed to save update due to an internal error"
            raise

        finally:
            self._record_action(provact)

        self.log.info("Updated data for %s record %s (%s) for %s",
                      self.dbcli.project, _prec.id, _prec.name, self.who)
        return self._extract_data_part(data, part)

    def _jsondiff(self, old, new):
        return {"jsonpatch": jsonpatch.make_patch(old, new)}

    def _merge_into(self, update: Mapping, base: Mapping, depth: int=-1):
        if depth == 0:
            return

        for prop in update:
            if prop in base and isinstance(base[prop], Mapping):
                if (depth < 0 or depth > 1) and isinstance(update[prop], Mapping):
                    # the properties from the base and update must both be dictionaries; otherwise,
                    # update is ignored.
                    self._merge_into(update[prop], base[prop], depth-1)
            else:
                base[prop] = update[prop]

        return base

    def _new_data_for(self, recid, meta=None):
        """
        return an "empty" data object set for a record with the given identifier.  The returned 
        dictionary can contain some minimal or default properties (which may or may not include
        the identifier or information based on the identifier).  
        """
        return OrderedDict()

    def _new_metadata_for(self, shoulder=None):
        """
        return an "empty" metadata object set for a record with the given identifier.  The returned 
        dictionary can contain some minimal or default properties (which may or may not include
        the identifier or information based on the identifier).  

        Recall that a project record's "metadata" stores information that helps manage the evolution of
        the record, and does not normally contain information set directly from data provided by the 
        user client.  An exception is when a record is created: the client can provide some initial 
        metadata that gets filtered by :py:method:`_moderate_metadata`
        """
        return OrderedDict()

    def _moderate_metadata(self, mdata: MutableMapping, shoulder=None):
        """
        massage and validate the given record metadata provided by the user client, returning a 
        valid version of the metadata.  The implementation may modify the given dictionary in place. 
        The default implementation does accepts none of the client-provided properties
        
        The purpose of this function is to filter out data properties that are not supported or 
        otherwise should not be settable by the client.  
        :raises ValueError:   if the mdata is disallowed in a way that should abort the entire request.
        """
        out = self._new_metadata_for(shoulder)
        out.update(mdata)
        out['agent_vehicle'] = self.who.vehicle
        return out

    def replace_data(self, id, newdata, part=None, message="", _prec=None):
        """
        Replace the currently stored data content of a record with the given data.  It is expected that 
        the new data will be filtered/cleansed via an internal call to :py:method:`dress_data`.  
        :param str      id:  the identifier for the record whose data should be updated.
        :param str newdata:  the data to save as the new content.  
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             the given `newdata` is a value that should be set to the property pointed 
                             to by `part`.  
        :param ProjectRecord prec:  the previously fetched and possibly updated record corresponding to `id`.
                             If this is not provided, the record will by fetched anew based on the `id`.  
        :raises ObjectNotFound:  if no record with the given ID exists or the `part` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by `id`.  
        :raises PartNotAccessible:  if replacement of the part of the data specified by `part` is not allowed.
        :raises InvalidUpdate:  if the provided `newdata` represents an illegal or forbidden update or 
                             would otherwise result in invalid data content.
        """
        set_action = False
        if not _prec:
            set_action = True  # setting the last action will NOT be the caller's responsibility
            _prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        olddata = deepcopy(_prec.data)

        if _prec.status.state not in [status.EDIT, status.READY]:
            raise NotEditable(id)

        if not part:
            # this is a complete replacement; merge it with a starter record
            data = self._new_data_for(id)
            self._merge_into(newdata, data)

        else:
            # replacing just a part of the data
            data = _prec.data
            steps = part.split('/')
            while steps:
                prop = steps.pop(0)
                if prop not in data or data[prop] is None:
                    if not steps:
                        data[prop] = newdata
                    else:
                        data[prop] = {}
                elif not steps:
                    data[prop] = newdata
                elif not isinstance(data[prop], Mapping):
                    raise PartNotAccessible(id, part)
                data = data[prop]

            data = _prec.data

        if message is None:
            message = "draft updated"

        # prep the provenance record
        obj = self._jsondiff(olddata, newdata)
        tgt = _prec.id
        if part:
            # if patching a specific part, record it as a subaction
            provact = Action(Action.PATCH, tgt, self.who, _prec.status.message)
            tgt += "#data.%s" % part
            provact.add_subaction(Action(Action.PUT, tgt, self.who, "replacing data."+part, obj))
        else:
            provact = Action(Action.PUT, tgt, self.who, _prec.status.message, obj)

        # ensure the replacing data is sufficiently complete and valid.
        # If it is invalid, InvalidUpdate is raised.
        try:
            data = self._save_data(data, _prec, message, set_action and _STATUS_ACTION_UPDATE)

        except PartNotAccessible as ex:
            # client request error; don't record action
            raise

        except Exception as ex:
            self.log.error("Failed to save update to project, %s: %s", _prec.id, str(ex))
            provact.message = "Failed to save update due to an internal error"
            self._record_action(provact)
            raise

        else:
            self._record_action(provact)

        self.log.info("Replaced data for %s record %s (%s) for %s",
                      self.dbcli.project, _prec.id, _prec.name, self.who)
        return self._extract_data_part(data, part)

    def _save_data(self, indata: Mapping, prec: ProjectRecord,
                   message: str, action: str = _STATUS_ACTION_UPDATE) -> Mapping:
        """
        expand, validate, and save the data modified by the user as the record's data content.
        
        The given data represents a merging of input from the user with the latest saved data.  
        This function provides the final transformations and validation checks before being saved.
        It may have two side effects: first, as part of the final transformations, the indata 
        mapping may get updated in place.  Second, the function may update the record's metadata 
        (stored in its `meta` property).

        :param dict indata:  the user-provided input merged into the previously saved data.  After 
                             final transformations and validation, this will be saved the the 
                             record's `data` property.
        :param ProjectRecord prec:  the project record object to save the data to.  
        :param str message:  a message to save as the status action message; if None, no message 
                             is saved.
        :param str  action:  the action label to record; if None, the action is not updated.
        :return:  the (transformed) data that was actually saved
                  :rtype: dict
        :raises InvalidUpdate:  if the provided `indata` represents an illegal or forbidden update or 
                             would otherwise result in invalid data content
        """
        # this implementation does not transform the data
        self._validate_data(indata)  # may raise InvalidUpdate

        prec.data = indata

        # update the record status according to the inputs
        if action:
            prec.status.act(action, message)
        elif message is not None:
            prec.message = message
        prec.status.set_state(status.EDIT)

        prec.save();
        return indata

    def _validate_data(self, data):
        pass

    def clear_data(self, id: str, part: str=None, message: str=None, prec=None) -> bool:
        """
        remove the stored data content of the record and reset it to its defaults.  Note that
        no change is recorded if the requested data does not exist yet.
        :param str      id:  the identifier for the record whose data should be cleared.
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             only that property will be cleared (either removed or set to an initial
                             default).
        :return:  True the data was properly cleared; return False if ``part`` was specified but does not
                  yet exist in the data.
        :param ProjectRecord prec:  the previously fetched and possibly updated record corresponding to `id`.
                             If this is not provided, the record will by fetched anew based on the `id`.  
        :raises ObjectNotFound:  if no record with the given ID exists or the `part` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by `id`.  
        :raises PartNotAccessible:  if clearing of the part of the data specified by `part` is not allowed.
        """
        set_state = False
        if not prec:
            set_state = True
            prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized

        if prec.status.state not in [status.EDIT, status.READY]:
            raise NotEditable(id)

        initdata = self._new_data_for(prec.id, prec.meta)
        if not part:
            # clearing everything: return record to its initial defaults
            prec.data = initdata
            if message is None:
                message = "reset draft to initial defaults"
            prec.status.act(self.STATUS_ACTION_CLEAR, message)

        else:
            # clearing only part of the data
            steps = part.split('/')
            data = prec.data
            while steps:
                prop = steps.pop(0)
                if prop in initdata:
                    if not steps:
                        data[prop] = initdata[prop]
                    elif prop not in data:
                        data[prop] = {}
                elif prop not in data:
                    return False
                elif not steps:
                    del data[prop]
                    break
                data = data[prop]
                initdata = initdata.get(prop, {})

            if message is None:
                message = "reset %s to initial defaults" % part
            prec.status.act(self.STATUS_ACTION_UPDATE, message)

        # prep the provenance record
        tgt = prec.id
        if part:
            # if deleting a specific part, record it as a subaction
            provact = Action(Action.PATCH, tgt, self.who, prec.status.message)
            tgt += "#data.%s" % part
            provact.add_subaction(Action(Action.DELETE, tgt, self.who, "clearing data."+part))
        else:
            provact = Action(Action.DELETE, tgt, self.who, prec.status.message)

        if set_state:
            prec.status.set_state(status.EDIT)

        try:
            prec.save()

        except Exception as ex:
            self.log.error("Failed to save cleared data for project, %s: %s", tgt, str(ex))
            provact.message = "Failed to clear requested data due to internal error"
            raise

        finally:
            self._record_action(provact)
        self.log.info("Cleared out data for %s record %s (%s) for %s",
                      self.dbcli.project, prec.id, prec.name, self.who)
        return True


    def update_status_message(self, id: str, message: str, _prec=None) -> status.RecordStatus:
        """
        set the message to be associated with the current status regarding the last action
        taken on the record with the given identifier
        :returns:  a Project status instance providing status after updating the message
                   :rtype: RecordStatus
        :param str      id:  the identifier of the record to attach the message to
        :param str message:  the message to attach
        """
        if not _prec:
            _prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        stat = _prec.status

        if stat.state != status.EDIT:
            raise NotEditable(id)
        stat.message = message
        _prec.save()
        self._record_action(Action(Action.COMMENT, _prec.id, self.who, message))

        return stat.clone()
        

    def finalize(self, id, message=None, as_version=None, _prec=None) -> status.RecordStatus:
        """
        Assume that no more client updates will be applied and apply any final automated updates 
        to the record in preparation for final publication.  After the changes are applied, the 
        resulting record will be validated.  Normally, the record's state will not be changed as 
        a result.  The record must be in the edit state to be applied.  
        :param str      id:  the identifier of the record to finalize
        :param str message:  a message summarizing the updates to the record
        :returns:  a Project status instance providing the post-finalization status
                   :rtype: RecordStatus
        :raises ObjectNotFound:  if no record with the given ID exists or the `part` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by `id`.  
        :raises NotEditable:  the requested record in not in the edit state 
        :raises InvalidUpdate:  if the finalization produces an invalid record
        """
        reset_state = False
        if not _prec:
            reset_state = True  # if successful, resetting state will NOT be caller's responsibility
            _prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized

        stat = _prec.status
        if _prec.status.state not in [status.EDIT, status.READY]:
            raise NotEditable(id)

        stat.set_state(status.PROCESSING)
        stat.act(self.STATUS_ACTION_FINALIZE, "in progress")
        _prec.save()
        
        try:
            defmsg = self._apply_final_updates(_prec)

        except InvalidRecord as ex:
            emsg = "finalize process failed: "+str(ex)
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "finalize", "errors": ex.errors}))
            stat.set_state(status.EDIT)
            stat.act(self.STATUS_ACTION_FINALIZE, ex.format_errors())
            self._try_save(_prec)
            raise

        except Exception as ex:
            self.log.error("Failed to finalize project record, %s: %s", _prec.id, str(ex))
            emsg = "Failed to finalize due to an internal error"
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "finalize", "errors": [emsg]}))
            stat.set_state(status.EDIT)
            stat.act(self.STATUS_ACTION_FINALIZE, emsg)
            self._try_save(_prec)
            raise

        else:
            # record provenance record
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, defmsg, {"name": "finalize"}))

            if reset_state:
                stat.set_state(status.READY)
            stat.act(self.STATUS_ACTION_FINALIZE, message or defmsg)
            _prec.save()

        self.log.info("Finalized %s record %s (%s) for %s",
                      self.dbcli.project, _prec.id, _prec.name, self.who)
        return stat.clone()

    MAJOR_VERSION_LEV = 0
    MINOR_VERSION_LEV = 1
    TRIVIAL_VERSION_LEV = 2

    def _apply_final_updates(self, prec: ProjectRecord, vers_inc_lev: int=None):
        # update the data content
        self._finalize_data(prec)

        # ensure a finalized version
        ver = self._finalize_version(prec, vers_inc_lev)

        # ensure a finalized data identifier
        id = self._finalize_id(prec)

        self._validate_data(prec)
        return "draft is ready for submission as %s, %s" % (id, ver)

    def _finalize_version(self, prec: ProjectRecord, vers_inc_lev: int=None):
        """
        determine what the version string for the to-be-submitted document should be
        and save it into the record.

        This implementation will increment the "minor" (i.e. second) field in the version
        string by default and save as part of the data field as a "@version" property.  To 
        be incremented, the current version must include a suffix the starts with "+"; after
        being incremented, the suffix is dropped (to indicate that it should not be further 
        incremented).  If a version is not yet assigned, it will be set as 1.0.0.  

        :param ProjectRecord prec:  the record to finalize
        :param int   vers_inc_lev:  the field position in the version to increment if the 
                                    version needs incrmenting.  
        :returns:  the version that the record will be published as
        """
        # determine output version
        if vers_inc_lev is None:
            # assess the state of the revision to determine proper level
            vers_inc_lev = self.MINOR_VERSION_LEV

        vers = OARVersion(prec.data.setdefault('@version', "1.0.0"))
        if vers.is_draft():
            vers.drop_suffix().increment_field(vers_inc_lev)
            prec.date['@version'] = str(vers)

        return prec.data['@version']

    def _finalize_id(self, prec):
        """
        finalize the identifier that will be attached to to-be-submitted document.  Generally,
        an identifier is assigned once the first time it is finalized and is normally not changed
        subsequently; however, an implementations may alter this ID according to policy.
        """
        # determine output identifier
        if not prec.data.get('@id'):
            prec.data['@id'] = self._arkify_recid(prec.id)

        return prec.data['@id']

    def _arkify_recid(self, recid):
        """
        turn a standard project record identifier into an institutional ARK identifier.
        If the the given identifier is not recognized as a project record identifier 
        (based on its form), it is returned unchanged.  
        """
        naan = self.cfg.get('ark_naan', ARK_NAAN)
        m = re.match('^(\w+):([\w\-\/]+)$', recid)
        if m:
            return "ark:/%s/%s-%s" % (naan, m.group(1), m.group(2))
        return recid

    def _finalize_data(self, prec):
        """
        update the data content for the record in preparation for submission.
        """
        pass

    def submit(self, id: str, message: str=None, _prec=None) -> status.RecordStatus:
        """
        finalize (via :py:meth:`finalize`) the record and submit it for publishing.  After a successful 
        submission, it may not be possible to edit or revise the record until the submission process 
        has been completed.  The record must be in the "edit" state prior to calling this method.
        :param str      id:  the identifier of the record to submit
        :param str message:  a message summarizing the updates to the record
        :returns:  a Project status instance providing the post-submission status
                   :rtype: RecordStatus
        :raises ObjectNotFound:  if no record with the given ID exists or the `part` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by `id`.  
        :raises NotEditable:  the requested record is not in the edit state.  
        :raises NotSubmitable:  if the finalization produces an invalid record because the record 
                             contains invalid data or is missing required data.
        :raises SubmissionFailed:  if, during actual submission (i.e. after finalization), an error 
                             occurred preventing successful submission.  This error is typically 
                             not due to anything the client did, but rather reflects a system problem
                             (e.g. from a downstream service). 
        """
        if not _prec:
            _prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        stat = _prec.status
        self.finalize(id, message, _prec=_prec)  # may raise NotEditable

        # this record is ready for submission.  Send the record to its post-editing destination,
        # and update its status accordingly.
        try:
            defmsg = self._submit(_prec)

        except InvalidRecord as ex:
            emsg = "submit process failed: "+str(ex)
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "submit", "errors": ex.errors}))
            stat.set_state(status.EDIT)
            stat.act(self.STATUS_ACTION_SUBMIT, ex.format_errors())
            self._try_save(_prec)
            raise

        except Exception as ex:
            emsg = "Submit process failed due to an internal error"
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "submit", "errors": [emsg]}))
            stat.set_state(status.EDIT)
            stat.act(self.STATUS_ACTION_SUBMIT, emsg)
            self._try_save(_prec)
            raise

        else:
            # record provenance record
            self.dbcli.record_action(Action(Action.PROCESS, _prec.id, self.who, defmsg, {"name": "submit"}))

            stat.set_state(status.SUBMITTED)
            stat.act(self.STATUS_ACTION_SUBMIT, message or defmsg)
            _prec.save()

        self.log.info("Submitted %s record %s (%s) for %s",
                      self.dbcli.project, _prec.id, _prec.name, self.who)
        return stat.clone()
            

    def _submit(self, prec: ProjectRecord) -> str:
        """
        Actually send the given record to its post-editing destination and update its status 
        accordingly.

        This method should be overridden to provide project-specific handling.  This generic 
        implementation will simply copy the data contents of the record to another collection.
        :returns:  the label indicating its post-editing state
                   :rtype: str
        :raises NotSubmitable:  if the finalization process produced an invalid record because the record 
                             contains invalid data or is missing required data.
        :raises SubmissionFailed:  if, during actual submission (i.e. after finalization), an error 
                             occurred preventing successful submission.  This error is typically 
                             not due to anything the client did, but rather reflects a system problem
                             (e.g. from a downstream service). 
        """
        pass
        

                    

class ProjectServiceFactory:
    """
    a factory object that creates ProjectService instances attached to the backend DB implementation
    and which act on behalf of a specific user.  

    As this is a concrete class, it can be instantiated directly to produce generic ProjectService 
    instances but serving a particular project type.  Instances are also attached ot a particular
    DB backend by virtue of the DBClientFactory instance that is passed in at factory construction 
    time.  

    The configuration provided to this factory will be passed directly to the service instances 
    it creates.  See the :py:class:`ProjectService` documentation for the configuration 
    parameters supported by this implementation.
    """
    def __init__(self, project_type: str, dbclient_factory: DBClientFactory, config: Mapping={},
                 log: Logger=None):
        """
        create a service factory associated with a particulr DB backend.
        :param str project_type:  the project data type desired.  This name is usually used as the 
                                  name of the collection in the backend database.  Recognized values
                                  include ``dbio.DAP_PROJECTS`` and ``dbio.DMP_PROJECTS``
        :param DBClientFactory dbclient_factory:  the factory instance to use to create a DBClient to 
                                 talk to the DB backend.
        :param Mapping  config:  the configuration for the service (see class-level documentation).  
        :param Logger      log:  the Logger to use in the service.  
        """
        self._dbclifact = dbclient_factory
        self._prjtype = project_type
        self._cfg = config
        self._log = log

    @property
    def project_type(self):
        """
        the name for the type of DBIO project this is a service factory for.  Values can include
        "DAP" and "DMP".
        """
        return self._prjtype

    def create_service_for(self, who: Agent=None):
        """
        create a service that acts on behalf of a specific user.  
        :param Agent who:    the user that wants access to a project
        """
        return ProjectService(self._prjtype, self._dbclifact, self._cfg, who, self._log)


class PartNotAccessible(DBIORecordException):
    """
    an exception indicating that the user-provided data is invalid or otherwise would result in 
    invalid data content for a record. 
    """
    def __init__(self, recid, part, message=None, sys=None):
        """
        initialize the exception
        :param str recid:  the id of the record that was existed
        :param str  part:  the part of the record that was requested.  Do not provide this parameter if 
                           the entire record does not exist.  
        """
        if not message:
            message = "%s: data property, %s, is not in an updateable state" % (recid, part)
        super(PartNotAccessible, self).__init__(recid, message, sys=sys)
        self.record_part = part
    
class NotEditable(DBIORecordException):
    """
    An error indicating that a requested record cannot be updated because it is in an uneditable state.
    """
    def __init__(self, recid, message=None, sys=None):
        """
        initialize the exception
        """
        if not message:
            message = "%s: not in an editable state" % recid
        super(NotEditable, self).__init__(recid, message, sys=sys)
    
class NotSubmitable(InvalidRecord):
    """
    An error indicating that a requested record cannot be finalized and submitted for publication, 
    typically because it is contains invalid data or is missing required data.  
    """
    def __init__(self, recid: str, message: str=None, errors: List[str]=None, sys=None):
        """
        initialize the exception
        """
        if not message:
            message = "%s: not in an submitable state" % recid
        super(NotSubmitable, self).__init__(message, recid, errors, sys=sys)


