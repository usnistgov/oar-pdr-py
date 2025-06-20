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
from typing import List, Union
from copy import deepcopy

import jsonpatch

from .base import (DBClient, DBClientFactory, ProjectRecord, ACLs, PUBLIC_GROUP, ANONYMOUS, AUTOADMIN,
                   RecordStatus, AlreadyExists, NotAuthorized, ObjectNotFound, DBIORecordException,
                   InvalidUpdate, InvalidRecord)
from . import status
from .. import MIDASException, MIDASSystem
from nistoar.pdr.utils.prov import Agent, Action
from nistoar.pdr.utils.validate import ValidationResults, ALL, REQ, WARN
from nistoar.id.versions import OARVersion
from nistoar.pdr import ARK_NAAN
from nistoar.base.config import ConfigurationException

_STATUS_ACTION_CREATE   = RecordStatus.CREATE_ACTION
_STATUS_ACTION_UPDATE   = RecordStatus.UPDATE_ACTION
_STATUS_ACTION_CLEAR    = "clear"
_STATUS_ACTION_REVIEW   = "review"
_STATUS_ACTION_FINALIZE = "finalize"
_STATUS_ACTION_SUBMIT   = "submit"
_STATUS_ACTION_UPDATEPREP = "update-prep"
_STATUS_ACTION_RESTORE  = "restore"
_STATUS_ACTION_PUBLISH  = "publish"

DEF_PUBLISHED_SUFFIX = "_published"

class ProjectService(MIDASSystem):
    """
    A base class for a service to create, access, or update a project.  This generic 
    base can be used as is or extended and overridden to specialize the business logic for updating 
    a particular type of project under particular conventions or policies.  The service is attached 
    to a particular user at construction time (as given by a :py:class:`~nistoar.pdr.utils.Agent`
    instance); thus, requests to this service are subject to internal Authorization checks.

    This base service supports two parameters: ``dbio`` and ``default_perms``.  The ``dbio``
    parameter will be passed to the :py:class:`~nistoar.midas.dbio.base.DBClientFactory`'s 
    ``create_client()`` function to create the :py:class:`~nistoar.midas.dbio.base.DBClient`.  In 
    principle, the ``dbio`` parameter is optional; however, this is usually where required 
    :ref:`restrictions on ID minting<ref-id-minting>` are included.

    The optional ``default_perms`` is an object that sets the ACLs for newly created project records.  
    Its optional properties name the permisson types that defaults are to be set for, including "read", 
    "write", "admin", and "delete" but can also include other (non-standard) category names.  Each 
    property is a list of user identifiers that the should be given the particular type of permission.
    Typically, only virtual group identifiers (like "grp0:public") make sense.

    This implementation only supports one parameter as part of the group configuration: ``default_shoulder``.
    This parameter gives the identifier shoulder that should be used the identifier for a new record 
    created under the user group.  Subclasses of this service class may support other parameters. 
    """
    STATUS_ACTION_CREATE   = _STATUS_ACTION_CREATE
    STATUS_ACTION_UPDATE   = _STATUS_ACTION_UPDATE
    STATUS_ACTION_CLEAR    = _STATUS_ACTION_CLEAR
    STATUS_ACTION_FINALIZE = _STATUS_ACTION_FINALIZE
    STATUS_ACTION_SUBMIT   = _STATUS_ACTION_SUBMIT
    STATUS_ACTION_PUBLISH  = _STATUS_ACTION_PUBLISH
    STATUS_ACTION_UPDATEPREP = _STATUS_ACTION_UPDATEPREP
    STATUS_ACTION_RESTORE  = _STATUS_ACTION_RESTORE  

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
        for param in "default_perms".split():
            if not isinstance(self.cfg.get(param,{}), Mapping):
                raise ConfigurationException("%s: value is not a object as required: %s" %
                                             (param, type(self.cfg.get(param))))
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

        self.dbcli = dbclient_factory.create_client(project_type, self.cfg.get("dbio", {}), self.who)
        if not self.dbcli.people_service:
            self.log.warning("No people service available for %s service", project_type)

    @property
    def user(self) -> Agent:
        """
        the Agent instance representing the user that this service acts on behalf of.
        """
        return self.who

    def exists(self, id) -> bool:
        """
        return True if there exists a project record (of the type this service is configured for) 
        with the given identifier.  

        This implementation simply delegates the question to the internal DBClient instance.  
        """
        return self.dbcli.exists(id)

    def create_record(self, name, data=None, meta=None, dbid: str=None) -> ProjectRecord:
        """
        create a new project record with the given name.  An ID will be assigned to the new record.
        :param str  name:  the mnuemonic name to assign to the record.  This name cannot match that
                           of any other record owned by the user. 
        :param dict data:  the initial data content to assign to the new record.  
        :param dict meta:  the initial metadata to assign to the new record.  
        :param str  dbid:  a requested identifier or ID shoulder to assign to the record; if the 
                           value does not include a colon (``:``), it will be interpreted as the
                           desired shoulder that will be attached an internally minted local 
                           identifier; otherwise, the value will be taken as a full identifier. 
                           If not provided (default), an identifier will be minted using the 
                           default shoulder.
        :raises NotAuthorized:  if the authenticated user is not authorized to create a record, or 
                                when ``dbid`` is provided, the user is not authorized to create a 
                                record with the specified shoulder or ID.
        :raises AlreadyExists:  if a record owned by the user already exists with the given name or
                                the given ``dbid``.
        """
        localid = None
        shoulder = None
        if dbid:
            if ':' not in dbid:
                # interpret dbid to be a requested shoulder
                shoulder = dbid
            else:
                shoulder, localid = dbid.split(':', 1)
        else:
            shoulder = self._get_id_shoulder(self.who, meta)  # may return None (DBClient will set it)

        foruser = None
        if meta and meta.get("foruser"):
            # format of value: either "newuserid" or "olduserid:newuserid"
            foruser = meta.get("foruser", "").split(":")
            if not foruser or len(foruser) > 2:
                foruser = None
            else:
                foruser = foruser[-1]
                
        if self.dbcli.user_id == ANONYMOUS:
            # Do we need to be more careful in production by cancelling reassign request?
            # foruser = None
            self.log.warning("A new record requested for an anonymous user")

        prec = self.dbcli.create_record(name, shoulder, localid=localid)
        self._set_default_perms(prec.acls)
        shoulder = prec.id.split(':', 1)[0]

        prec.status._data["created_by"] = self.who.id  # don't do this: violates encapsulation
        if foruser:
            if self.dbcli.user_id == ANONYMOUS:
                self.log.warning("%s wants to reassign new record to %s", self.dbcli.user_id, foruser)
            try:
                prec.reassign(foruser)  
            except NotAuthorized as ex:
                self.log.warning("%s: %s not authorized to reassign owner to %s",
                                 prec.id, self.dbcli.user_id, foruser)

        if meta:
            meta = self._moderate_metadata(meta, shoulder)
            if prec.meta:
                self._merge_into(meta, prec.meta)
            else:
                prec.meta = meta
        elif not prec.meta:
            prec.meta = self._new_metadata_for(shoulder)
        prec.data = self._new_data_for(prec.id, prec.meta)
        prec.status.act(self.STATUS_ACTION_CREATE, "draft created", self.who.actor)
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
        prec = self.dbcli.get_record_for(id, ACLs.DELETE)   # may raise ObjectNotFound/NotAuthorized

        out = None
        provact = Action(Action.DELETE, prec.id, self.who, "deleted draft record")
        try: 
            if prec.status.published_as:
                # restore record to the last published version
                self._restore_last_published_data(prec,
                                          "Deleted draft revision (restored previously published version)",
                                                  provact)
                out = prec
            else:
                # can completely forget this record
                self.dbcli.delete_record(prec.id)

        except Exception as ex:
            self.log.error("Failed to delete draft for rec, %s: %s", id, str(ex))
            provact.message = "Failed to delete draft due to internal error"
            raise
        finally:
            self._record_action(provact)
        return out

    def _restore_last_published_data(self, prec: ProjectRecord, message: str=None,
                                     foract: Action = None, reset_state: bool=True):
        """
        use the information in the status of the given record to restore the data content to that
        of the last published version.  The status data must include a non-empty ``published_as``
        property.

        This implementation assumes the default publishing strategy and will pull the data from 
        publish section of the backend store.  Subclasses may override this to handle other strategies.
        Note that this implementation does not support honoring "archived_at".  
        """
        pubid = prec.status.published_as
        if not pubid:
            raise ValueError("_restore_last_published_data(): project record is missing "
                             "published_as property")

        if prec.status.archived_at:
            self.log.warning("%s: archived_at property is set but will be ignored; assuming default")
        archived_at = f"dbio_store:{self.dbcli.project}_latest/{self._arkify_recid(prec.id)}"

        # setup prov action
        defmsg = "Restored draft to last published version"
        provact = Action(Action.PROCESS, prec.id, self.who,
                         f"restored data to last published ({archived_at})",
                         {"name": "restore_last_published"})
        if foract:
            foract.add_subaction(provact)

        try:
            # Create restorer from archived_at URL
            # TODO: this will be replaced with the use of a factory that processes the archived_at URL
            pubclient = self.dbcli.client_for(f"{self.dbcli.project}_latest")
            # restorer = DBIOStoreRestorer(dbclient, self._arkify_recid(prec.id))
            # prec.data = restorer.get_data()

            # Restore data and set into project record
            pubrec = pubclient.get_record_for(self._arkify_recid(prec.id), ACLs.READ)
            prec.data = pubrec.data

            if reset_state:
                prec.status.set_state(pubrec.status.state)
            prec.status.act(self.STATUS_ACTION_RESTORE, message or defmsg)
            prec.save()
            
        except Exception as ex:
            self.log.error("Failed to save prepped-for-revision record for project, %s: %s",
                           prec.id, str(ex))
            provact.message = "Failed to save prepped-for-revision data due to internal error"
            raise
        finally:
            if not foract:
                self._record_action(provact)

    def reassign_record(self, id, recipient: str, disown: bool=False):
        """
        reassign ownership of the record with the given recepient ID.  This is a wrapper around 
        :py:class:`~nistoar.midas.dbio.base.ProjectRecord`.reassign() that also logs the change.
        :param            id:  the record identifier to reassign
        :param str recipient:  the identifier of the user to reassign ownership to
        :raises InvalidUpdate:  if the recipient ID is not legal or unrecognized
        :raises NotAuthorized:  if the current user does is not authorized to reassign.  Non-superusers 
                                must have "admin" permission to reassign.
        :raises ObjectNotFound: if the record ``id`` is not found
        :returns:  the identifier for the new owner that was set for the record
                   :rtype: str
        """
        prec = self.dbcli.get_record_for(id)  # may raise ObjectNotFound

        message = "from %s to %s" % (prec.owner, recipient)
        try:
            self.log.info("Reassigning ownership of %s %s", id, message)
            prec.reassign(recipient, disown)
            prec.save()
            self._record_action(Action(Action.COMMENT, prec.id, self.who,
                                       f"Reassigned ownership {message}"))
            return prec.owner

        except Exception as ex:
            self.log.error("Failed to reassign record %s to %s: %s", id, recipient, str(ex))
            raise

    def rename_record(self, id, newname: str):
        """
        change the short, mnemonic name assigned to the record with the given recepient ID.  This is 
        a wrapper around :py:class:`~nistoar.midas.dbio.base.ProjectRecord`.rename() that also logs 
        the change.
        :param          id:  the record identifier to rename
        :param str newname:  the new name to give to the record
        :raises AlreadyExists:  if the name has already been assigned to another record owned by the 
                                current user.
        :raises NotAuthorized:  if the current user does is not authorized to reassign.  Non-superusers 
                                must have "admin" permission to reassign.
        :raises ObjectNotFound: if the record ``id`` is not found
        :returns:  the identifier for the new owner that was set for the record
                   :rtype: str
        """
        prec = self.dbcli.get_record_for(id)  # may raise ObjectNotFound

        message = "from %s to %s" % (prec.name, newname)
        try:
            self.log.info("Renaming %s %s", id, message)
            prec.rename(newname)
            prec.save()
            self._record_action(Action(Action.COMMENT, prec.id, self.who,
                                       f"Renaming {message}"))
            return prec.name

        except Exception as ex:
            self.log.error("Failed to rename record %s to %s: %s", id, newname, str(ex))
            raise

    def _get_id_shoulder(self, user: Agent, meta: Mapping):
        """
        return an ID shoulder that is appropriate for the given user agent and meta constraints.

        If None is returned, the shoulder should be determined by the DBClient.  This implementation
        always returns None.
        :param Agent user:  the user agent that is creating a record, requiring a shoulder
        :param dict  meta:  the meta datas provided when creating record
        """
        return None

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

    def review(self, id, want=ALL, _prec=None) -> ValidationResults:
        """
        Review the record with the given identifier for completeness and correctness, and return lists of 
        suggestions for completing the record.  If None is returned, review is not supported for this type
        of project.  
        :param str   id:  the identifier for the project record to review
        :param int want:  a flag (default: ALL) indicating the types of tests to apply and return
        :raises ObjectNotFound:  if a record with that ID does not exist
        :raises NotAuthorized:   if the record exists but the current user is not authorized to read it.
        :return: the review results
                 :rtype: ValidationResults
        """
        prec = self.get_record(id)   # may raise exceptions
        return None

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

        if _prec.status.state == status.PUBLISHED:
            self.log.info("%s: Preparing published record for revision", id)
            self._prep_for_update(_prec)   # this should change state to EDIT
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

    def _prep_for_update(self, prec: ProjectRecord, message: str=None, reset_state: bool=True):
        """
        prepare a record that is currently in the PUBLISHED state to be updated and reset the state
        to EDIT.  This may involve resetting the content of the data property to that consistent with 
        the last published version.  

        This implementation assumes that the data property already contains data matching the last 
        published version.  Subclasses may override this and can safely call this super method to 
        record the provenance action and reset the state.

        Note that this implementation ignores the presence of the 
        :py:class:`~nistoar.midas.dbio.status.Status` property, ``archived_at``.  

        :param ProjectRecord prec:  the project record to prepare for an update
        :param str        message:  A message to record as the status message; if not provided, a 
                                    default will be set.
        :param bool   reset_state:  If True (default), the state will be reset to EDIT; if False,
                                    it will not be changed.  
        """
        defmsg = "Previous publication is ready for revision"
        provact = Action(Action.PROCESS, prec.id, self.who, "trivial prep for update",
                         {"name": "prep_for_update"})
        if reset_state:
            prec.status.set_state(status.EDIT)
        prec.status.act(self.STATUS_ACTION_UPDATEPREP, message or defmsg)

        try:
            prec.save()
        except Exception as ex:
            self.log.error("Failed to save prepped record for project, %s: %s", prec.id, str(ex))
            provact.message = "Failed to save prepped record due to internal error"
            raise
        finally:
            self._record_action(provact)

    def replace_data(self, id, newdata, part=None, message="", _prec=None):
        """
        Replace the currently stored data content of a record with the given data.  It is expected that 
        the new data will be filtered/cleansed via an internal call to :py:method:`dress_data`.  
        :param str      id:  the identifier for the record whose data should be updated.
        :param str newdata:  the data to save as the new content.  
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             the given `newdata` is a value that should be set to the property pointed 
                             to by `part`.  
        :param str message:  an optional message that will be recorded as an explanation of the replacement.
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

        if _prec.status.state == status.PUBLISHED:
            self.log.info("%s: Preparing published record for revision", id)
            self._prep_for_update(_prec)   # this should change state to EDIT
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

    def _save_data(self, indata: Mapping, prec: ProjectRecord, message: str, 
                   action: str = _STATUS_ACTION_UPDATE, update_state: bool = True) -> Mapping:
        """
        expand, validate, and save the data modified by the user as the record's data content.
        
        The given data represents a merging of input from the user with the latest saved data.  
        This function provides the final transformations and validation checks before being saved.
        It may have two side effects: first, as part of the final transformations, the indata 
        mapping may get updated in place.  Second, the function may update the record's metadata 
        (stored in its `meta` property).

        :param dict indata:  the user-provided input merged into the previously saved data.  After 
                             final transformations and validation, this will be saved to the 
                             record's `data` property.
        :param ProjectRecord prec:  the project record object to save the data to.  
        :param str message:  a message to save as the status action message; if None, no message 
                             is saved.
        :param str  action:  the action label to record; if None, the action is not updated.
        :param bool update_status:  if True (default), that record status will be reset to EDIT;
                             if False, the status will be unchanged.  False should be used when 
                             the record is in a state where it cannot be directly editable by 
                             the end user (e.g. it is being processed for submission)
        :return:  the (transformed) data that was actually saved
                  :rtype: dict
        :raises InvalidUpdate:  if the provided `indata` represents an illegal or forbidden update or 
                             would otherwise result in invalid data content
        """
        # this implementation does not transform the data
        res = self._minimally_validate_data(indata, prec.id)
        if res and res.count_failed() > 0:
            raise InvalidUpdate("data property is not minimally compliant", prec.id,
                                errors=[t.specification for t in res.failed()])

        prec.data = indata

        # update the record status according to the inputs
        if action:
            prec.status.act(action, message, self.who.actor)
        elif message is not None:
            prec.status.message = message
        if update_state:
            prec.status.set_state(status.EDIT)

        prec.save();
        return indata

    def _minimally_validate_data(self, data, id, **kw) -> ValidationResults:
        # default does nothing; caller may assume either no problems found or no tests were applied
        return None

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
            prec.status.act(self.STATUS_ACTION_CLEAR, message, self.who.actor)

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
            prec.status.act(self.STATUS_ACTION_UPDATE, message, self.who.actor)

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
        :raises ObjectNotFound:  if no record with the given ID exists
        :raises NotAuthorized:   if the authenticated user does not have permission to update the 
                                 record given by `id`.  
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
        stat.act(self.STATUS_ACTION_FINALIZE, "in progress", self.who.actor)
        _prec.save()
        
        try:
            defmsg = self._apply_final_updates(_prec)

        except InvalidRecord as ex:
            emsg = "finalize process failed: "+str(ex)
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "finalize", "errors": ex.errors}))
            stat.set_state(status.EDIT)
            stat.act(self.STATUS_ACTION_FINALIZE, ex.format_errors(), self.who.actor)
            self._try_save(_prec)
            raise

        except Exception as ex:
            self.log.error("Failed to finalize project record, %s: %s", _prec.id, str(ex))
            emsg = "Failed to finalize due to an internal error"
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "finalize", "errors": [emsg]}))
            stat.set_state(status.EDIT)
            stat.act(self.STATUS_ACTION_FINALIZE, emsg, self.who.actor)
            self._try_save(_prec)
            raise

        else:
            # record provenance record
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, defmsg, {"name": "finalize"}))

            if reset_state:
                stat.set_state(status.READY)
            stat.act(self.STATUS_ACTION_FINALIZE, message or defmsg, self.who.actor)
            _prec.save()

        self.log.info("Finalized %s record %s (%s) for %s",
                      self.dbcli.project, _prec.id, _prec.name, self.who)
        return stat.clone()

    MAJOR_VERSION_LEV = 0
    MINOR_VERSION_LEV = 1
    TRIVIAL_VERSION_LEV = 2

    def _apply_final_updates(self, prec: ProjectRecord, vers_inc_lev: int=None):
        # update the data content
        vil = self._finalize_data(prec)
        if vers_inc_lev is None:
            vers_inc_lev = vil

        # ensure a finalized version
        ver = self._finalize_version(prec, vers_inc_lev)

        # ensure a finalized data identifier
        id = self._finalize_id(prec)

        note = ""
        res = self._finally_validate(prec)
        if not res:
            self.log.warning(f"{prec.id}: No final validations applied!")
        elif res.count_failed(REQ) > 0:
            raise InvalidUpdate("Final validation checks failed", prec.id,
                                errors=[t.specification for t in res.failed()])
        elif res.count_failed(WARN) > 0:
            note = " (some warnings detected)"

        return "draft is ready for submission as %s, %s%s" % (id, ver, note)

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
        m = re.match(r'^(\w+):([\w\-\/]+)$', recid)
        if m:
            return "ark:/%s/%s-%s" % (naan, m.group(1), m.group(2))
        return recid

    def _finalize_data(self, prec) -> Union[int,None]:
        """
        update the data content for the record in preparation for submission.
        @return  the version increment level that should be applied to determine the 
                 published version based on the status of the data.  1 means incrment 
                 the patch field only, 2 means a minor incrment, and 3 means a major 
                 increment.  0 means the version should not be incrmented, and None 
                 means take default.  
                 @rtype int
        """
        return None

    def _finally_validate(self, prec: ProjectRecord) -> ValidationResults:
        # Note: we'll need to expand the checks applied; just do a review for now
        return self.review(prec.id, REQ&WARN, prec)

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
            _prec = self.dbcli.get_record_for(id, ACLs.ADMIN)   # may raise ObjectNotFound/NotAuthorized
        stat = _prec.status
        if stat.state not in [status.EDIT, status.READY]:
            raise NotSubmitable(_prec.id, "Project not in submitable state: "+ stat.state)
        self.finalize(id, message, _prec=_prec)  # may raise NotEditable

        # this record is ready for submission.  Send the record to its post-editing destination,
        # and update its status accordingly.
        try:
            poststat = self._submit(_prec)

        except InvalidRecord as ex:
            emsg = "submit process failed: "+str(ex)
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "submit", "errors": ex.errors}))
            stat.set_state(status.EDIT)
            stat.act(self.STATUS_ACTION_SUBMIT, ex.format_errors(), self.who.actor)
            self._try_save(_prec)
            raise SubmissionFailed("Invalid record could not be submitted: %s", str(ex))

        except Exception as ex:
            emsg = "Submit process failed due to an internal error"
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "submit", "errors": [emsg]}))
            stat.set_state(status.EDIT)
            stat.act(self.STATUS_ACTION_SUBMIT, emsg, self.who.actor)
            self._try_save(_prec)
            raise SubmissionFailed("Submission action failed: %s", str(ex)) from ex

        else:
            if not message:
                if _prec.data.get('@version', '1.0.0') == '1.0.0':
                    message = "Initial version " + poststat
                else:
                    message = "Revision " + poststat

            # record provenance record
            self.dbcli.record_action(Action(Action.PROCESS, _prec.id, self.who, message,
                                            {"name": "submit"}))

            stat.set_state(poststat)
            stat.act(self.STATUS_ACTION_SUBMIT, message or defmsg, self.who.actor)
            _prec.save()

        self.log.info("Submitted %s record %s (%s) for %s",
                      self.dbcli.project, _prec.id, _prec.name, self.who)
        if poststat != status.SUBMITTED:
            self.log.info("Final status: %s", poststat)
        return stat.clone()

    def _submit(self, prec: ProjectRecord) -> str:
        """
        Actually send the given record to its post-editing destination and update its status 
        accordingly.

        This method should be overridden to provide project-specific handling.  This generic 
        implementation will immediately publish the record (via :py:meth:`_publish`).  Its 
        default implementation will simply copy the data contents of the record to another 
        collection.

        :returns:  the label indicating its post-editing state
                   :rtype: str
        :raises NotSubmitable:  if the finalization process produced an invalid record because the record 
                             contains invalid data or is missing required data.
        :raises SubmissionFailed:  if, during actual submission (i.e. after finalization), an error 
                             occurred preventing successful submission.  This error is typically 
                             not due to anything the client did, but rather reflects a system problem
                             (e.g. from a downstream service). 
        """
        return self._publish(prec)  # returned state will be PUBLISHED if successful

    def apply_external_review(self, id: str, revsys: str, phase: str, revid: str=None, 
                              infourl: str=None, feedback: List[Mapping]=None, 
                              request_changes: bool=False, fbreplace: bool=True, **extra_info):
        """
        register information about external review activity and possibly apply specific updates
        accordingly.  

        Generally, regular users are not authorized to call this function.

        :param str  revsys:  a unique name for the external review system providing this information.
        :param str   phase:  a label indicating the phase of review that the project is currently in. 
                             The values are defined by the external review system.
        :param str      id:  an identifier used by the external review system to track the review.  If 
                             None, then there is none defined and can probably default to the current 
                             project identifier
        :param str infourl:  a URL that DBIO client user can access to get information on the status of 
                             the external review.  If None, such information is not (yet) available
        :param list feedback:  a list of reviewer feedback.  If None, the previously saved feedback will 
                             be retained.  If an empty list and ``fbreplace`` is True (default), the 
                             previously save feedback will be dropped and replaced with an empty list.
        :param boo request_changes:  if True, return this record to a state that allows the authors to 
                             make further edits.  (This would include changing the record state to 
                             "edit".)  This record must currently be in the "submitted" state, otherwise,
                             this parameter will be ignored.
        :param bool fbreplace:  if True (default), this feedback should replace all previously registered 
                             feedback
        :param extra_info:   Other JSON-encodable properties that should be included in the registration.
        """
        _prec = self.dbcli.get_record_for(id, ACLs.PUBLISH)   # may raise ObjectNotFound/NotAuthorized
        stat = _prec.status

        revmd = stat.pubreview(revsys, phase, revid, infourl, feedback, fbreplace, **extra_info)
        if self._apply_external_review_updates(_prec, revmd, request_changes):
            _prec.save()

        msg = "external review phase in progress"
        if revmd.get('phase'):
            msg += ": "+revmd['phase']
        if revmd.get('feedback'):
            msg += "; feedback provided"
        self.dbcli.record_action(Action(Action.COMMENT, _prec.id, self.who, msg))
        self.log.info("%s: %s", _prec.id, msg)

        return _prec.status.state

    def _apply_external_review_updates(self, prec: ProjectRecord, pubrevmd: Mapping=None,
                                       request_changes: bool=False) -> bool:
        # apply any automated changes to the record according the given feedback
        # This exists as a specialization point.  return True if the record was saved 
        return False

    def approve(self, id: str, revsys: str, revid: str=None, infourl: str=None, publish: bool=True):
        """
        mark this project as approved for publication by an external review system.  This will
        call :py:meth:`apply_external_review` with phase "approved".  If ``publish`` is ``True``
        and the record is in a publishable state, the publishing process will be triggered 
        automatically.  

        Generally, regular users are not authorized to call this function.
        """
        self.apply_external_review(id, revsys, "approved", revid, infourl, [])
        if publish:
            self.publish()

    def cancel_external_review(self, id: str, revsys: str = None, revid: str=None, infourl: str=None):
        """
        cancel the review process from a particular review system or for all systems.  
        """
        prec = self.dbcli.get_record_for(id, ACLs.PUBLISH)   # may raise ObjectNotFound/NotAuthorized
        if not revsys:
            revsys = list(prec.status.to_dict().get(status._pubreview_p, {}).keys())
        elif isinstance(revsys, str):
            revsys = [ revsys ]

        for sys in revsys:
            self.apply_external_review(id, sys, "canceled", revid, infourl, feedback=[])

        return prec
                
            

    def publish(self, id: str, _prec=None, **kwargs):
        """
        initiate the publishing processing for the given record (including preservation).  Generally,
        regular users are not authorized to call this function directly.  The record must be in a 
        SUBMITTED state.
        :param str      id:  the identifier of the record to submit
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
            _prec = self.dbcli.get_record_for(id, ACLs.PUBLISH)   # may raise ObjectNotFound/NotAuthorized
        stat = _prec.status
        if stat.state == status.PUBLISHED:
            raise NotSubmitable(_prec.id, "Already published")
        if stat.state == status.INPRESS:
            raise NotSubmitable(_prec.id, "Publication already in progress")
        if stat.state == status.EDIT:
            raise NotSubmitable(_prec.id, "Project has not been submitted for publication yet")
        if stat.state not in [status.SUBMITTED, status.ACCEPTED]:
            raise NotSubmitable(_prec.id, "Project has not in a publishable state: "+stat.state)

        if stat.state != status.ACCEPTED:
            reviews = stat._data.get(status._pubreview_p)
            if reviews and not all(r.get('phase','') == "approved" for r in reviews.values()):
                raise NotSubmitable(_prec.id, "Not all external reviews are completed")

        self.log.info("Submitting rec, %s, for publication", _prec.id)
        try:
            poststat = self._publish(_prec)
            if poststat not in [status.PUBLISHED, status.INPRESS]:
                raise RuntimeException("Publishing submission returned unexpected state: "+poststat)

        except InvalidRecord as ex:
            emsg = "publishing process failed: "+str(ex)
            self.log.error(f"{emsg}")
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "publish", "errors": ex.errors}))
            stat.set_state(status.UNWELL)
            stat.act(self.STATUS_ACTION_PUBLISH, ex.format_errors(), self.who.actor)
            self._try_save(_prec)
            raise

        except Exception as ex:
            emsg = "Publishing process failed due to an internal error"
            self.log.exception(ex)
            self._record_action(Action(Action.PROCESS, _prec.id, self.who, emsg,
                                       {"name": "publish", "errors": [emsg]}))
            stat.set_state(status.UNWELL)
            emsg += f": {str(ex)}"
            stat.act(self.STATUS_ACTION_PUBLISH, emsg, self.who.actor)
            self._try_save(_prec)
            raise

        else:
            message = "Revised"
            if _prec.data.get('@version', '1.0.0') == '1.0.0':
                message = "Initial"
            message +=  " publication"
            if poststat == status.PUBLISHED:
                message += " successful"
            else:
                message += " in progress"

            # record provenance record
            self.dbcli.record_action(Action(Action.PROCESS, _prec.id, self.who, message, {"name": "publish"}))

            stat.set_state(poststat)
            stat.act(self.STATUS_ACTION_PUBLISH, message, self.who.actor)
            _prec.save()

#        self.log.info("Submitted %s record %s (%s) for %s for final publication",
#                      self.dbcli.project, _prec.id, _prec.name, self.who)
        return stat.clone()
        
    def free(self):
        """
        free up resources used by this service.  

<<<<<<< HEAD
    def _publish(self, prec: ProjectRecord):
        """
        Actually launch the publishing process on the given record and update its state  
        accordingly.

        This method should be overridden to provide project-specific handling.  This generic 
        implementation will simply copy the data contents of the record to another collection.

        This default implement will save the data in a project record with PROJCOLL_latest collection
        (and the PROJCOLL_version collection).  

        :returns:  the label indicating its post-editing state
                   :rtype: str
        :raises NotSubmitable:  if this record is not in a publishable state.
        :raises SubmissionFailed:  if an error occurs while submitting the record for publication.
                             This error is typically not due to anything the client did, but rather 
                             reflects a system problem (e.g. from a downstream service). 
        """
        endstate = status.PUBLISHED    # or could be status.SUBMITTED
        try:
            latestcli = self.dbcli.client_for(f"{self.dbcli.project}_latest", AUTOADMIN)
            versioncli = self.dbcli.client_for(f"{self.dbcli.project}_version", AUTOADMIN)

            recd = prec.to_dict()
            recd['id'] = self._arkify_recid(prec.id)
            latest = ProjectRecord(latestcli.project, deepcopy(recd), latestcli)
            recd['id'] += "/pdr:v/" + recd['data'].get("@version", "0")
            version = ProjectRecord(versioncli.project, deepcopy(recd), versioncli)

            # Fix permissions, state
            for pubrec in (latest, version):
                pubrec.status.set_state(endstate)

                # no one can delete, write, or admin (except superusers)
                pubrec.acls.revoke_perm_from_all(ACLs.DELETE)
                pubrec.acls.revoke_perm_from_all(ACLs.WRITE)
                pubrec.acls.revoke_perm_from_all(ACLs.ADMIN, protect_owner=False)

                # everyone can read
                pubrec.acls.revoke_perm_from_all(ACLs.READ, protect_owner=False)
                pubrec.acls.grant_perm_to(ACLs.READ, PUBLIC_GROUP)

            version.save()
            latest.save()

        except Exception as ex:
            # TODO: back out version?
            self.log.error("%s: Problem with default publication submission: %s", prec.id, str(ex))
            raise SubmissionFailed(prec.id) from ex

        if endstate == status.PUBLISHED:
            base = self.cfg.get('published_resolver_ep', 'midas:')
            prec.status.publish(recd['id'], recd['data'].get("@version", "0"),
                                f"{base}{self.dbcli.project}_latest/{recd['id']}")

        self.log.info("Successfully published %s as %s version %s (into %s_latest collection)",
                      prec.id, recd['id'], recd['data'].get("@version", 0), self.dbcli.project)
        return endstate

=======
        The client of this service can call this method when it is finished using it.  The implementation
        should *not* disable the service, making the instance unusable for further use; it should just free
        up resources as possible.  This implementation calls the ``free()`` function on the underlying 
        :py:class:`~nistoar.midas.dbio.base.DBClient` instance.
        """
        self.dbcli.free()
                    
>>>>>>> usnistgov/integration

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

class SubmissionFailed(DBIORecordException):
    """
    An error indicating that the process of submitting a record failed due to a system problem
    (rather than a problem with the record; see :py:class:`NotSubmitable`).
    """
    def __init__(self, recid, message=None, sys=None):
        """
        initialize the exception
        """
        if not message:
            message = "%s: system error occurred while submitting record" % recid
        super(DBIORecordException, self).__init__(recid, message, sys=sys)
    

