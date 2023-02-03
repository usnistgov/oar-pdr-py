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
from logging import Logger, getLogger
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping, Sequence
from typing import List

from .base import (DBClient, DBClientFactory, ProjectRecord, ACLs,
                   AlreadyExists, NotAuthorized, ObjectNotFound, DBIOException)
from .. import MIDASException, MIDASSystem
from nistoar.pdr.publish.prov import PubAgent

class ProjectService(MIDASSystem):
    """
    A base class for a service to create, access, or update a project.  This generic 
    base can be used as is or extended and overridden to specialize the business logic for updating 
    a particular type of project under particular conventions or policies.  The service is attached 
    to a particular user at construction time (as given by a :py:class:`~nistoar.pdr.publish.prov.PubAgent`
    instance); thus, requests to this service are subject to internal Authorization checks.

    This base service supports a two parameters, ``dbio`` and ``clients``.  The optional ``dbio`` 
    parameter will be passed to the :py:class:`~nistoar.midas.dbio.base.DBClientFactory`'s 
    ``create_client()`` function to create the :py:class:`~nistoar.midas.dbio.base.DBClient`. 

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

    def __init__(self, project_type: str, dbclient_factory: DBClient, config: Mapping={},
                 who: PubAgent=None, log: Logger=None, _subsys=None, _subsysabbrev=None):
        """
        create the service
        :param str  project_type:  the project data type desired.  This name is usually used as the 
                                   name of the collection in the backend database.  Recognized values
                                   include ``dbio.DAP_PROJECTS`` and ``dbio.DMP_PROJECTS``
        :param DBClient dbclient:  the DBIO client instance to use to access and save project records
        :param dict       config:  the handler configuration tuned for the current type of project
        :param who      PubAgent:  the representation of the user that is requesting access
        :param Logger        log:  the logger to use for log messages
        """
        if not _subsys:
            _subsys = "DBIO Project Service"
        if not _subsysabbrev:
            _subsysabbrev = "DBIO"
        super(ProjectService, self).__init__(_subsys, _subsysabbrev)
        self.cfg = config
        if not who:
            who = PubAgent("unkwn", PubAgent.USER, "anonymous")
        self.who = who
        if not log:
            log = getLogger(self.system_abbrev).getChild(self.subsystem_abbrev).getChild(project_type)
        self.log = log

        user = who.actor if who else None
        self.dbcli = dbclient_factory.create_client(project_type, self.cfg.get("dbio", {}), user)

    @property
    def user(self) -> PubAgent:
        """
        the PubAgent instance representing the user that this service acts on behalf of.
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
        prec = self.dbcli.create_record(name, shoulder)

        if meta:
            meta = self._moderate_metadata(meta, shoulder)
            if prec.meta:
                self._merge_into(meta, prec.meta)
            else:
                prec.meta = meta
        elif not prec.meta:
            prec.meta = self._new_metadata_for(shoulder)
        prec.data = self._new_data_for(prec.id, prec.meta)
        if data:
            self.update_data(prec.id, data, prec=prec)  # this will call prec.save()
        else:
            prec.save()

        return prec

    def _get_id_shoulder(self, user: PubAgent):
        """
        return an ID shoulder that is appropriate for the given user agent
        :param PubAgent user:  the user agent that is creating a record, requiring a shoulder
        :raises NotAuthorized: if an uathorized shoulder appropriate for the user cannot be determined.
        """
        out = None
        client_ctl = self.cfg.get('clients', {}).get(user.group)
        if client_ctl is None:
            client_ctl = self.cfg.get('clients', {}).get("default")
        if client_ctl is None:
            self.log.debug("Unrecognized client group, %s", user.group)
            raise NotAuthorized(user.actor, "create record",
                                "Client group, %s, not recognized" % user.group)

        out = client_ctl.get('default_shoulder')
        if not out:
            self.log.info("No default ID shoulder configured for client group, %s", user.group)
            raise NotAuthorized(user.actor, "create record",
                                "No default shoulder defined for client group, "+user.group)
        return out

    def get_record(self, id) -> ProjectRecord:
        """
        fetch the project record having the given identifier
        :raises ObjectNotFound:  if a record with that ID does not exist
        :raises NotAuthorized:   if the record exists but the current user is not authorized to read it.
        """
        return self.dbcli.get_record_for(id)

    def get_data(self, id, part=None):
        """
        return a data content from the record with the given ID
        :param str   id:  the record's identifier
        :param str path:  a path to the portion of the data to get.  This is the same as the `datapath`
                          given to the handler constructor.  This will be an empty string if the full
                          data object is requested.
        :raises ObjectNotFound:  if no record with the given ID exists or the `part` parameter points to 
                          a non-existent part of the data content.  
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                          given by `id`.  
        :raises PartNotAccessible:  if access to the part of the data specified by `part` is not allowed.
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

    def update_data(self, id, newdata, part=None, prec=None):
        """
        merge the given data into the currently save data content for the record with the given identifier.
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
        if not prec:
            prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized

        if not part:
            # updating data as a whole: merge given data into previously saved data
            self._merge_into(newdata, prec.data)

        else:
            # updating just a part of the data
            steps = part.split('/')
            data = prec.data
            while steps:
                prop = steps.pop(0)
                if prop not in data or data[prop] is None:
                    if not steps:
                        data[prop] = newdata
                    else:
                        data[prop] = {}
                elif not steps:
                    if isinstance(data[prop], Mapping) and isinstance(newdata, Mapping):
                        self._merge_into(newdata, data[prop])
                    else:
                        data[prop] = newdata
                elif not isinstance(data[prop], Mapping):
                    raise PartNotAccessible(id, part,
                                            "%s: data property, %s, is not in an updatable state")
                data = data[prop]

        data = prec.data

        # ensure the replacing data is sufficiently complete and valid and then save it
        # If it is invalid, InvalidUpdate is raised.
        data = self._save_data(data, prec)

        return self._extract_data_part(data, part)


    def _merge_into(self, update: Mapping, base: Mapping, depth: int=-1):
        if depth == 0:
            return

        for prop in update:
            if prop in base and isinstance(base[prop], Mapping):
                if depth > 1 and isinstance(update[prop], Mapping):
                    # the properties from the base and update must both be dictionaries; otherwise,
                    # update is ignored.
                    self._merge_into(base[prop], update[prop], depth-1)
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
        return out

    def replace_data(self, id, newdata, part=None, prec=None):
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
        if not prec:
            prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized

        if not part:
            # this is a complete replacement; merge it with a starter record
            data = self._new_data_for(id)
            self._merge_into(newdata, data)

        else:
            # replacing just a part of the data
            data = prec.data
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

            data = prec.data

        # ensure the replacing data is sufficiently complete and valid.
        # If it is invalid, InvalidUpdate is raised.
        data = self._save_data(data, prec)

        return self._extract_data_part(data, part)

    def _save_data(self, indata: Mapping, prec: ProjectRecord = None) -> Mapping:
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
        :return:  the (transformed) data that was actually saved
                  :rtype: dict
        :raises InvalidUpdate:  if the provided `indata` represents an illegal or forbidden update or 
                             would otherwise result in invalid data content
        """
        # this implementation does not transform the data
        self._validate_data(indata)  # may raise InvalidUpdate

        prec.data = indata
        prec.save();

        return indata

    def _validate_data(self, data):
        pass

    def clear_data(self, id, part=None, prec=None):
        """
        remove the stored data content of the record and reset it to its defaults.  
        :param str      id:  the identifier for the record whose data should be cleared.
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             only that property will be cleared (either removed or set to an initial
                             default).
        :param ProjectRecord prec:  the previously fetched and possibly updated record corresponding to `id`.
                             If this is not provided, the record will by fetched anew based on the `id`.  
        :raises ObjectNotFound:  if no record with the given ID exists or the `part` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by `id`.  
        :raises PartNotAccessible:  if clearing of the part of the data specified by `part` is not allowed.
        """
        if not prec:
            prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized

        initdata = self._new_data_for(prec.id, prec.meta)
        if not part:
            # clearing everything: return record to its initial defaults 
            prec.data = initdata

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
                    break
                elif not steps:
                    del data[prop]
                    break
                data = data[prop]
                initdata = initdata.get(prop, {})
                    

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

    def create_service_for(self, who: PubAgent=None):
        """
        create a service that acts on behalf of a specific user.  
        :param PubAgent who:    the user that wants access to a project
        """
        return ProjectService(self._prjtype, self._dbclifact, self._cfg, who, self._log)


class InvalidUpdate(DBIOException):
    """
    an exception indicating that the user-provided data is invalid or otherwise would result in 
    invalid data content for a record. 

    The determination of invalid data may result from detailed data validation which may uncover 
    multiple errors.  The ``errors`` property will contain a list of messages, each describing a
    validation error encounted.  The :py:meth:`format_errors` will format all these messages into 
    a single string for a (text-based) display. 
    """
    def __init__(self, message: str=None, recid=None, part=None, errors: List[str]=None, sys=None):
        """
        initialize the exception
        :param str message:  a brief description of the problem with the user input
        :param str   recid:  the id of the record that was existed
        :param str    part:  the part of the record that was requested.  Do not provide this parameter if 
                             the entire record does not exist.  
        :param [str] errors: a listing of the individual errors uncovered in the data
        """
        if errors:
            if not message:
                if len(errors) == 1:
                    message = "Validation Error: " + errors[0]
                elif len(errors) == 0:
                    message = "Unknown validation errors encountered"
                else:
                    message = "Encountered %d validation errors, including: %s" % (len(errors), errors[0])
        elif message:
            errors = [message]
        else:
            message = "Unknown validation errors encountered while updating data"
            errors = []
        
        super(InvalidUpdate, self).__init__(message)
        self.record_id = recid
        self.record_part = part
        self.errors = errors

    def __str__(self):
        out = ""
        if self.record_id:
            out += "%s: " % self.record_id
        if self.record_part:
            out += "%s: " % self.record_part
        return out + super().__str__()

    def format_errors(self):
        """
        format into a string the listing of the validation errors encountered that resulted in 
        this exception.  The returned string will have embedded newline characters for multi-line
        text-based display.
        """
        if not self.errors:
            return str(self)

        out = ""
        if self.record_id:
            out += "%s: " % self.record_id
        out += "Validation errors encountered"
        if self.record_part:
            out += " in data submitted to update %s" % self.record_part
        out += ":\n  * "
        out += "\n  * ".join([str(e) for e in self.errors])
        return out
    
class PartNotAccessible(DBIOException):
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
        self.record_id = recid
        self.record_part = part

        if not message:
            message = "%s: data property, %s, is not in an updateable state" % (recid, part)
        super(PartNotAccessible, self).__init__(message, sys=sys)


    
