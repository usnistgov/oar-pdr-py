"""
a module providing the :py:class:`ProjectRecordBroker` class, a base for classes that hold the business 
logic for creating and updating MIDAS DBIO project records.  `ProjectRecordBroker` classes mediate 
between a RESTful web interface and the :py:module:`~nistoar.midas.dbio<DBIO>` layer.  Broker classes 
can be subclassed to provide specialized logic for a particular project record type (e.g. DMP, 
EDI draft).  
"""
from logging import Logger
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping, Sequence

from .. import DBClient, ProjectRecord
from ..base import AlreadyExists, NotAuthorized, ObjectNotFound
from ... import MIDASException
from nistoar.pdr.publish.prov import PubAgent


class ProjectRecordBroker:
    """
    A base class for handling requests to create, access, or update a project record.  This generic 
    base can be used as is or extended and overridden to specialize the business logic for updating 
    a particular type of project.  
    """

    def __init__(self, dbclient: DBClient, config: Mapping={}, who: PubAgent=None,
                 wsgienv: dict=None, log: Logger=None):
        """
        create a request handler
        :param DBClient dbclient:  the DBIO client instance to use to access and save project records
        :param dict       config:  the handler configuration tuned for the current type of project
        :param dict      wsgienv:  the WSGI request context 
        :param Logger        log:  the logger to use for log messages
        """
        self.dbcli = dbclient
        self.cfg = config
        if not who:
            who = PubAgent("unkwn", prov.PubAgent.USER, self.dbcli.user_id or "anonymous")
        self.who = who
        if wsgienv is None:
            wsgienv = {}
        self.env = wsgienv
        self.log = log

    def create_record(self, name, data=None, meta=None):
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

        prec.data = self._new_data_for(prec.id)
        if meta:
            self._merge_into(self._moderate_metadata(meta), prec.meta)
        if data:
            self.update_data(prec.id, data, prec=prec)  # this will call prec.save()
        elif meta:
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
            self.log.info("No default ID shoulder configured for client group, %s", user.group)
            raise NotAuthorized(user.actor, "create record",
                                "Client group, %s, not recognized" % user.group)

        out = client_ctl.get('default_shoulder')
        if not out:
            raise NotAuthorized(user.actor, "create record",
                                "No default shoulder defined for client group, "+user.group)
        return out

    def get_record(self, id):
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
            prec = self.dbcli.get_record_for(id)   # may raise ObjectNotFound/NotAuthorized

        if not part:
            # this is a complete replacement; merge it with a starter record
            self._merge_into(newdata, prec.data)

        else:
            # replacing just a part of the data
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

    def _new_data_for(self, recid):
        """
        return an "empty" data object set for a record with the given identifier.  The returned 
        dictionary can contain some minimal or default properties (which may or may not include
        the identifier or information based on the identifier).  
        """
        return OrderedDict()

    def _new_metadata_for(self, recid):
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

    def _moderate_metadata(self, mdata: MutableMapping):
        """
        massage and validate the given record metadata provided by the user client, returning a 
        valid version of the metadata.  The implementation may modify the given dictionary in place. 
        The default implementation does accepts none of the client-provided properties
        
        The purpose of this function is to filter out data properties that are not supported or 
        otherwise should not be settable by the client.  
        :raises ValueError:   if the mdata is disallowed in a way that should abort the entire request.
        """
        return OrderedDict()

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
            prec = self.dbcli.get_record_for(id)   # may raise ObjectNotFound/NotAuthorized

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


class InvalidUpdate(MIDASException):
    """
    an exception indicating that the user-provided data is invalid or otherwise would result in 
    invalid data content for a record. 
    """
    def __init__(self, message, recid=None, part=None):
        """
        initialize the exception
        :param str recid:  the id of the record that was existed
        :param str  part:  the part of the record that was requested.  Do not provide this parameter if 
                           the entire record does not exist.  
        """
        super(InvalidUpdate, self).__init__(message)
        self.record_id = recid
        self.record_part = part
    
class PartNotAccessible(MIDASException):
    """
    an exception indicating that the user-provided data is invalid or otherwise would result in 
    invalid data content for a record. 
    """
    def __init__(self, recid, part, message=None):
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
        super(PartNotAccessible, self).__init__(message)


    
