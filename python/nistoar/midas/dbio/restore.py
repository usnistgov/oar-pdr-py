"""
a module that defines an interface for restoring project records and provides default implementations
"""
from __future__ import annotations
import re
from logging import Logger, getLogger
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Mapping
from urllib.parse import urlparse

from nistoar.pdr.utils.prov import Action, Agent
from .base import (ProjectRecord, DBClient, ACLs, DBIOException, DBIORecordException,
                   NotAuthorized, ObjectNotFound)

import requests

class ProjectRestorer(ABC):
    """
    an abstract interface for restoring a published DBIO project from its archive state.  

    An object of this type is constructed to restore a particular project from a particular 
    archive location, both of which are established at construction time.  Often, calling 
    :py:meth:`restore` or :py:meth:`get_data` will call :py:meth:`recover` internally as 
    necessary to pull the archived data from its archived location.  This may cause internally
    managed resources or artifacts to be created (e.g. a data cache); these can be cleaned up
    with a call to :py:meth:`free`.
    """

    @abstractmethod
    def get_data(self) -> Mapping:
        """
        return the archived data portion of the record.  Generally, this should be the full data
        record, not a summary, regardless of what is usually stored into a project record.
        :raises ObjectNotFound: if the identified project cannot be found at the archived location
        :raises NotAuthorized:  if the current user is not authorized to access the project at the 
                                archived location.  As published versions of records are typically 
                                publicly accessible, this exception is not commonly raised.
        :raises DBIOExcption:   if some other error occurs preventing recovery of the archive record.
        """
        raise NotImplemented()

    @abstractmethod
    def restore(self, prec: ProjectRecord, dofree: bool=False) -> bool:
        """
        recover the archived record data and load it into the given ProjectRecord.  

        Typically, this is a ProjectRecord that has its identifier assigned and current 
        owner and permissions already set; so normally only the ``data`` property (and 
        possibly the ``meta`` property) is reset.  Generally, the record will not be yet 
        be saved after this method is called.  

        :param ProjectRecord prec:  the record to load with archived data
        :param bool dofree:         If True, the :py:meth:`free` method will be called automatically 
                                    after the update is done.  (Default: False)
        :return:  True, if after restoration, it is still necessary to save the record.
        :raises ObjectNotFound: if the identified project cannot be found at the archived location
        :raises NotAuthorized:  if the current user is not authorized to access the project at the 
                                archived location.  As published versions of records are typically 
                                publicly accessible, this exception is not commonly raised.
        :raises DBIOExcption:   if some other error occurs preventing recovery of the archive record.
        """
        raise NotImplemented()

    @abstractmethod
    def recover(self):
        """
        if necessary, fetch the archived data from the archive location (set at construction time)
        and cache it internally.  The :py:meth:`free` method can be used to clean up any cached data
        resulting from this call.
        :raises ObjectNotFound: if the identified project cannot be found at the archived location
        :raises NotAuthorized:  if the current user is not authorized to access the project at the 
                                archived location.  As published versions of records are typically 
                                publicly accessible, this exception is not commonly raised.
        :raises DBIOExcption:   if some other error occurs preventing recovery of the archive record.
        """
        raise NotImplemented()

    @abstractmethod
    def free(self):
        """
        clean up any resources or cached data resulting from the recovery of the published data.  

        Temporary resources may have been created/allocated as a result of calls to :py:meth:`restore` 
        or :py:meth:`get_data`.  This function deletes them (as needed).
        """
        raise NotImplemented()

class DBIORestorer(ProjectRestorer):
    """
    A Restorer that pulls its record data from the default published collection in the DBIO database.

    This is the default restore for DBIO project records.  The record's ``archived_at`` property 
    should have the form ``dbio_store:``_collection_``/``_pubid_, where _collection_ is the name of 
    the DBIO project collection that contains the latest published versions of records and _pubid_ 
    is the identifier given to the record at publication time (which may be different from its 
    DBIO draft ID).  As an example, the collection containing published DMP records is called 
    ``dmp_latest``.  
    """

    def __init__(self, dbcli: DBClient, coll: str, pubid: str, log: Logger=None):
        """
        create a restorer for a record published with a given identifier into a particular 
        DBIO collection.
        :param DBClient dbcli:  the DBIO client that manages the ProjectRecord that will be restored
        :param str       coll:  the name of the collection that contains published records
        :param str      pubid:  the identifier of the published record
        :param Logger     log:  a Logger to use for messages; if not provided, a default will be used
                                if needed
        """
        self.pubcli = dbcli.client_for(coll)
        self.pubid = pubid
        self._pubrec = None
        self._log = log

    @property
    def log(self):
        if not self._log:
            return getLogger(self.__class__.__name__)
        return self._log

    def recover(self):
        self.free()
        try:
            self._pubrec = self.pubcli.get_record_for(self.pubid, ACLs.READ)
        except DBIOException:
            raise
        except Exception as ex:
            # not expected
            self.log.exception(ex)
            raise DBIORecordException(self.pubid,
                                      "Unexpected error while retrieving %s from %s: %s" %
                                      (self.pubid, self.pubcli.project)) from ex

    def free(self):
        self._pubrec = None

    def get_data(self):
        if not self._pubrec:
            self.recover()    # may raise DBIOException

        return OrderedDict(self._pubrec.data)

    def restore(self, prec: ProjectRecord, dofree: bool=False) -> bool:
        if not self._pubrec:
            self.recover()    # may raise DBIOException

        prec.data = self._pubrec.data

        if dofree:
            self.free()

    @classmethod
    def from_archived_at(cls, locurl: str, dbcli: DBClient,
                         config: Mapping={}, log: Logger=None) -> DBIORestorer:
        """
        instantiate a DBIORestorer given an ``archived_at`` URL.  

        :param str locurl:  the ``archived_at`` URL for the published project record to restore.  
                            This _must_ have the form, "dbio_store:_collection_/_pubid_"
        :param DBClient dbcli:  the DBClient for the draft project record that will be restored
        :param dict config: The configuration for the restorer; in this implementation, this is 
                            ignored.
        :param Logger log:  a Logger to use for messages; if not provided, a default will be used
                            if needed
        :rtype: DBIORestorer
        :raises ValueError:  if locurl does not comply with the proper URL form
        """
        pat = re.compile("^dbio_store:([\w\-]+)/(\w[\w\/\-\+=:]+)$")
        m = pat.match(locurl)
        if not m:
            if not locurl.startswith("dbio_store"):
                raise ValueError("Not a dbio_store URL: "+locurl)
            raise ValueError("Non-compliant dbio_store URL: "+locurl)

        return cls(dbcli, m.group(1), m.group(2))

class URLRestorer(ProjectRestorer):
    """
    a ProjectRestorer that retrieves the project data from an HTTP URL
    """

    def __init__(self, dataurl: str, projid: str=None, log: Logger=None):
        """
        create the restorer.

        :param str dataurl:  an HTTP or HTTPS URL that, when resolved (with a GET), returns the data 
                             portion of the desired record
        :param str  projid:  the ID corresponding to the project record being restored.  It doesn't 
                             matter if this is the published ID or the one corresponding to the DAP
                             draft, as long as it is unique to the record.  
        :param str     log:  a Logger to use for messages.  This is typically used for reporting on 
                             unexpected errors.  If not provided, a default Logger will be used. 
        """
        if not dataurl:
            raise ValueError("URLRestorer: dataurl not provided")
        if not dataurl.startswith("https:") and not dataurl.startswith("http:"):
            raise ValueError("URLRestorer: dataurl not an HTTP(S) URL")
        try:
            urlparse(dataurl).port
        except ValueError:
            raise ValueError("URLRestorer: dataurl is malformed URL")
        self._src = dataurl
        self._id = projid
        self._data = None
        self._log = log

    @property
    def log(self):
        if not self._log:
            return getLogger(self.__class__.__name__)
        return self._log

    def recover(self):
        self.free()
        try:
            resp = requests.get(self._src, hdrs={ "Accept": "application/json" })

            if resp.status_code > 500:
                raise DBIOException("Server error while accessing project data archive: "+resp.reason)
            elif resp.status_code > 400:
                msg = "Archive URL "+resp.reason
                if self._id:
                    msg += f" ({self._src})"
                if resp.status_code == 404:
                    raise ObjectNotFound(self._id or self._src, message=msg)
                elif resp.status_code == 401:
                    raise NotAuthorized(self._id or self._src, message=msg)
                elif resp.status_code == 406:
                    msg = "Archive URL cannot return JSON as expected: "+self._src
                    raise NotAuthorized(self._id or self._src, message=msg)
                else:
                    raise DBIOException(self._id or self._src, message=msg)
            elif resp.status_code >= 300 or resp.status_code < 200:
                raise DBIOException("Unexpected %d (%s) response accessing archive URL, %s" %
                                    (resp.status_code, resp.reason, self._src))

            self._data = resp.json(object_pairs_hook=OrderedDict)

        except ValueError as ex:
            if resp and resp.text and \
               ("<body" in resp.text or "<BODY" in resp.text):
                raise DBIOException("HTML returned where JSON expected (is service URL correct?): " +
                                    self._src) from ex
            else:
                raise DBIOException("Unable to parse response as JSON (is service URL correct?): " +
                                    self._src) from ex

        except requests.RequestException as ex:
            raise DBIOException("Server communication error while accessing %s: %s" %
                                (self._src, str(ex))) from ex

        except DBIOException:
            raise

        except Exception as ex:
            # not expected
            self.log.exception(ex)
            raise DBIORecordException(self._id or "unknown",
                                      "Unexpected error while retrieving %s from %s: %s" %
                                      (self._id or "published data", self._src, str(ex))) from ex

    def free(self):
        self._data = None

    def get_data(self):
        if not self._data:
            self.recover()    # may raise DBIOException

        return OrderedDict(self._data)

    def restore(self, prec: ProjectRecord, dofree: bool=False) -> bool:
        if not self._data:
            self.recover()    # may raise DBIOException

        prec.data = self._data

        if dofree:
            self.free()


    @classmethod
    def from_archived_at(cls, locurl: str, dbcli: DBClient,
                         config: Mapping={}, log: Logger=None) -> DBIORestorer:
        """
        instantiate a DBIORestorer given an ``archived_at`` URL.  

        :param str locurl:  the ``archived_at`` URL for the published project record to restore.  
                            This _must_ be a compliant HTTP or HTTPS URL (i.e starts with "http://"
                            or "https://"
        :param DBClient dbcli:  the DBClient for the draft project record that will be restored;
                            In this implementation, this is ignored and can, thus, can be None.
        :param dict config: The configuration for the restorer; in this implementation, this is 
                            ignored.
        :param Logger log:  a Logger to use for messages; if not provided, a default will be used
                            if needed
        :rtype: DBIORestorer
        :raises ValueError:  if locurl does not comply with the proper URL form
        """
        if not locurl.startswith('http://') and not locurl.startswith('https://'):
            raise ValueError("Not an HTTP(S) URL: "+locurl)
        return cls(locurl, log=log)
        
        
        

        

        
