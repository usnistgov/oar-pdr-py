"""
dbio:  a module for accessing information from a common database.  

In the MIDAS framework, a common database model can be used for storing different types of _project 
records_ that can be created by users.  There are two key types supported currently: *DMPs* (Data 
Management Plans) and *draft EDIs* (Enterprise Data Inventory records).  This module provides an 
interface to access and update those records through the life cycle of the records.  This includes 
managing authorization to access or update the records.

----------------------
Typical Use
----------------------

Access to the database starts by obtaining a :py:class:`~nistoar.midas.dbio.base.DBClient` instance 
from a :py:class:`~nistoar.midas.dbio.base.DBClientFactory`.  To create a factory, you need to pass in 
a configuration dictionary (see :ref:`ref-dbio-config` below).  To create the database client instance,
you indicate the type of record you want to access and the identity of the end user requesting access.

.. code-block::
   :caption: Example use of the DBIO module

   from nistoar.midas import dbio

   # the factory will need a configuration (see CLIENT CONFIGURATION section)
   storeconfig = { "mongodb//localhost:27017/MIDAS" }  # storage-specific configuration
   config = { "default_shoulder": "mdst" }

   # connect to the DMP collection
   client = dbio.MIDASDBClientFactory(storeconfig).create_client(dbio.DMP_PROJECTS, config, userid)

   # create a new record:
   rec = client.create_record(user_specified_rec_name)
   recid = rec.id
   rec.data.update({ "searchable": True })
   rec.save()

   # access and update an existing 
   rec = client.get(recid)
   rec.data.update({ "title": user_provided_title })
   rec.save()

--------------
Database Model
--------------

The database is made up of various *collections* to hold the different types of records.  (The nature 
of the collections depends on the implementation of the database backend; for example, if the backend 
is an SQL relational database, then a collection would be represented by a table or interlinked tables.)
In particular, each *key* (or *project*) record type (dmp or dap) has its own collection associated 
with it; these collections have logical names (accessible via ``dbio.DMP_PROJECTS`` and 
``dbio.DAP_PROJECTS``).  Other collections are supported as well, including one that tracks 
user-defined user groups and another capturing people that can servce as authors or collaborators in a 
project.

^^^^^^^^^^^^^^^
Project Records
^^^^^^^^^^^^^^^

Each project record has the following properties associated with it:

``id``
   a unique identifier for the record.  This is usually of the form, *shldr:####*, where *shldr* is a 
   namespace *shoulder* naming the set of identifiers it belongs to, and *####* is a number that is 
   unique within that namespace.  

``owner``
   the user identifier for the user that owns this record.  This is usually the user that created the 
   record, but it is possible to transfer ownership of a record to another user after it is created. 

``name``
   a mnumonic name given to the record by the owner.  It is not globally unique (like the `id`); 
   however, it should be unique among records owned by the same user.  

``data``
   a dictionary of user-updatable data.  The properties in this dictionary typically correspond to things
   the user can set directly and which would be displayed to others as the records logical content.

``meta``
   a dictionary of indirectly-updatabe data.  These properties generally are not consider part of the 
   formal contents of the record and generally cannot be directly updated by the user.  The application
   sets/updates the properties on behalf of the user as part of managing the lifecycle of the record.  

``acls``
   access control lists that indicating which users are authorized for different types of operations 
   on the record, like reading and writing.  See :ref:`ref-dbio-acls` below for more details. 

A project record supports other properties as well; see :py:class:`~nistoar.midas.dbio.ProjectRecord` for 
more details.

^^^^^^^^^^^
User Groups
^^^^^^^^^^^

Users have the ability to define their own *groups*.  A group is a named set of users intended to share a 
common set of permissions on project records.  Once defined it can be assigned permissions just like any 
user, and those permissions will be afforded to each of the users that make up a group.  Another group 
can also be made a member of a group, and the permissions will be applied transitively to the members of 
that second group.  

Like project records, a group is assigned a unique ``id`` and a ``name``, where the ``id`` is globally 
unique but the `name` (usually specified by the owner) is only unique only to the owner (that is two 
users can define groups with the same name).  A group can be used to assign permissions to any type of 
project record.  Another key property of a group is its list of members (each represented either as a 
user or a group identifier).  Finally, a group also has an `acls` property which controls which users 
can use the group (i.e. read its membership) or update its membership.  

The DBIO framework supports one built-in group with the ``id``, ``grp0:public`` which implicitly includes
all users in its membership.  This group can be used to, say, assign read permission on a record to all 
users.  

For more informations on creating groups, see :py:class:`~nistoar.midas.dbio.DBGroups`.  For information
on setting a group's membership see :py:class:`~nistoar.midas.dbio.Group`.

^^^^^^
People
^^^^^^

*editing in progress*


.. _ref-dbio-config

---------------------
Client Configuration
---------------------

Database clients can be configured by passing a configuration dictionary to the factory constructor.  
All backend types support the following configuration properties:

``superusers``
    a list of user identifiers for users that should be considered *superusers* who implicitly have 
    full permissions on all records.  If not specified, no users will have this status.

``default_shoulder``
    the identifier prefix--i.e. the ID *shoulder*--that will be used to create the identifier for 
    a new project record if one is not specified in the call to 
    :py:method:`~nistoar.midas.dbio.DBClient.create_record`.  This is effectively a required 
    parameter; however, if not specified, ``allowed_project_shoulders`` must be set to create new 
    project records.

``allowed_project_shoulders``
    a list of shoulders that one can request when creating new project records via 
    :py:method:`~nistoar.midas.dbio.DBClient.create_record`.  Note that the value the of 
    ``default_shoulder`` is implicitly added to this list; thus, if not specified, the 
    default is the ``default_shoulder``.

``allowed_group_shoulders``
    a list of shoulders that one can request when creating new user groups via 
    :py:method:`~nistoar.midas.dbio.DBGroups.create_group`.  If not specified, the 
    default is the value of ``dbio.base.DEF_GROUP_SHOULDER``.  

Specific :ref:`backend implementations` may define additional supported configuration properties; see
the factory's class documentation for details.

.. _ref-dbio-backends

-----------------------
Backend Implementations
-----------------------

The DBIO framework allows for different backend database stores; the desired implementation is chosen 
by using the corresponding :py:class:`~nistoar.midas.dbio.DBClientFactory` implementation.  The 
implementation intended for production use is made available via `dbio.MIDASDBClientFactory` 
(currently set to :py:class:`~nistoar.midas.dbio.mongo.MongoDBClientFactory`); however, 
other implementations are available:

:py:class:`nistoar.midas.dbio.mongo.MongoDBClientFactory`
    An implementation that stores all collections in a MongoDB database.  To use, a separate MongoDB
    instance must be running.  The database URL (indicating the connection point of the database)
    must be provided either via the factory's constructor or via configuration; see the 
    :py:class:`class documentation <nistoar.midas.dbio.mongo.MongoDBClientFactory>` for details.

:py:class:`nistoar.midas.dbio.inmem.InMemoryDBClientFactory`
    An implementation where the data is held completely within data structures kept in memory.  This 
    implementation is provided primarily for use in unit tests.  The database persists in memory for 
    the lifetime of the factory and all the clients it produces.  

:py:class:`nistoar.midas.dbio.fsbased.FSBasedDBClientFactory`
    An implementation where the data is persisted as JSON files on disk under a common file directory.  
    This implementation is generally easier to setup and can be useful for debugging, but will not 
    be as performant as a real database backend under production conditions.


.. code-block::
   :caption: Example creating clients using different backend implementations

   import os
   from nistoar.midas.dbio import inmem, fsbased

   # The in-memory implementation for unit tests
   config = { "default_shoulder": "mdst" }
   fact = inmem.InMemoryDBClientFactory(config)

   # The file-based implementation
   os.makedirs("./db", exist_ok=True)
   fact = fsbased.FSBasedDBClientFactory(config, "./db")

"""
from .base    import *
from .mongo   import MongoDBClientFactory
from .inmem   import InMemoryDBClientFactory
from .fsbased import FSBasedDBClientFactory

MIDASDBClientFactory = MongoDBClientFactory

from .project import ProjectService, ProjectServiceFactory, InvalidUpdate
