.. _ch_intro:

************
Introduction
************

What the PDR Does
=================

The NIST Public Data Repository (PDR) is one compnent of the NIST Open Access to Research
(OAR) Data System.  Its purpose is to manage and make available to the public the research
data outputs from NIST and NIST-funded researchers.  In particular, it is the platform,
analogous to a scientific journal, through which NIST researchers may publish their data;
consequently, special emphasis is placed on how the data is presented and accessed to
maximize its usefulness to the ongoing research process.  The products that are published
through the PDR are not limited to simple data files; it can also include software,
services, and web sites.  Thus, we often refer to such publications as *digital asset
publications* (DAP).  

The overall function of the PDR platform is to accept new data submissions and
turn them into a publication accessible through its web site.  We can break
down this overall goal into the following sub-functions:

* from a submission, create a data package for long-term preservation; the
  purpose of this package is ensure that its contents can be understood long into
  the future.  
* create and maintain a web-accessible landing page which presents the
  publication, providing a descriptive view of the data as well as links for
  accessing it.
* provide web-access to data entities (also referred to as *data products*) in
  the publication.  (Some data products may not be made web-accessible directly;
  this might include data that might be delivered by digital media for a fee.)
* provide tools and linkages that enhance the value and usability of the data
  products.

One key value-enhancing tool typically provided by a data repository is a search tool of
some kind which allows users to discover data products in the repository relevant to
particular research interests.  Currently, search capabilities are not strictly part of
the PDR ``nistoar`` Software; searching is provided by other OAR components--namely, the
Resource Metadata Manager (RMM, providing the back-end service) and the Science Data
Portal (SDP, providing the web front-end).

Another value-enhancing feature of a typical data repository is the assignment and
management of a `Digital Object Identifier (DOI) <https://doi.org/>`_ to data in the
repository.  Such an identifier provides a persistent (that is, long-lived) pointer to the
data.  The PDR metadata model explicitly supports DOIs (as well
`ORCIDs <https://orcid.org.>`_ for identifying authors): each data publication may have a
DOI assigned to it which resolves to its landing page.  A publication is not required to
have a DOI; nevertheless, every publication has a PDR-specific persistant identifier
assigned to it, and long-term URLs are assigned to publications and its various
constituents. 

Data products that are part of data publications in the PDR can be stored anywhere on the
web as access to a product is provided simply via a URL.  In the PDR deployed at NIST,
storage and access to data products is restricted to NIST-managed resources and web sites.
When the data products are submitted to the PDR as actual data files rather than links to
products stored elsewhere, the data is packaged up and delivered to long term,
preservation storage.  The files are also made available through the Data Distribution
Service (DDS); this service is also not strictly part of the PDR software and is handled
as a stand-alone service (which could deliver other products that are not in the
repository).

Architecture Overview
=====================

* Summarizing Diagram 
* Metadata Descriptions with NERDm
* Dynamic Landing Pages generated from metadata
* Ingest in terms of the OASIS model

Data Resource Descriptions with NERDm
-------------------------------------

A Data Asset Publication (DAP) exists in the PDR fundementally as a metadata description
(stored in the RMM's database).  The data model that the PDR uses internally to store and
transmit metadata descriptions is called NERDm: the *NIST Extensible Resource Data
Model*.  It is characterized by these key features:

* NERDm instances are encoded JSON-LD format [ref].
* The NERDm schema is defined by a suite of JSON Schema [ref] documents, including a
  core schema and multiple extension schemas.  The Schema documents include semantic
  definitions for all properties.  
* The schema documents comply with a specialized extension to JSON Schema (referred to
  as Extensible JSON Schema, or ejsonschema) that promotes flexible extension, evolution, 
  and validation, such that:

  * a NERDm instance document is allowed to include metadata from any compliant NERDm
    extension schema, regardless of whether all clients understand them.
  * clients interpreting a NERDm document must accept the syntax and symantics from the
    core NERDm schema, but they can choose which extensions they choose to interpret;
    unfamiliar extensions can be safely ignored.
  * clients requiring validation can choose which extension they are validating;
    unrecognized extensions can be ignored.

The NERDm data model is object-oriented based around types that have properties.  The key
base type is ``Resource`` (semantically equivalent to the DCAT ``Resource`` type [ref]).
A PDR DAP is represented as one of the ``Resource`` subtypes (e.g. ``DataPublication``,
``Software``, etc.).  A ``Resource`` can contain zero or more components, represented as a
subtype of the ``Component`` type.  The most common subtypes in the PDR are ``DataFile``
and ``Subcollection``; however, other subtypes can represent other components such as a
software repository, a database, or a service interface.

The PDR uses the NERDm description of a DAP to construct a human-readble, HTML
presentation of the publication.  It also serves as the basis for conversion into other
metadata models and formats (e.g. schema.org, DCAT, etc.).  NERDm descriptions are
available via the PDR APIs allowing automated clients to access information about a
publication, discover its components (such as files) and how to access them.  

In accordance with using the JSON-LD format, the NERDm data model adheres to the RDF data
model implied by JSON-LD:

* Each JSON object describes an instance of some type--the subject of an RDF assertion.

  * the object may include an ``@id`` property providing the subject's identifier
  * if the ``@id`` is not provided, an anonymous identifier is implied.

* Each JSON property within an object maps to a predicate in an assertion
* The property value provides object of the assertion

The core NERDm data model is based on the DCAT-US model [ref].  Most NERDm types and
properties map semantically from existing community ontologies; this mapping is encoded in
the JSON Schema documents that defines them.

The `oar-metadata GitHub repository <https://github.com/usnistgov/oar-metadata>`_ provides
support for handling NERDm data; in partiuclar,

* it contains the JSON Schema files for the core NERDm schema as well as the extensions
  used by the PDR (in the ``model`` directory),
* it provides a tool for validating NERDm records, and
* it provides a Python package, :py:mod:`nistoar.nerdm`, various helper capabilities for
  handling NERDm data, such as,

  * validating NERDm documents
  * analyzing and matching NERDm types
  * merging NERDm descriptions
  * converting NERDm descriptions into other models and formats

For more information, consult these references:

1. JSON-LD
2. JSON Schema
3. DCAT-US
4. NERDm
5. ejsonschema
6. Reader's Guide to NERDm Metadata

nistoar Software Overview
=========================

The `oar-pdr-py GitHub repository <https://github.com/usnistgov/oar-pdr-py>`_ provides an
extensive Python library for operating and accessing OAR systems.  It includes another
repository as a submodule:
`oar-metadata GitHub repository <https://github.com/usnistgov/oar-metadata>`_.  This
latter module contains some base libraries (including for working with NERDm metadata 
and identifiers) and can be used independently.  However, when you install the python
library from ``oar-pdr-py``, you will get all OAR subpackages.

The base Python package name is ``nistoar``.  Each OAR system is represented by a
subpackage, including:

* :py:mod:`~nistoar.midas` -- the implementation of the MIDAS backend service,
  responsible for the creation of DAPs, Data Management Plans (DMPs), and other digital
  assets.
* :py:mod:`~nistoar.pdr` -- the implementation of the key PDR services, including the
  publishing service, the preservation service, and the identifier resolver service.
* :py:mod:`~nistoar.nsd` -- an implementation of NIST Staff Directory Service broker,
  along with a client for accessing the service.
* :py:mod:`~nistoar.rmm` -- clients and services that interact with the Resource
  Metadata Manager (RMM), including metadata ingest services.  (See also related
  repositories below.)
* :py:mod:`~nistoar.distrib` -- clients for accessing the Data Distribution service
  that provides access to downloadable files.  (See also related repositories below.)

The base package also includes several infrastructure subpackages used across the OAR
systems:

* :py:mod:`~nistoar.base` -- provides some base classes and utilities, including support
  for the system configurations
* :py:mod:`~nistoar.nerdm` -- utilities for handling NERDm metadata
* :py:mod:`~nistoar.web` -- support for building consistent REST-based web services
* :py:mod:`~nistoar.jobmgt` --  a framework for launching asynchronous processing in
  separate processes.
* :py:mod:`~nistoar.id` -- utilities for handling and minting identifiers
* :py:mod:`~nistoar.doi` -- utilities for accessing DOI metadata
* :py:mod:`~nistoar.jq` -- a Python interface to the external tool `jq` [ref], used to
  convert JSON data from one schema to another.

Software implementing other OAR systems are contained in other related repositories.  Some
systems are implemented in languages other than Python.  These related repositories
include:

* `oar-rmm-python <https://github.com/usnistgov/oar-rmm-python>`_ -- a
  Python implementation of the RMM service.
* `oar-dist-service <https://github.com/usnistgov/oar-dist-service>`_
  -- a Java implementation of the Data Distribution service.
* `oar-sdp <https://github.com/usnistgov/oar-sdp>`_ -- the front-end browser application
  that presents the Science Data Portal, used for searching and browsing resources in the
  RMM and implemented using the `Angular (Typescript-based) framework <https://angular.io>`_.
* `oar-pdr-angular <https://github.com/usnistgov/oar-pdr-angular>`_ --
  implementations of various PDR and MIDAS front-end applications, including the DAPTool
  for creating DAPs, the Landing Page Service that creates HTML presentations of DAPs
  from NERDm descriptions, and front-end applications for handling restricted public
  data, implemented with Angular.
* `oar-dmp-angular-ui <https://github.com/usnistgov/oar-dmp-angular-ui>`_ -- an 
  Angular implementation of the DMPTool for creating data management plans
* `oar-midas-portal <https://github.com/usnistgov/oar-midas-portal-py>`_ -- an Angular
  implementation of the MIDAS portal application.

Finally, as described in further detail in the next section, the OAR system operates not
as a single application but rather as a suite of services.  How these services are
configured, deployed on hardware, and distributed across different machines are expected
to be highly site-specific.  At NIST (where the publishing side and the public side are
run in different environments), Docker is currently used to orchestrate deployment of
servers.  

.. _sec_designprinc:
    
Software Design Principles
==========================

* Largely Object-oriented
* Service Classes and Interface layers
* Software Configuration

If you are a developer who will be will be maintaining or adding to this libary (or you
otherwise want to learn about the code by reading it and this manual), you may find it
helpful to understand some of the key design principles that were employed in its
development.

The Library is Largely Object-oriented
--------------------------------------

As a means for promoting good software practices (e.g. good encapsulation, enforced
interfaces, separation of concerns, etc.), an object-oriented development approach has
been adopted: almost everything that happens happens inside a class instance.  A class can
isolate changes in state of some resource in the system it controls.  It also ensures a
consistent configuration of the way resource state is managed (see more on this below).

Service Classes and Interface Layers
------------------------------------

Most of the OAR systems capabilities can be accessed thourgh web services, but that is not
the only way.  Another way to access them is through command-line tools.  Regardless of
how a client (from outside the Python interpreter) accesses the capability, you want the
action to behave the same way.  Thus, web service and command-line implementations do
their work through a separate, common *service class* instance.

A *service class* implements the full business logic for some resource or a capability.
Its Python class interface defines everything that the service can do.  Here are some
examples of service classes:

* :py:class:`~nistoar.midas.dap.service.mds3.DAPService` -- a service that creates a
  draft digital asset publication (DAP) and allows it to be incrementally edited.

* :py:class:`~nistoar.midas.dap.fm.service.MIDASFileManagerService` -- a service that
  manages DAP client's file upload space in the File Manager.

* :py:class:`~nistoar.pdr.publish.service.pdp.PDPublishingService` -- a service that 
  accepts a draft DAP submissions (also called an *SIP*) and publishes 
  it into the PDR.

The implementations of these services make no assumptions about where calls to them are
coming from--be it a web service, command-line tool, or another Python function.

The service is made available to clients outside of Python through thinner *interface
layers*.  Most commonly, this will be a web service layer or a command-line layer.  For
example:

* :py:mod:`nistoar.midas.dap.cmd` -- various command-line interfaces to the
  :py:class:`~nistoar.midas.dap.service.mds3.DAPService`.

* :py:mod:`nistoar.midas.dap.fm.flask` -- the web interface (built on Flask) for the
  :py:class:`~nistoar.midas.dap.fm.service.MIDASFileManagerService`.

* :py:class:`~nistoar.pdr.publish.service.wsgi.pdp0.PDP0App` -- the web (WSGI) interface 
  to the :py:class:`~nistoar.pdr.publish.service.pdp.PDPublishingService`.

The logic in an interface layer typically should be restricted to parsing the inputs
provided by the client and routing them to the proper service function(s).  This
separation between the service class and the interface layers make it easier to add 
or replace interface layers over time.

Service Behavior is Configured with a Dictionary of Parameters
--------------------------------------------------------------

Classes in the ``nistoar`` library (particularly service and client classes) are *highly*
configurable.  That configuration is set at application start-up time and flows down to
individual class instances as dictionaries provided to constructors.  More specifically,
a class accepts configuration in the form of a Python ``dict`` passed to its constructor
where the keys are stings naming the configuration parameters.

Each class defines what parameter it will look for in the configuration dictionary;
indeed, the class should document each parameter as part of its in-line documentation.
A parameter can required or optional.  The values are typically of types supported by
JSON, including a dictionary value; this makes configuration data hierarchical.

A *parent* class instance--that is, one that contains instances of other classes, its
*children*--is responsible for providing the necessary configuration to the child class
constructors.  Often, a child's configuration is bundled into a single dictionary under a
single parameter name in its parent's configuration: the parent just sends that
"sub-dictionary" to the child.  This is helpful for preventing parameter name collisions
across classes; in particular, a class need not know or care what the global configuration
schema or structure is.

The section, :doc:`config`, describes the configuration framework in detail.  It also
spells out useful patterns for accessing and using configuration data.  

Web Service Layers are designed on the REST Model
-------------------------------------------------

We strive for a strictly REST design for web service endpoints.  This means:

* Endpoints represent a *resource* with state; a resource is modeled as a noun  (not a
  verb)

* Resources are organized hierarchically, where parent resources contain child
  resources, and this is reflected in its endpoint URLs: ``/body/arm`` is considered a
  part of ``/body``.  

* A collection resource contains individual member resources accessible by their IDs:

  * one can discover IDs by doing a search on a collection resource (``/collection``)

  * appending an ID to the collection resource endpoint accesses that member resource
    (``/collection/mem:1234``)

* Actions are governed by the HTTP method:

  * GET retrieves a representation of the resource.  Query parameters can control what
    kind of representation to return (as well as an ``Accept`` header as part of content
    negotiation).  A GET does not change the state of the resource.  
  * POST on a collection endpoint will create a new member of the collection.
  * PUT on a member endpoint will replace the member's current data with the data
    provided (although the service is free to mediate and filter that data).
  * DELETE removes access to a member resource.

* Clients use the HTTP response status codes to determine success or failure of their
  request.  In case of failure, the body *may* contain more information of what went
  wrong than the status and is associated status text can convey.  

* Conceptually, some resources can disappear as soon as they are created (with a POST).
  This is one way to implement a verb-like action; think of it as creating a "job" (a
  noun) that disappears after its work is done.

The section, :doc:`websvc`, describes how web frameworks are assembled.



    
