.. _ch_logging:

*********************************
Building and Running Web Services
*********************************

As described in the package :ref:`design principles <sec_designprinc>`, a ``nistoar``
web service implementation should be a logically-thin layer on top of a service class.
Its job should be about collecting and parsing inputs and routing them to calls on a
service class instance.  All knowledge about the web layer interface are kept in the web
layer (and its clients), allowing the service class to be agnostic about what kind of
client is accessing it.

This separation of concerns allows us to evolve our use of web frameworks over time.
Indeed, the ``nistoar`` package is not locked into using any single web framework.
Regardless of which framework is used, it is important to strive for a
:ref:`strictly REST interface design <sec_designprinc>`.  

Background
==========

The Native ``nistoar`` WSGI Framework
=====================================

Third-party Web Service Frameworks
==================================

Using Flask
-----------



.. note::
   More content is planned for this section

