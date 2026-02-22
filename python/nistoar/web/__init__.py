"""
Utilities for creating web servers and clients.

This package is organized into the following modules:

``utils``
    General utilities that can be used potentially in any web service framework.  This includes
    functions for interpreting the ``Accept`` HTTP header.
``formats``
    Classes that help a web service implementation manage its output format options
``exceptions``
    Exceptions representing common failure conditions while using a web client
``webrecord``
    a facility for recording HTTP requests to a file (e.g. for debugging purposes), assuming 
    access to the request via the WSGI environment.
``rest``
    a simple framework for creating strict REST services
"""
