"""
support for interacting with the remote file manager application, including via a client library

This module includes the following components:

:py:mod:`service`
    An implementation of the so-called MIDAS Application Layer of the file manager
:py:mod:`flask`
    A Flask-implementation that exposes the Application Layer as a web service (alt to wsgi)
:py:mod:`wsgi`
    The OAR web service wrapper for the Application Layer (alt to flask)
:py:mod:`scan`
    Implememtations of the scanning capabilities of the Application Layer
:py:mod:`clients`
    Client interfaces to the different file manager APIs, including the Generic Layer, the 
    WebDAV interface, and the Application Layer.  

File Manager Architecture
=========================

The MIDAS File Manager is currently built on top of a generic Nextcloud instance which 
provide a browser-based GUI interface to file storage where users can upload files, browse 
and organize files into hierarchical folders, and download files.  Nextcloud also natively 
supports a WebDAV interface to accessing the files in its storage.  MIDAS provides users 
with access to space in the File Manager so that they can upload and organize files that 
will be part of Digital Asset Publication (DAP).  

To facilitate use of Nextcloud as a File Manager for MIDAS, two additional web APIs have been 
added.  First is the Generic Layer that provides some general purpose capabilities not 
necessarily specific to MIDAS:
  * registering users
  * registering files that have been put into storage outside of normal Nextcloud interfaces
  * updating permissions on files

Second is the MIDAS Application-specific Layer; as its name suggests, this provides 
capabilities that are specific to the needs of MIDAS.  This includes:
  * creating and setting up storage space for users to upload files
  * scanning the uploaded files to extract metadata to go into the DAP's NERDm metadata


MIDAS Application Layer
-----------------------

The Application-specific Layer is implemented in :py:mod:`service` module and its 
capabilities are exposed to the web via the _[flask/wsgi]_ module.  Its design is based on 
the assumption that it has direct access to users' storage spaces via the filesystem.  This 
means it can read the files via the filesystem so that it can extract metadata most efficiently.  
An optional, additional assumption might be that it can also run Nextcloud command-line tools 
as well (see the :py:mod:`scan` module for cases where this is leveraged).  This generally 
implies that the Application Layer service runs on the same platform (or close to it) that 
is running the Nextcloud instance.  
"""
# from .apiclient import *
