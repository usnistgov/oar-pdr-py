"""
Web Service interface implementations to a staff directory.  This module offers two takes on the service:
  * :py:mod:`~nistoar.nsd.wsgi.nsd1` -- a service intended to be compatible with the 
    NIST Staff Directory (NSD) Service.  Its purpose is to facilitate testing of NSD clients.
  * :py:mod:`~nistoar.nsd.wsgi.oar1` -- a service designed to be optimized for use with OAR 
    applications.  As it is intended to be populated from the NSD, it uses the NSD data schema for
    records; however, the RESTful interface is slightly different and leverages 
    :py:mod:`downloadable indexes <nistoar.midas.dbio.index>`
    for fast lookups.
"""
