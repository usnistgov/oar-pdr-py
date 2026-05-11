"""
a module that defines and implements the framework for preserving data submitted to the PDR through
the concept of a _preservation task_.

In this :py:mod:`framework`, a preservation task accepts a Submission Information Package (SIP), 
converts it into an Archive Information Package (AIP).  It then delivers to that AIP to long-term
storage and submits needed AIP artifacts (e.g. metadata, data files) into the public PDR system for 
public access.  

.. seealso:: the :py:mod:`framework module<framework>` for a detailed description of the framework 
design.
"""
