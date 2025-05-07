.. nistoar documentation master file, created by
   sphinx-quickstart on Wed May  7 09:50:31 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

#####################################################################################
nistoar: The Python Library for the NIST Public Data Repository and Publishing System
#####################################################################################

The NIST Public Data Repository (PDR) is a platform for presenting and distributing
digital (non-literature) research products--data, software, models, etc.--as citable
publications.  It is a component of the broader program at NIST known as Open Access to
Research (OAR).

This manual describes ``nistoar``, the Python library and system that powers the PDR's
backend systems, especially the systems used for publishing into the PDR.  In this manual,
we introduce the primary functions of the PDR and its overall architecture.  We then
describe the subcomponents and services that make up the PDR and how they are designed to
deliver the functionality.  Links are provided to software API documentation.  Not
included in this document are repository operations.

The ``nistoar`` packages are provided primarily via the
`oar-pdr-py GitHub repository <https://github.com/usnistgov>`_.

"""""""""""""""""
Table of Contents
"""""""""""""""""

.. note::
   This is a placeholder TOC for editing purposes.  It will be replaced by the 
   dynamically-generated list below.

* Architecture Overview
* Data Publication System
  
  * Overview
  * MIDAS: Services for Managing Digital Assets
    
    * DMPs and DAPs: an Overview
    * The MIDAS-DBIO Service

      * Creating Data Management Plans (DMPs)
      * Creating Digital Asset Publications (DAPs)
      
    * The Staff Directory Service
    * The DBIO Client Notification Service
      
  * The Publication Service
  * The Preservation Service
    
* Public PDR Services

  * The Identifier Resolver Service
  * Client Tools

* The NIST Extensible Resource Data Model (NERDm)
  
  * Overview
  * Python tools
    
* The Python API Reference
* Web Service API References

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api

