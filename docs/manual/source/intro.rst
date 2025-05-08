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
publications*.  

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
repository.

Architecture Overview
=====================

* Summarizing Diagram 
* Metadata Descriptions with NERDm
* Dynamic Landing Pages generated from metadata
* Ingest in terms of the OASIS model


