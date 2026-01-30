# The Developers' Guide

This directory contains the source for the the nistoar library Developers' Guide.  It
includes software overviews, how-tos, and the reference API built from in-line
documentation.  It is built using the Sphinx documentation markup system.  

## Note

As of this writing, this document is very much a work-in-progress.  It is missing sections,
and the in-line documentation forming the reference API contains many formatting errors.
See [`source/index.rst`](source/index.rst) for an outline of the contents planned for this
document. 

## Building the Guide

Sphinx should be installed into your environment; version 8.1.3 or higher is recommended.
To build the documentation, be sure that your `PYTHONPATH` includes access to the full
`nistoar` package.  Build the HTML version of the documentation by typing:

```bash
make html
```

Building other formats may require additional dependencies to be installed.  Consult the
[Sphinx documentation](https://www.sphinx-doc.org/) for more information.


