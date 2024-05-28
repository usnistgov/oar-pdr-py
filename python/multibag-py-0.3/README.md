# multibag-py
This python library provides a reference implementation for the
Multibag BagIt Profile.

## About BagIt

The Multibag standard is a profile on the [BagIt
standard](https://tools.ietf.org/html/draft-kunze-bagit).  BagIt
defines a way to package data, metadata, and related files into a
single coherent package.  Such a package is fundementally represented
as a directory with a constrained structure.  When the directory is
serialized into a file (e.g. with zip), it becomes an appropriate way
to transmit the complete package.

## About the Multibag Profile

The Multibag BagIt Profile defines a means for splitting a data
aggregation across multiple bags which we refer to as the _Multibag_
profile.  One key motivation for splitting an aggregation over several
bags is to make it easier to handle very large aggregations in storage
or transmission environments that would otherwise place a limit on the
size of a bag.  The BagIt specification already supports this basic
functionality; this profile expands on this functionality to
accomplish the following goals: 
   * provide a standard recipe for combining all bags representing a
     single logical aggregation into a single compliant bag. 
   * support non-destructive updates to a bag aggregation by allowing
     one to create a new aggregation that combines an existing set of
     bag aggregations with an "errata" or "update" bag that contains
     only the files that have changed. 

The draft specification for this Profile can be found in the [docs
directory](docs).


## About this Reference Implementation

This repo provides a python reference implementation of the multibag
profile.

_This implementation is in progress.  Further information on how to
use the library is forthcoming._

### Package Prerequisites

This package is dependent on the following Python packages:
   * bagit (> 1.6.0)
   * fs  (> 2.0.0)

These can be installed using the `pip` tool:

```
  pip install -r requirements.txt
```

### Building or Installing the Package

To build this package, use the included `setup.py` script:

```
  python setup.py build
```

This installs the compiled Python code into a subdirectory called
`build`.  To install the package, use the `install` command:

```
  python setup.py install
```

Use the `--prefix` option to install the package in a specific
location (see `setuptools` documentation for details).

### Running the Unit Tests

The unit tests can be executed via the included `setup.py` script:

```
  python setup.py test
```

## Acknowlegements

This project gratefully acknowledges the Library of Congress and the
developers who contributed to the
[bagit-python package](https://github.com/LibraryOfCongress/bagit-python),
made available into the public domain.  The multibag-py code borrows
code from this library to extend its capabilities to reading
serialized bags (via the `fs` package).  
