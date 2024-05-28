import os, sys, subprocess, unittest
from setuptools import setup

setup(name='multibag',
      version='0.2',
      description="multibag: a Python-based reference implementation of the Multibag BagIt profile",
      author="Ray Plante",
      author_email="raymond.plante@nist.gov",
      url='https://github.com/usnistgov/multibag-py',
      scripts=[ ],
      packages=['multibag', 'multibag.access', 'multibag.validate',
                'multibag.testing'],
      test_suite="tests.suite",
      test_runner="unittest:TextTestRunner"
)
