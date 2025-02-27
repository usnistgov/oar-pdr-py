"""
A subpackage for scanning user files to extract metadata.  

A key feature of the MIDAS Application Layer of the File Manager is the ability to asynchronously 
scan files and extract metadata.  This can be as simple as file sizes and checksum hashes or more 
complex to include format detection and format-specific metadata extraction.  
"""
from .base import *
