"""
Interfaces and implementations to NERDm metadata storage.  

The drafting service collects bits of NERDm metadata that describes the SIP which it must 
persist as part of the draft SIP.  This package defines an abstract interface for pulling 
the metadata from storage and into memory, enabling new metadata to be merge in, and storing 
the result.  
"""
