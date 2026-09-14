"""
An ingest subpackage that handles submission of metadata to DataCite to create or update DOIs
"""
from .client import DOIMintingClient
from nistoar.doi import DOIClientException
