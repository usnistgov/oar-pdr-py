"""
NSDi -- a service module that provides client-side indexes of staff directory (NSD) search results.

A common desired feature of front-end GUI forms is as-you-type suggestions.  This is often employed,
for example, when the user is entering a person's name: as they type, the GUI can provide suggested 
completions of names, and that list of suggestions can be refined as the user types more characters.
Obviously, the lookup service that provides the suggestions must be fast enough to complete close to 
the time it takes the user to type an additional character.  When the lookup service is a remote web
service, this performance may be a challenge.

This indexing service provides a performant lookup mechanism for front-end clients.  It sits in front 
of another search service that it creates indexes into and returns to the client.  Specifically, the
client sends to this indexing service only an initial query using just the first few (e.g. one or two) 
characters typed by the user (referred to as a _prompt_).  The index service uses the prompt to query 
the search service and turns the results into an index--a fast lookup dictionary of all records 
matching the prompt which the client can use to create additional suggestions on the client side.  A 
lookup into that index returns a list of matching identifier and a display string pairs.  The client 
shows the display strings as suggestions to the user; when the user selects one of the suggestions, 
the client then sends a by-identifier query to search service to get back the full record corresponding 
to the selected suggestion.

This module provides the indexing service and its REST web service interfaces.  It leverages the 
index-generating capabilities provided by :py:mod:`nistoar.midas.dbio.index`.  

For the details on the service endpoints, see the convention/version-specific documentation:
  *  :py:mod:`nistoar.midas.nsdi.wsgi.v1
"""
