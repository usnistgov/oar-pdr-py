"""
A module for reviewing the completeness of a draft NERDm record.  

This module is built on the validation framework defined in :py:mod:`nistoar.pdr.utils.validate`,
and is intended to feed suggestions and prompts to a DAPTool user about next steps for preparing 
a DAP draft for submission.  To enable integration with the DAPTool front end, the following conventions
are implemented in :py:class:`~nistoar.pdr.utils.validate.ValidationIssue` instances:
  * the test ``label`` value ends with the pattern, ``#``_prop_, where _prop_ indicates the NERDm 
    property that requires some attention.  This allows the DAPTool to direct user to the corresponding
    widget for making the needed change.
  * the first value in the ``comments`` array is a user-oriented suggestion about what the user should
    do. (In contrast, the ``specification`` value is expected to be a more pendantic statement.)
"""
