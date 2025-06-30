"""
module supporting a command-line interface to PDR operations.  It includes a common base infrastructure for 
assembling a nestable command script, as well as high-level implementations that cut across nistoar.pdr 
modules.

EXIT STATUS

It is recommended that commands built into this cli infrastructure follow the following conventions for 
exit status codes:

  0 - normal successful completion
  1 - the item or resource requested by the input values could not be found or recognized
  2 - an error was found in the option or argument values, preventing proper parsing or interpretation
  3 - syntax or other read error while reading provided input data
  4 - error occured while writing output data
  5 - an unexpected remote system error occured
  6 - if a configuration error was detected
  7 - a bad service URL was provided
  8 - other unexpected but caught exception was raised

Higher exit values can be used for meanings more specific to the command invoked.  The value 200 is 
raise if any unexpected, uncaught exception bubbles to the top of the execution stack.  
"""

