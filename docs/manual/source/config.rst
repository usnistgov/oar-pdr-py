.. _ch_config:


Configuring Classes and Applications
====================================

Configuration data are named values that are set at run-time that can control the behavior
of the application at a fine-grained level.  A single configuration file or multiple ones
can be provided to a ``nistoar`` application when it starts up; however, parts of the
configuration will flow down to individual classes instances as Python dictionaries
provided to their constructors.

This chapter looks at the ``nistoar`` approach to configuration in two parts.  The first
is about configuring individual classes; this is what most ``nistoar`` developers have to
contend with.  The second part looks at how configuration data gets into the application.

Configuring a Class Instance
----------------------------

The Python ``dict`` (or any ``dict``-like class) is the primary vehicle for passing
configuration data around an application.  Each key of dictionary is a string that names
a particular parameter.  The value can, in principle, be anything and of any type;
however, becuse configuration data is provided to an application in a serialized form,
like a YAML file, the values are usually the primitive types supported by YAML or JSON.  
A class instance is configured by passing a Python dictionary to the class's constructor.  

A class defines what parameters it will look for in the configuration dictionary.  In
particular, a class should document (as part of its in-line) documentation the name of
each parameter, its allowed value type(s), what it represents, and whether it is
required or optional.  If it is optional, it should state what the default value
(or behavior) will be.
See :py:class:`~nistoar.midas.dap.fm.service.MIDASFileManagerService` as an example.

Configuration dictionaries can be heirarchical; that is, the expected value may be
another dictionary with its own sub-parameters.

A class that takes a configuration dictionary is responsible for configuring the
instances of classes it creates within its implementation.  The parent class instance
(in the sense that it *contains* other child instances of classes) has full control 
over what parameters it provides to its children and how it forms the configuration
dictionary that it passes to them; however, a common and convenient pattern is to have 
a child's configuration bundled into a sub-dictionary in the parent's configuration.  
For example, you may see this documented in a parent class like this:

``webdav``
    (dict) *optional*.  the data for configuring the client for the file manager's
    WebDAV API.  (See :py:class:`~nistoar.midas.dap.fm.clients.webdav.FMWebDAVClient`
    for the supported sub-parameters.)  If not provided, defaults will be assembled 
    from the configuration given to this class's constructor.

(See also the code example below.)

.. _ssec_patterns: 

Patterns of Use
^^^^^^^^^^^^^^^

As a class developer, you'll that a dictionary is a very convenient and flexible way to
deal with a variety of parameters of different type, some required and some optional, and
some being pushed to other constructors.  Support for new parameters can be added easily
over time with minimal impact on code and interaces.

Here are some key patterns for handling configuration data within a class:

* If any parameters that are considered required, the constructor should check for
  their existence.  If a required parameter does not exist or the value is invalid,
  the constructor can raise a :py:class:`nistoar.base.config.ConfigurationException`:

  .. code-block:: python

     if not config.get('endpoint_url'):     # works if not set or empty (or empty string)
         raise ConfigurationException("MyClass: missing required config parameter: endpoint_url")

* Optional parameters can be accessed when they are needed specifying the default, if
  appropriate:

  .. code-block:: python

     verify = False
     if self.config.get('ca_bundle'):
         verify = self.config['ca_bundle']

     path = self.config.get("resolver_path", "/od/id")
      
* Configuring a child class instance is straight-forward if its parameters are bundled
  into a sub-dictionary:

  .. code-block:: python

     self.child = ChildService(self.config.get("child_service", {})

  Note how the ``ChildService`` constructor has no idea that its configuration was
  called ``child_service``; all it knows is that it was handed a dictionary that it
  expects to find certain key names in.  *This is a key feature*: a class does not
  have to have the global understanding of the configuration schema, just the part it
  cares about.  Nor do names need to be globally unique to avoid collisions with other
  classes: different instances of the same class can easily have different
  configurations.

* The pattern for configuring a child class shown above is the simplest approach; however,
  a parent class is free to construct or manipulate the configuration however it likes
  before passing it to the child's constructor.  It can add or remove parameters, or merge
  in dictionaries from other parts of its own configuration.  Typically, configuration
  dictionaries are handled in a read-only mode; however, if a class wants to modify the
  configuration it received via its constructor, it's recommend that it makes a copy of it
  first using the standard Python function ``copy.deepcopy``.  This preserves the copy of
  the configuration data still held by the class's own parent.

The :py:mod:`nistoar.base.config` module provides utilities for dealing with
configuration data.  Most of its capabilities are used primarily at the application
level for loading data and set up logging (discussed in the next section).  However, these
features may be useful at the class level (see examples above):

* :py:class:`~nistoar.base.config.ConfigurationException` -- for complaining about
  mising or ill-valued parameters
* :py:func:`~nistoar.base.config.merge_config` -- a function for merging two
  configuration dictionaries, where one can be considered defaults for the other.
* :py:func:`hget() <nistoar.base.config.hget_jq>` -- a function for safely extracting 
  a parameter deep within configuration hierarchy.

Loading Configuration Data into an Application
----------------------------------------------

The :py:mod:`nistoar.base.config` module supports several ways of loading configuration
data into an application:

* **From a configuration web service:**  the application requests its configuration data
  by an application name from a web service.
* **From a file on disk:**  the application reads a file containing all the configuration
  data, typically in YAML or JSON format.
* **From an arbitrary URL:** the application resolves the URL which should return a file
  in YAML or JSON format.

Typically, an application will support all three; which one actually gets used can depend
on environment variables or command-line inputs.  

...

