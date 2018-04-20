===============================
Spark and system configurations
===============================

The configuration of the Java properties and the Spark environment can be done by getting the singleton instance of the
configuration class as follows::

    conf = gl.get_configuration()

Follows the description of this object:

..  autoclass:: gmql.configuration.Configuration
    :members: