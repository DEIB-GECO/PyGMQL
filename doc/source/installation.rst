Installation
============

-------------
Prerequisites
-------------

Here we list the requirements of this library from the point of view of the Python
versions that are supported and the external programs needed in order to use it.

++++
Java
++++

In order to use the library you need to have Java installed in your system. And, in particular,
the environment variable JAVA_HOME must be setted to your current Java installation.

If JAVA_HOME is not setted an error will be thrown at the first import of the library.
In that case the following steps must be perfomed:

1. Install the latest version of Java (follows `this link <https://www.java.com/it/download/>`_)
2. Set JAVA_HOME. This can be done differently depending on the your OS:

   1. Linux:

    .. code-block:: none

       echo export "JAVA_HOME=/path/to/java" >> ~/.bash_profile
       source ~/.bash_profile


   2. Mac:

    .. code-block:: none

       echo export "JAVA_HOME=\$(/usr/libexec/java_home)" >> ~/.bash_profile
       source ~/.bash_profile

   3. Windows:

      1. Right click My Computer and select Properties
      2. On the Advanced tab, select Environment Variables, and then edit JAVA_HOME to point to where the JDK software is located,
         for example, C:\\Program Files\\Java\\jdk1.6.0_02

++++++
Python
++++++
Currently PyGMQL supports only Python 3.5, 3.6 and 3.7.


---------------------------
Using the github repository
---------------------------
You can install this library by downloading its source code from the github repository::

    git clone https://github.com/DEIB-GECO/PyGMQL.git

and then using::

    cd PyGMQL/
    pip install -e .

This will install the library and its dependencies in your system.

---------
Using PIP
---------
The package can be also downloaded and installed directly in your python distribution using::

    pip install gmql


---------
Installation of the backend
---------
PyGMQL computational engine is written in Scala. The backend comes as a JAR file which will be downloaded at the first
usage of the library::

    import gmql

