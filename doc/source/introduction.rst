============
Introduction
============

In this brief tutorial we will explain the typical workflow of the user of PyGMQL.
You can use this library both interactively and programmatically. **We strongly suggest to use it
inside a Jupyter notebook for the best graphical render and data exploration.**

---------------------
Importing the library
---------------------
To import the library simply type::

    import gmql as gl

---------------
Loading of data
---------------

The first thing we want to do with PyGMQL is to load our data. You can do that by calling the
:meth:`gmql.load_from_path`.

