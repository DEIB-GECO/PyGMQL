.. PyGMQL documentation master file, created by
   sphinx-quickstart on Wed May 17 17:39:53 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to PyGMQL's documentation!
==================================
PyGMQL is a python module that enables the user to perform operation on genomic data in a scalable way.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   introduction
   genomic_data_model
   GMQLDataset
   GDataframe
   remote
   settings
   ml

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Indices and tables
==================

------------------
Dataset structures
------------------

.. currentmodule:: gmql.dataset
.. autosummary::
   :nosignatures:

   GMQLDataset.GMQLDataset
   GDataframe.GDataframe

-------------------------
Dataset loading functions
-------------------------

.. currentmodule:: gmql.dataset.loaders.Loader
.. autosummary::
   :nosignatures:

   load_from_path
   load_from_remote

-------
Parsing
-------

.. currentmodule:: gmql.dataset.parsers
.. autosummary::
   :nosignatures:

   BedParser.BedParser


--------------------
Aggregates operators
--------------------

.. currentmodule:: gmql.dataset.DataStructures.Aggregates
.. autosummary::

   COUNT
   SUM
   MIN
   MAX
   AVG
   BAG
   STD
   MEDIAN
   Q1
   Q2
   Q3

---------------------
Genometric predicates
---------------------

.. currentmodule:: gmql.dataset.DataStructures.GenometricPredicates
.. autosummary::

   MD
   DLE
   DGE
   UP
   DOWN