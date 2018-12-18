.. PyGMQL documentation master file, created by
   sphinx-quickstart on Wed May 17 17:39:53 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to PyGMQL's documentation!
==================================
PyGMQL is a python module that enables the user to perform operation on genomic data in a scalable way.

This library is part of the bigger project `GMQL <http://www.bioinformatics.deib.polimi.it/genomic_computing/>`_
which aims at designing and developing a genomic data management and analysis software on top of
big data engines for helping biologists, researchers and data scientists.

GMQL is a declarative language with a SQL-like syntax. PyGMQL translates this paradigm to the
interactive and script-oriented world of python, enabling the integration of genomic data with
classical Python packages for machine learning and data science.

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   installation
   introduction
   genomic_data_model
   GMQL
   GMQLDataset
   expressions
   GDataframe
   remote
   settings
   configurations

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Tutorials
==================

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   tutorial_chapter0
   tutorial_chapter1
   tutorial_chapter2

Data structures and functions
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
For the list of the available parsers go to:

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   parsers


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
   DL
   DGE
   DG
   UP
   DOWN

----------------------
Mathematical operators
----------------------

.. currentmodule:: gmql.dataset.DataStructures.ExpressionNodes
.. autosummary::

   SQRT