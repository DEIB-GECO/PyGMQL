====================
Building expressions
====================

.. currentmodule:: gmql.dataset.GMQLDataset.GMQLDataset

When doing a selection (using :meth:`meta_select`, :meth:`reg_select`)
or a projection (using :meth:`meta_project`, :meth:`reg_project`)
you are required to specify an expression on the metadata or region fields.

An expression can therefore use metadata attributes or region fields.
Given a GMQLDataset :code:`dataset`, one can access its **region fields** by typing::

    dataset.field1
    dataset.field2
    dataset.chr
    dataset.start
    ...

and one can access its **metadata attributes** by typing::

    dataset['metadata_attribute_1']
    dataset['metadata_attribute_2']
    dataset['metadata_attribute_3']
    ...

The expressions in PyGMQL can be of two types:

    * *Predicate*: a logical condition that enables to select a portion of the dataset.
      This expression is used in selection. Some example of predicates follow::

          # region predicate
          (dataset.chr == 'chr1' || dataset.pValue < 0.9)
          # region predicate with access to metadata attributes
          dataset.score > dataset['size']

      It is possible, based on the function that requires a predicate, to mix region fields and
      metadata attributes in a region condition. Of course it is not possible to mix metadata and
      region conditions in a metadata selection (this is due to the fact that to each metadata
      attribute can be associated multiple values for each region field).

    * *Extension*: a mathematical expression describing how to build new metadata
      or region fields based on the existent ones. Some examples of expression follow::

          # region expression
          dataset.start + dataset.stop
          dataset.p_value / dataset.q_value
          # metadata expression
          dataset['size'] * 8.9
          dataset['score'] / dataset['size']

      It is possible to mix region fields and metadata attributes in region extensions::

          # region expression using metadata attributes
          (dataset.pvalue / 2) + dataset['metadata'] + 1
