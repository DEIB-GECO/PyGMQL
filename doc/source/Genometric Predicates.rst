=====================
Genometric predicates
=====================

.. py:currentmodule:: gmql.dataset.DataStructures.GenometricPredicates

.. autoclass:: MD
   :members:

.. autoclass:: DLE
   :members:

.. autoclass:: DGE
   :members:

An additional clause that can be specified in a genometric predicate is UP/DOWN, called
the upstream/downstream clause, which refers to the upstream and downstream directions
of the genome. This clause requires that the rest of the predicate hold only on the
upstream (downstream) genome with respect to the anchor region. More specifically:

   * In the positive strand (or when the strand is unknown), UP is true for those regions of
     the experiment whose right-end is lower than, the left-end of the anchor, and DOWN is
     true for those regions of the experiment whose left-end is higher than the right-end
     of the anchor
   * In the negative strand disequations are exchanged
   * Remaining regions of the experiment must be overlapping with the anchor region

.. autoclass:: UP
   :members:

.. autoclass:: DOWN
   :members: