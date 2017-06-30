The Genomic data model
======================

As we have said, PyGMQL is a Python interface to the GMQL system.
In order to understand how the library works, a little insights on
the data model used by GMQL is necessary.

GMQL is based on a representation of the genomic information known as GDM - Genomic
Data Model. Datasets are composed of samples , which in turn contains two kinds of data:

    * **Region values** (or simply **regions**), aligned w.r.t. a given reference, with specific
      left-right ends within a chromosome. Regions can store different information regarding the
      “spot” they mark in a particular
      sample, such as region length or statistical significance. Regions of the model
      describe processed data, e.g. mutations, expression or bindings; they have a
      **schema** , with 5 common attributes ( id , chr , left , right , strand ) including the id of the
      region and the region coordinates, along the aligned reference genome, and then
      arbitrary typed attributes. This provides interoperability across a plethora of genomic
      data formats

    * **Metadata**, storing all the knowledge about the particular sample, are arbitrary
      attribute-value pairs, independent from any standardization attempt; they trace the
      data provenance, including biological and clinical aspects
