Threading
---------

Instances of the following classes are considered *stateful*, and should not be
shared across multiple threads without locks in place:

* :ref:`BatchFabric` (see :doc:`batch`)
* :ref:`BatchJob` (see :doc:`batch`)
* :ref:`Cursor` (see :doc:`cursor`)

The rest of pyC8 is safe to use in multi-threaded environments.
