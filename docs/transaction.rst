Transactions
------------

pyC8 supports **transactions**, where requests to C8 Data Fabric server are
placed in client-side in-memory queue, and committed as a single, logical unit
of work (ACID compliant). After a successful commit, results can be retrieved
from :ref:`TransactionJob` objects.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "test" fabric as tenant admin.
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the API wrapper for "students" collection.
    students = fabric.collection('students')

    # Begin a transaction via context manager. This returns an instance of
    # TransactionFabric, a fabric-level API wrapper tailored specifically
    # for executing transactions. The transaction is automatically committed
    # when exiting the context. The TransactionFabric wrapper cannot be
    # reused after commit and may be discarded after.
    with fabric.begin_transaction() as txn_fabric:

        # Child wrappers are also tailored for transactions.
        txn_col = txn_fabric.collection('students')

        # API execution context is always set to "transaction".
        assert txn_fabric.context == 'transaction'
        assert txn_col.context == 'transaction'

        # TransactionJob objects are returned instead of results.
        job1 = txn_col.insert({'_key': 'Abby'})
        job2 = txn_col.insert({'_key': 'John'})
        job3 = txn_col.insert({'_key': 'Mary'})

    # Upon exiting context, transaction is automatically committed.
    assert 'Abby' in students
    assert 'John' in students
    assert 'Mary' in students

    # Retrieve the status of each transaction job.
    for job in txn_fabric.queued_jobs():
        # Status is set to either "pending" (transaction is not committed yet
        # and result is not available) or "done" (transaction is committed and
        # result is available).
        assert job.status() in {'pending', 'done'}

    # Retrieve the job results.
    metadata = job1.result()
    assert metadata['_id'] == 'students/Abby'

    metadata = job2.result()
    assert metadata['_id'] == 'students/John'

    metadata = job3.result()
    assert metadata['_id'] == 'students/Mary'

    # Transactions can be initiated without using a context manager.
    # If return_result parameter is set to False, no jobs are returned.
    txn_fabric = fabric.begin_transaction(return_result=False)
    txn_fabric.collection('students').insert({'_key': 'Jake'})
    txn_fabric.collection('students').insert({'_key': 'Jill'})

    # The commit must be called explicitly.
    txn_fabric.commit()
    assert 'Jake' in students
    assert 'Jill' in students

.. note::
    * Be mindful of client-side memory capacity when issuing a large number of
      requests in a single transaction.
    * :ref:`TransactionFabric` and :ref:`TransactionJob` instances are
      stateful objects, and should not be shared across multiple threads.
    * :ref:`TransactionFabric` instance cannot be reused after commit.

See :ref:`TransactionFabric` and :ref:`TransactionJob` for API specification.

Error Handling
==============

Unlike :doc:`batch <batch>` or :doc:`async <async>` execution, job-specific
error handling is not possible for transactions. As soon as a job fails, the
entire transaction is halted, all previous successful jobs are rolled back,
and :class:`c8.exceptions.TransactionExecuteError` is raised. The exception
describes the first failed job, and all :ref:`TransactionJob` objects are left
at "pending" status (they may be discarded).

**Example:**

.. testcode::

    from c8 import C8Client, TransactionExecuteError

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "test" fabric as tenant admin.
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the API wrapper for "students" collection.
    students = fabric.collection('students')

    # Begin a new transaction.
    txn_fabric = fabric.begin_transaction()
    txn_col = txn_fabric.collection('students')

    job1 = txn_col.insert({'_key': 'Karl'})  # Is going to be rolled back.
    job2 = txn_col.insert({'_key': 'Karl'})  # Fails due to duplicate key.
    job3 = txn_col.insert({'_key': 'Josh'})  # Never executed on the server.

    try:
        txn_fabric.commit()
    except TransactionExecuteError as err:
        assert err.http_code == 409
        assert err.error_code == 1210
        assert err.message.endswith('conflicting key: Karl')

    # All operations in the transaction are rolled back.
    assert 'Karl' not in students
    assert 'Josh' not in students

    # All transaction jobs are left at "pending "status and may be discarded.
    for job in txn_fabric.queued_jobs():
        assert job.status() == 'pending'

Restrictions
============

This section covers important restrictions that you must keep in mind before
choosing to use transactions.

:ref:`TransactionJob` results are available only *after* commit, and are not
accessible during execution. If you need to implement a logic which depends on
intermediate, in-transaction values, you can instead call the method
:func:`c8.fabric.Fabric.execute_transaction` which takes raw Javascript
command as its argument.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "test" fabric as tenant admin.
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the API wrapper for "students" collection.
    students = fabric.collection('students')

    # Execute transaction in raw Javascript.
    result = fabric.execute_transaction(
        command='''
        function () {{
            var fabric = require('internal').fabric;
            fabric.students.save(params.student1);
            if (fabric.students.count() > 1) {
                fabric.students.save(params.student2);
            } else {
                fabric.students.save(params.student3);
            }
            return true;
        }}
        ''',
        params={
            'student1': {'_key': 'Lucy'},
            'student2': {'_key': 'Greg'},
            'student3': {'_key': 'Dona'}
        },
        read='students',  # Specify the collections read.
        write='students'  # Specify the collections written.
    )
    assert result is True
    assert 'Lucy' in students
    assert 'Greg' in students
    assert 'Dona' not in students

Note that in above example, :func:`c8.fabric.Fabric.execute_transaction`
requires names of *read* and *write* collections as pyC8 has no way of
reliably figuring out which collections are used. This is also the case when
executing C8QL queries.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "test" fabric as tenant admin.
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='passwd')

    # Begin a new transaction via context manager.
    with fabric.begin_transaction() as txn_fabric:
        job = txn_fabric.c8ql.execute(
            'INSERT {_key: "Judy", age: @age} IN students RETURN true',
            bind_vars={'age': 19},
            # You must specify the "read" and "write" collections.
            read_collections=[],
            write_collections=['students']
        )
    cursor = job.result()
    assert cursor.next() is True
    assert fabric.collection('students').get('Judy')['age'] == 19

Due to limitations of C8 Data Fabric's REST API, only the following methods are
supported in transactions:

* :func:`c8.c8ql.C8QL.execute`
* :func:`c8.collection.StandardCollection.get`
* :func:`c8.collection.StandardCollection.get_many`
* :func:`c8.collection.StandardCollection.insert`
* :func:`c8.collection.StandardCollection.insert_many`
* :func:`c8.collection.StandardCollection.update`
* :func:`c8.collection.StandardCollection.update_many`
* :func:`c8.collection.StandardCollection.update_match`
* :func:`c8.collection.StandardCollection.replace`
* :func:`c8.collection.StandardCollection.replace_many`
* :func:`c8.collection.StandardCollection.replace_match`
* :func:`c8.collection.StandardCollection.delete`
* :func:`c8.collection.StandardCollection.delete_many`
* :func:`c8.collection.StandardCollection.delete_match`
* :func:`c8.collection.StandardCollection.properties`
* :func:`c8.collection.StandardCollection.revision`
* :func:`c8.collection.StandardCollection.checksum`
* :func:`c8.collection.StandardCollection.rotate`
* :func:`c8.collection.StandardCollection.truncate`
* :func:`c8.collection.StandardCollection.count`
* :func:`c8.collection.StandardCollection.has`
* :func:`c8.collection.StandardCollection.ids`
* :func:`c8.collection.StandardCollection.keys`
* :func:`c8.collection.StandardCollection.all`
* :func:`c8.collection.StandardCollection.find`
* :func:`c8.collection.StandardCollection.find_near`
* :func:`c8.collection.StandardCollection.find_in_range`
* :func:`c8.collection.StandardCollection.find_in_radius`
* :func:`c8.collection.StandardCollection.find_in_box`
* :func:`c8.collection.StandardCollection.find_by_text`
* :func:`c8.collection.StandardCollection.get_many`
* :func:`c8.collection.StandardCollection.random`
* :func:`c8.collection.StandardCollection.indexes`
* :func:`c8.collection.VertexCollection.get`
* :func:`c8.collection.VertexCollection.insert`
* :func:`c8.collection.VertexCollection.update`
* :func:`c8.collection.VertexCollection.replace`
* :func:`c8.collection.VertexCollection.delete`
* :func:`c8.collection.EdgeCollection.get`
* :func:`c8.collection.EdgeCollection.insert`
* :func:`c8.collection.EdgeCollection.update`
* :func:`c8.collection.EdgeCollection.replace`
* :func:`c8.collection.EdgeCollection.delete`

If an unsupported method is called, :class:`c8.exceptions.TransactionStateError`
is raised.

**Example:**

.. testcode::

    from c8 import C8Client, TransactionStateError

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "test" fabric as tenant admin.
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='passwd')

    # Begin a new transaction.
    txn_fabric = fabric.begin_transaction()

    # API method "fabrics()" is not supported and an exception is raised.
    try:
        txn_fabric.fabrics()
    except TransactionStateError as err:
        assert err.source == 'client'
        assert err.message == 'action not allowed in transaction'

When running queries in transactions, the :doc:`cursors <cursor>` are loaded
with the entire result set right away. This is regardless of the parameters
passed in when executing the query (e.g batch_size). You must be mindful of
client-side memory capacity when executing queries that can potentially return
a large result set.

**Example:**

.. testcode::

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "test" fabric as tenant admin.
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the total document count in "students" collection.
    document_count = fabric.collection('students').count()

    # Execute a C8QL query normally (without using transactions).
    cursor1 = fabric.c8ql.execute('FOR doc IN students RETURN doc', batch_size=1)

    # Execute the same C8QL query in a transaction.
    with fabric.begin_transaction() as txn_fabric:
        job = txn_fabric.c8ql.execute('FOR doc IN students RETURN doc', batch_size=1)
    cursor2 = job.result()

    # The first cursor acts as expected. Its current batch contains only 1 item
    # and it still needs to fetch the rest of its result set from the server.
    assert len(cursor1.batch()) == 1
    assert cursor1.has_more() is True

    # The second cursor is pre-loaded with the entire result set, and does not
    # require further communication with C8 Data Fabric server. Note that value of
    # parameter "batch_size" was ignored.
    assert len(cursor2.batch()) == document_count
    assert cursor2.has_more() is False
