Batch Execution
---------------

pyC8 supports **batch execution**. Requests to C8 Data Fabric server are
placed in client-side in-memory queue, and committed together in a single HTTP
call. After the commit, results can be retrieved from :ref:`BatchJob` objects.

**Example:**

.. code-block:: python

    from c8 import C8Client, C8QLQueryExecuteError

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" fabric as tenant admin.
    # This returns an API wrapper for the "test" fabric on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the API wrapper for "students" collection.
    students = fabric.collection('students')

    # Begin batch execution via context manager. This returns an instance of
    # BatchFabric, a fabric-level API wrapper tailored specifically for
    # batch execution. The batch is automatically committed when exiting the
    # context. The BatchFabric wrapper cannot be reused after commit.
    with fabric.begin_batch_execution(return_result=True) as batch_fabric:

        # Child wrappers are also tailored for batch execution.
        batch_c8ql = batch_fabric.c8ql
        batch_col = batch_fabric.collection('students')

        # API execution context is always set to "batch".
        assert batch_fabric.context == 'batch'
        assert batch_c8ql.context == 'batch'
        assert batch_col.context == 'batch'

        # BatchJob objects are returned instead of results.
        job1 = batch_col.insert({'_key': 'Kris'})
        job2 = batch_col.insert({'_key': 'Rita'})
        job3 = batch_c8ql.execute('RETURN 100000')
        job4 = batch_c8ql.execute('INVALID QUERY')  # Fails due to syntax error.

    # Upon exiting context, batch is automatically committed.
    assert 'Kris' in students
    assert 'Rita' in students

    # Retrieve the status of each batch job.
    for job in batch_fabric.queued_jobs():
        # Status is set to either "pending" (transaction is not committed yet
        # and result is not available) or "done" (transaction is committed and
        # result is available).
        assert job.status() in {'pending', 'done'}

    # Retrieve the results of successful jobs.
    metadata = job1.result()
    assert metadata['_id'] == 'students/Kris'

    metadata = job2.result()
    assert metadata['_id'] == 'students/Rita'

    cursor = job3.result()
    assert cursor.next() == 100000

    # If a job fails, the exception is propagated up during result retrieval.
    try:
        result = job4.result()
    except C8QLQueryExecuteError as err:
        assert err.http_code == 400
        assert err.error_code == 1501
        assert 'syntax error' in err.message

    # Batch execution can be initiated without using a context manager.
    # If return_result parameter is set to False, no jobs are returned.
    batch_fabric = fabric.begin_batch_execution(return_result=False)
    batch_fabric.collection('students').insert({'_key': 'Jake'})
    batch_fabric.collection('students').insert({'_key': 'Jill'})

    # The commit must be called explicitly.
    batch_fabric.commit()
    assert 'Jake' in students
    assert 'Jill' in students

.. note::
    * Be mindful of client-side memory capacity when issuing a large number of
      requests in single batch execution.
    * :ref:`BatchFabric` and :ref:`BatchJob` instances are stateful objects,
      and should not be shared across multiple threads.
    * :ref:`BatchFabric` instance cannot be reused after commit.

See :ref:`BatchFabric` and :ref:`BatchJob` for API specification.
