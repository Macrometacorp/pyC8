Async Execution
---------------

pyC8 supports **async execution**, where it sends requests to C8 Data Fabric
server in fire-and-forget style (HTTP 202 returned). The server places incoming
requests in its queue and processes them in the background. The results can be
retrieved from the server later via :ref:`AsyncJob` objects.

**Example:**

.. testcode::

    import time

    from c8 import (
        C8Client,
        C8QLQueryExecuteError,
        AsyncJobCancelError,
        AsyncJobClearError
    )

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "test" fabric as tenant admin.
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='passwd')

    # Begin async execution. This returns an instance of AsyncFabric, a
    # fabric-level API wrapper tailored specifically for async execution.
    async_fabric = fabric.begin_async_execution(return_result=True)

    # Child wrappers are also tailored for async execution.
    async_c8ql = async_fabric.c8ql
    async_col = async_fabric.collection('students')

    # API execution context is always set to "async".
    assert async_fabric.context == 'async'
    assert async_c8ql.context == 'async'
    assert async_col.context == 'async'

    # On API execution, AsyncJob objects are returned instead of results.
    job1 = async_col.insert({'_key': 'Neal'})
    job2 = async_col.insert({'_key': 'Lily'})
    job3 = async_c8ql.execute('RETURN 100000')
    job4 = async_c8ql.execute('INVALID QUERY')  # Fails due to syntax error.

    # Retrieve the status of each async job.
    for job in [job1, job2, job3, job4]:
        # Job status can be "pending", "done" or "cancelled".
        assert job.status() in {'pending', 'done', 'cancelled'}

        # Let's wait until the jobs are finished.
        while job.status() != 'done':
            time.sleep(0.1)

    # Retrieve the results of successful jobs.
    metadata = job1.result()
    assert metadata['_id'] == 'students/Neal'

    metadata = job2.result()
    assert metadata['_id'] == 'students/Lily'

    cursor = job3.result()
    assert cursor.next() == 100000

    # If a job fails, the exception is propagated up during result retrieval.
    try:
        result = job4.result()
    except C8QLQueryExecuteError as err:
        assert err.http_code == 400
        assert err.error_code == 1501
        assert 'syntax error' in err.message

    # Cancel a job. Only pending jobs still in queue may be cancelled.
    # Since job3 is done, there is nothing to cancel and an exception is raised.
    try:
        job3.cancel()
    except AsyncJobCancelError as err:
        assert err.message.endswith('job {} not found'.format(job3.id))

    # Clear the result of a job from C8 Data Fabric server to free up resources.
    # Result of job4 was removed from the server automatically upon retrieval,
    # so attempt to clear it raises an exception.
    try:
        job4.clear()
    except AsyncJobClearError as err:
        assert err.message.endswith('job {} not found'.format(job4.id))

    # List the IDs of the first 100 async jobs completed.
    fabric.async_jobs(status='done', count=100)

    # List the IDs of the first 100 async jobs still pending.
    fabric.async_jobs(status='pending', count=100)

    # Clear all async jobs still sitting on the server.
    fabric.clear_async_jobs()

.. note::
    Be mindful of server-side memory capacity when issuing a large number of
    async requests in small time interval.

See :ref:`AsyncFabric` and :ref:`AsyncJob` for API specification.
