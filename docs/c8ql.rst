C8QL
----

**C8 Data Fabric Query Language (C8QL)** is used to read and write data. It is similar
to SQL for relational fabrics, but without the support for data definition
operations such as creating or deleting :doc:`fabrics <fabric>`,
:doc:`collections <collection>` or :doc:`indexes <indexes>`. For more
information, refer to `C8 Data Fabric manual`_.

.. _C8 Data Fabric manual: http://www.macrometa.co

C8QL Queries
===========

C8QL queries are invoked from C8QL API wrapper. Executing queries returns
:doc:`result cursors <cursor>`.

**Example:**

The Simple Way

..testcode::

    from c8 import C8Client, C8QLQueryKillError
    # Initialize the C8 client.
    client = C8Client(protocol='https', host='qa3.eng3.macrometa.io', port=443,
                      email='guest@macrometa.io', password='guest')
    # define constants
    documents = [
        {'_key': 'Abby', 'age': 22},
        {'_key': 'John', 'age': 18},
        {'_key': 'Mary', 'age': 21}
    ]
    query = 'FOR doc IN students RETURN doc'

    # Create a students Collection and Insert Documents in it.
    client.create_collection('students')
    client.insert_document(collection_name='students', document=documents)

    # Explain query and validate query
    valid = client.validate_query(query)
    print("Vaildate query: ",valid)
    explain = client.explain_query(query)
    print("Explain query: ", explain)

    # Execute Query
    resp = client.execute_query(query)
    print(resp)

    # Iterate through the result cursor
    student_keys = [doc['_key'] for doc in resp]

    # List running queries
    print(client.get_running_queries())

    # Kill Query(this should fail due to invalid ID).
    try:
        client.kill_query('some_query_id')
    
    except C8QLQueryKillError as err:
        print('ERROR: ', err)



The Object Oriented Way 

.. testcode::

    from c8 import C8Client, C8QLQueryKillError

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443)

    # For the "mytenant" tenant, connect to "test" fabric as tenant admin.
    # This returns an API wrapper for the "test" fabric on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    tenant = client.tenant(email='mytenant@example.com', password='tenant-password')
    fabric = tenant.useFabric('test')


    # Insert some test documents into "students" collection.
    fabric.collection('students').insert_many([
        {'_key': 'Abby', 'age': 22},
        {'_key': 'John', 'age': 18},
        {'_key': 'Mary', 'age': 21}
    ])

    # Get the C8QL API wrapper.
    c8ql = fabric.c8ql

    # Retrieve the execution plan without running the query.
    c8ql.explain('FOR doc IN students RETURN doc')

    # Validate the query without executing it.
    c8ql.validate('FOR doc IN students RETURN doc')

    # Execute the query
    cursor = fabric.c8ql.execute(
      'FOR doc IN students FILTER doc.age < @value RETURN doc',
      bind_vars={'value': 19}
    )
    # Iterate through the result cursor
    student_keys = [doc['_key'] for doc in cursor]

    # List currently running queries.
    c8ql.queries()

    # List any slow queries.
    c8ql.slow_queries()

    # Clear slow C8QL queries if any.
    c8ql.clear_slow_queries()

    # Kill a running query (this should fail due to invalid ID).
    try:
        c8ql.kill('some_query_id')
    except C8QLQueryKillError as err:
        assert err.http_code == 400
        assert err.error_code == 1591
        assert 'cannot kill query' in err.message

See :ref:`C8QL` for API specification.


