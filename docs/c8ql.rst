C8QL
----

**C8 Data Fabric Query Language (C8QL)** is used to read and write data. It is similar
to SQL for relational databases, but without the support for data definition
operations such as creating or deleting :doc:`databases <database>`,
:doc:`collections <collection>` or :doc:`indexes <indexes>`. For more
information, refer to `C8 Data Fabric manual`_.

.. _C8 Data Fabric manual: http://www.macrometa.co

C8QL Queries
===========

C8QL queries are invoked from C8QL API wrapper. Executing queries returns
:doc:`result cursors <cursor>`.

**Example:**

.. testcode::

    from c8 import C8Client, C8QLQueryKillError

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')


    # Insert some test documents into "students" collection.
    db.collection('students').insert_many([
        {'_key': 'Abby', 'age': 22},
        {'_key': 'John', 'age': 18},
        {'_key': 'Mary', 'age': 21}
    ])

    # Get the C8QL API wrapper.
    c8ql = db.c8ql

    # Retrieve the execution plan without running the query.
    c8ql.explain('FOR doc IN students RETURN doc')

    # Validate the query without executing it.
    c8ql.validate('FOR doc IN students RETURN doc')

    # Execute the query
    cursor = db.c8ql.execute(
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


C8QL User Functions
==================

**C8QL User Functions** are custom functions you define in Javascript to extend
C8QL functionality. They are somewhat similar to SQL procedures.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the C8QL API wrapper.
    c8ql = db.c8ql

    # Create a new C8QL user function.
    c8ql.create_function(
        # Grouping by name prefix is supported.
        name='functions::temperature::converter',
        code='function (celsius) { return celsius * 1.8 + 32; }'
    )
    # List C8QL user functions.
    c8ql.functions()

    # Delete an existing C8QL user function.
    c8ql.delete_function('functions::temperature::converter')

See :ref:`C8QL` for API specification.


C8QL Query Cache
===============

**C8QL Query Cache** is used to minimize redundant calculation of the same query
results. It is useful when read queries are issued frequently and write queries
are not.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the C8QL API wrapper.
    c8ql = db.c8ql

    # Retrieve C8QL query cache properties.
    c8ql.cache.properties()

    # Configure C8QL query cache properties
    c8ql.cache.configure(mode='demand', limit=10000)

    # Clear results in C8QL query cache.
    c8ql.cache.clear()

See :ref:`C8QLQueryCache` for API specification.
