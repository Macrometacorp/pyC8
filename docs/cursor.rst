Cursors
-------

Many operations provided by pyC8 (e.g. executing :doc:`c8ql` queries)
return result **cursors** to batch the network communication between C8 Data Fabric
server and pyC8 client. Each HTTP request from a cursor fetches the
next batch of results (usually documents). Depending on the query, the total
number of items in the result set may or may not be known in advance.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Set up some test data to query against.
    db.collection('students').insert_many([
        {'_key': 'Abby', 'age': 22},
        {'_key': 'John', 'age': 18},
        {'_key': 'Mary', 'age': 21},
        {'_key': 'Suzy', 'age': 23},
        {'_key': 'Dave', 'age': 20}
    ])

    # Execute a C8QL query which returns a cursor object.
    cursor = db.c8ql.execute(
        'FOR doc IN students FILTER doc.age > @val RETURN doc',
        bind_vars={'val': 17},
        batch_size=2,
        count=True
    )

    # Get the cursor ID.
    cursor.id

    # Get the items in the current batch.
    cursor.batch()

    # Check if the current batch is empty.
    cursor.empty()

    # Get the total count of the result set.
    cursor.count()

    # Flag indicating if there are more to be fetched from server.
    cursor.has_more()

    # Flag indicating if the results are cached.
    cursor.cached()

    # Get the performance profile.
    cursor.profile()

    # Get any warnings produced from the query.
    cursor.warnings()

    # Return the next item from the cursor. If current batch is depleted, the
    # next batch if fetched from the server automatically.
    cursor.next()

    # Return the next item from the cursor. If current batch is depleted, an
    # exception is thrown. You need to fetch the next batch manually.
    cursor.pop()

    # Fetch the next batch and add them to the cursor object.
    cursor.fetch()

    # Delete the cursor from the server.
    cursor.close()

See :ref:`Cursor` for API specification.

If the fetched result batch is depleted while you are iterating over a cursor
(or while calling the method :func:`c8.cursor.Cursor.next`), pyC8
automatically sends an HTTP request to the server to fetch the next batch
(just-in-time style). To control exactly when the fetches occur, you can use
methods :func:`c8.cursor.Cursor.fetch` and :func:`c8.cursor.Cursor.pop`
instead.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Set up some test data to query against.
    db.collection('students').insert_many([
        {'_key': 'Abby', 'age': 22},
        {'_key': 'John', 'age': 18},
        {'_key': 'Mary', 'age': 21}
    ])

    # If you iterate over the cursor or call cursor.next(), batches are
    # fetched automatically from the server just-in-time style.
    cursor = db.c8ql.execute('FOR doc IN students RETURN doc', batch_size=1)
    result = [doc for doc in cursor]

    # Alternatively, you can manually fetch and pop for finer control.
    cursor = db.c8ql.execute('FOR doc IN students RETURN doc', batch_size=1)
    while cursor.has_more(): # Fetch until nothing is left on the server.
        cursor.fetch()
    while not cursor.empty(): # Pop until nothing is left on the cursor.
        cursor.pop()

When running queries in :doc:`transactions <transaction>`, cursors are loaded
with the entire result set right away. This is regardless of the parameters
passed in when executing the query (e.g. batch_size). You must be mindful of
client-side memory capacity when executing queries that can potentially return
a large result set.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the total document count in "students" collection.
    document_count = db.collection('students').count()

    # Execute a C8QL query normally (without using transactions).
    cursor1 = db.c8ql.execute('FOR doc IN students RETURN doc', batch_size=1)

    # Execute the same C8QL query in a transaction.
    txn_db = db.begin_transaction()
    job = txn_db.c8ql.execute('FOR doc IN students RETURN doc', batch_size=1)
    txn_db.commit()
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
