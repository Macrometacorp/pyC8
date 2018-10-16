Indexes
-------

**Indexes** can be added to collections to speed up document lookups. Every
collection has a primary hash index on ``_key`` field by default. This index
cannot be deleted or modified. Every edge collection has additional indexes
on fields ``_from`` and ``_to``. For more information on indexes, refer to
`C8 Data Fabric manual`_.

.. _C8 Data Fabric manual: http://www.macrometa.co

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "test" database as tenant admin.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Create a new collection named "cities".
    cities = db.create_collection('cities')

    # List the indexes in the collection.
    cities.indexes()

    # Add a new hash index on document fields "continent" and "country".
    index = cities.add_hash_index(fields=['continent', 'country'], unique=True)

    # Add new fulltext indexes on fields "continent" and "country".
    index = cities.add_fulltext_index(fields=['continent'])
    index = cities.add_fulltext_index(fields=['country'])

    # Add a new skiplist index on field 'population'.
    index = cities.add_skiplist_index(fields=['population'], sparse=False)

    # Add a new geo-spatial index on field 'coordinates'.
    index = cities.add_geo_index(fields=['coordinates'])

    # Add a new persistent index on fields 'currency'.
    index = cities.add_persistent_index(fields=['currency'], sparse=True)

    # Delete the last index from the collection.
    cities.delete_index(index['id'])

See :ref:`StandardCollection` for API specification.
