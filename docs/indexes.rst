Indexes
-------

**Indexes** can be added to collections to speed up document lookups. Every
collection has a primary hash index on ``_key`` field by default. This index
cannot be deleted or modified. Every edge collection has additional indexes
on fields ``_from`` and ``_to``. For more information on indexes, refer to
`C8 Data Fabric manual`_.

.. _C8 Data Fabric manual: http://www.macrometa.co

**Example:**

The Simple Way

.. code-block:: python

    from c8 import C8Client, C8QLQueryKillError
    # Initialize the C8 client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443,
                      email='user@example.com', password='hidden')

    collection_name = 'students'

    # Create a new collection if it does not exist
    if client.has_collection(collection_name):
        print("Collection exists)
    else:
        client.create_collection(name=collection_name)

    # get indexes

    print("Indexes", client.list_collection_indexes(collection_name))

    # Add Indexes to a Collection

    print("Add Hash Index", client.add_hash_index(collection_name,
             fields=['continent', 'country'], unique=True))

    geo = client.add_geo_index(collname, fields=['coordinates'])

    print("Add geo Index", geo)
    print("Add skiplist index", client.add_skiplist_index(collection_name, fields=['population'], sparse=False))

    print("Add persistent Index", client.add_persistent_index(collection_name, fields=['currency'], sparse=True))

    print("Add full text index", client.add_fulltext_index(collection_name, fields=['country']))

    print("Add  TTL Index", client.add_ttl_index(collection_name, fields=['country'], expireAfter=0))

    geo_name = geo['name']

    # Get Index
    print("Get Index ", client.get_index(collname, geo_name))

    # Delete Index - You can fetch IndexName from the details after list Indexes
    #client.delete_index(collection_name, <index_name>, ignore_missing=False)
    print("Delete Geo Index ", client.delete_index(collname, geo_name))

    # Delete Cretaed Collection
    client.delete_collection(name=collname)


The Object Oriented Way

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443)

    # Connect to "test" fabric as tenant admin.
    tenant = client.tenant(email='mytenant@example.com', password='hidden')
    fabric = tenant.useFabric('test')
    # Create a new collection named "cities".
    cities = fabric.create_collection('cities')

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
