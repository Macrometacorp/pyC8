Collections
-----------

A **collection** contains :doc:`documents <document>`. It is uniquely identified
by its name which must consist only of hyphen, underscore and alphanumeric
characters. There are three types of collections in pyC8:

* **Standard Collection:** contains regular documents.
* **Vertex Collection:** contains vertex documents for graphs. See
  :ref:`here <vertex-collections>` for more details.
* **Edge Collection:** contains edge documents for graphs. See
  :ref:`here <edge-collections>` for more details.

Here is an example showing how you can manage standard collections:

The Simple Way

..testcode::

    from c8 import C8Client, C8QLQueryKillError
    # Initialize the C8 client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443,
                      email='user@example.com', password='hidden')

    collection_name = 'students'
    # List All Collections
    print(client.get_collections())

    # Filter collection based on collection models DOC/KV/DYNAMO
    colls = client.get_collections(collectionModel='KV')
    print(colls)

    # Create a new collection if it does not exist
    if client.has_collection(collection_name):
        print("Collection exists)
    else:
        client.create_collection(name=collection_name)

    # Insert a single document
    document = {'_key': 'Abby', 'age': 22}
    client.insert_document(collection_name=collection_name, document=document)

    # Insert multiple documents
    documents = [
        {'_key': 'John', 'age': 18},
        {'_key': 'Mary', 'age': 21}
    ]
    client.insert_document(collection_name=collection_name, document=documents)

    # Insert data from a csv file
    client.insert_document_from_file(collection_name=collection_name,
                                     csv_filepath="~/data.csv")

    # Get Collection properties
    collection_handle = client.get_collection(collection_name)
    print(collection_handle.name)
    print(collection_handle.fabric_name)
    print(collection_handle.count())

    # get collecion ids
    ids = client.get_collection_ids(collname)
    print("ids: ", ids)

    #get collection keys
    keys = client.get_collection_keys(collname)
    print("keys: ", keys)

    # get indexes
    index = client.get_collection_indexes(collname)
    print("indexes: ", index)
    
    # Truncate Collection
    collection_handle.truncate()

    # Delete Collection
    client.delete_collection(name=collection_name)


print(state)

The Object Oriented Way

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443)

    # For the "mytenant" tenant, connect to "test" fabric as tenant admin.
    # This returns an API wrapper for the "test" fabric on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    tenant = client.tenant(email='mytenant@example.com', password='hidden')
    fabric = tenant.useFabric('test')
    # List all collections in the fabric.
    fabric.collections()
    # Filter collections based on collection model DOC/KV/DYNAMO
    fabric.collections(collectionModel='DOC')

    # Create a new collection named "students" if it does not exist.
    # This returns an API wrapper for "students" collection.
    if fabric.has_collection('students'):
        students = fabric.collection('students')
    else:
        students = fabric.create_collection('students')

    # To insert data from a csv file
    # path to csv file should be an absolute path
    students.insert_from_file("~/data.csv")

    # Retrieve collection properties.
    students.name
    students.fabric_name
    students.count()

    # Perform various operations.
    students.truncate()
    students.configure(journal_size=3000000)

    # Delete the collection.
    fabric.delete_collection('students')

See :ref:`StandardFabric` and :ref:`StandardCollection` for API specification.
