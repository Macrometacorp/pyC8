API Keys
---------

The Simple Way

.. testcode::
    
    from c8 import C8Client, C8QLQueryKillError
    # Initialize the C8 client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443,
                      email='guest@macrometa.io', password='guest')
    
    # Create Collection
    if client.has_collection('testCollection'):
         print("Collection exists")
    else:
         client.create_collection(name='testCollection')

    # Create stream
    print(client.create_stream('testStream'))


    print("Create API Key: ", client.create_api_key('id1'))

    print("GET API Keys: ", client.list_all_api_keys())

    print("Accessible Databases: ", client.list_accessible_databases('id1'))

    print("Accessible Streams of a db: ", client.list_accessible_streams('id1', '_system'))

    print("Set DB Access Level: ", client.set_database_access_level('id1', '_system', 'rw'))

    print("Set Coll Access Level: ", client.set_collection_access_level('id1', 'testCollection', '_system', 'rw'))

    print("Set Stream Access Level: ", client.set_stream_access_level('id1','testStream', '_system'))

    print("Get DB Access Level", client.get_database_access_level('id1','_system'))

    print("Get Coll Access Level: ", client.get_collection_access_level('id1','testCollection', '_system'))

    print("Get Stream Access Level: ", client.get_stream_access_level('id1','testStream', '_system'))

    print("Clear DB Access Level: ", client.clear_database_access_level('id1','_system'))

    print("Clear Coll Access Level: ", client.clear_collection_access_level('id1','testCollection', '_system'))

    print("Clear Stream Access Level: ", client.clear_stream_access_level('id1','testStream', '_system'))


    print("Get Billing Access Level: ", client.get_billing_access_level('id1'))

    print("Set Billing Access Level: ", client.set_billing_access_level('id1','ro'))

    print("Clear Billing Access Level: ", client.clear_billing_access_level('id1'))
    remove = client.remove_api_key('id1')

    print(remove)


The Object Oriented Way

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443)
     # This returns an API wrapper for the "test" fabric on tenant 'mytenant'
     # Note that the 'mytenant' tenant should already exist.
     tenant = client.tenant(email='guest@macrometa.io', password='guest')
     fabric = tenant.useFabric('_system')
     fabric.create_stream('testStream')
     
     if fabric.has_collection('testCollection'):
         students = fabric.collection('testCollection')
     else:
         students = fabric.create_collection('testCollection')
     # API keys
     
     apiKeys = fabric.api_keys("id1")
     
     create = apiKeys.create_api_key()
     
     print("Create API Key: ", create)
     
     print("GET API Keys: ", fabric.list_all_api_keys())
     
     print("Accessible Databases: ", apiKeys.list_accessible_databases())
     
     print("Accessible Streams of a db: ", apiKeys.list_accessible_streams('_system'))
     
     print("Set DB Access Level: ", apiKeys.set_database_access_level('_system', 'rw'))
     
     print("Set Coll Access Level: ", apiKeys.set_collection_access_level('testCollection', '_system', 'rw'))
     
     print("Set Stream Access Level: ", apiKeys.set_stream_access_level('testStream', '_system'))
     
     print("Get DB Access Level", apiKeys.get_database_access_level('_system'))
     
     print("Get Coll Access Level: ", apiKeys.get_collection_access_level('testCollection', '_system'))
     
     print("Get Stream Access Level: ", apiKeys.get_stream_access_level('testStream', '_system'))
     
     print("Clear DB Access Level: ", apiKeys.clear_database_access_level('_system'))
     
     print("Clear Coll Access Level: ", apiKeys.clear_collection_access_level('testCollection', '_system'))
     
     print("Clear Stream Access Level: ", apiKeys.clear_stream_access_level('testStream', '_system'))
     
     
     print("Get Billing Access Level: ", apiKeys.get_billing_access_level())
     
     print("Set Billing Access Level: ", apiKeys.set_billing_access_level('ro'))
     
     print("Clear Billing Access Level: ", apiKeys.clear_billing_access_level())
     remove = apiKeys.remove_api_key()
     
     print(remove)

