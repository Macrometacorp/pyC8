
**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to the system database of the "mytenant" tenant.
    # This connection is made as the tenant admin using the tenant admin username and password
    tennt = client.tenant(name='mytenant', dbname='_system', username='root', password='root_pass')

    # Connect to "_system" database as root user.
    sys_db = client.db(tenant='mytenant', name='_system', username='root', password='root_pass')
    
    #Create a new global persistent stream called test-stream. If persistent flag set to False,
    # a non-persistent stream gets created. Similarly a local stream gets created if local 
    # flag is set to True. By default persistent is set to True and local is set to False . 
    sys_db.create_stream('test-stream', persistent=True, local=False)
    
    #List all persistent/non-persistent and global/local streams 
    streams = sys_db.streams()
    
    #Create producer for the given persistent/non-persistent and global/local stream.
    stream_collection = sys_db.stream()
    producer = stream_collection.create_producer('test-stream', persistent=True, local=False)
    
    #Send: is a plusar method that publish/sends a given message over stream in byte's.
    producer.send(b"Hello")
    
    #Create a subscriber to the given persistent/non-persistent and global/local stream with the given,
    # subscription name. If no subscription new is provided then a random name is generated. based on
    # tenant and db information
    subscriber = stream_collection.subscribe('test-stream', persistent=True, local=False, subscription_name="test-subscription")
    
    #receive: is a plusar function to read the published messages over stream.
    subscriber.receive()
    
    #Delete a given persistent/non-persistent and global/local stream.
    sys_db.delete_stream('test-stream', persistent=True, local=False)


See :ref:`C8Client` and :ref:`StreamCollection` for API specification.
