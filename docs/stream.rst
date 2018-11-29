Streams
---------

Macrometa Streams provide realtime pub/sub messaging capabilities for the Macrometa Edge Fabric. They allow client programs to send and receive messages to/from the fabric servers and allow for communication between different fabric components.

**Example:**

.. testcode::

    from c8 import C8Client
    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)
    # Connect to the system fabric of the "mytenant" tenant.
    # This connection is made as the tenant admin using the tenant admin username and password
    tennt = client.tenant(name='mytenant', fabricname='_system', username='root', password='root_pass')
    # Connect to "_system" fabric as root user.
    sys_fabric = client.fabric(tenant='mytenant', name='_system', username='root', password='root_pass')
    
    ######## Stream enumeration/listing and existence checks ########
    # List all streams present on the server for this DB, regardless of whether or not it is persistent/non-persistent and global/local
    streams = sys_fabric.streams()
    print("\nStream listing of all streams in the fabric:")
    print(str(streams))
    
    # List all persistent local streams.
    print( sys_fabric.persistent_streams(local=True) )
    
    # List all persistent global streams.
    print( sys_fabric.persistent_streams(local=False) )
    
    # List all nonpersistent local streams.
    print( sys_fabric.nonpersistent_streams(local=True) )
    
    # List all nonpersistent global streams.
    print( sys_fabric.nonpersistent_streams(local=False) )
    
    # Check if a given stream exists.
    sys_fabric.has_stream('testfabricPersLocal')
    
    # Check if a given persistent local stream exists.
    sys_fabric.has_persistent_stream('testfabricPersLocal', local=True)
    
    # Check if a given persistent global stream exists.
    sys_fabric.has_persistent_stream('testfabricPersGlobal', local=False)
    
    # Check if a given nonpersistent local stream exists.
    sys_fabric.has_nonpersistent_stream('testfabricNonpersLocal', local=True)
    
    # Check if a given nonpersistent global stream exists.
    sys_fabric.has_nonpersistent_stream('testfabricNonpersGlobal', local=False)
        
    ######## Stream creation and publish/subscribe messages on stream ########
    
    #Create a new global persistent stream called test-stream. If persistent flag set to False,
    # a non-persistent stream gets created. Similarly a local stream gets created if local 
    # flag is set to True. By default persistent is set to True and local is set to False . 
    sys_fabric.create_stream('test-stream', persistent=True, local=False)    
    
    #Create a new local non-persistent stream called test-stream-1
    sys_fabric.create_stream('test-stream-1', persistent=False, local=True)
    
    #Create a StreamCollection object to invoke stream management functions.
    stream_collection = sys_fabric.stream()
    
    #Create producer for the given persistent/non-persistent and global/local stream that is created.
    producer1 = stream_collection.create_producer('test-stream', persistent=True, local=False)
    producer2 = stream_collection.create_producer('test-stream-1', persistent=False, local=True)
    
    #send: publish/send a given message over stream in bytes.
    for i in range(10):
      msg1 = "Persistent: Hello from " + region + "("+ str(i) +")"
      msg2 = "Non-persistent: Hello from " + region + "("+ str(i) +")"
      producer1.send(msg1.encode('utf-8'))
      producer2.send(msg2.encode('utf-8'))
    
    #Create a subscriber to the given persistent/non-persistent and global/local stream with the given,
    # subscription name. If no subscription new is provided then a random name is generated based on
    # tenant and fabric information.
    # NOTE: If using producers and subscribers in the same source file, the stream object must be different
     between producers and subscribers.
    stream_collection = sys_fabric.stream()
    subscriber1 = substream_collection.subscribe('test-stream', persistent=True, local=False, subscription_name="test-subscription-1",consumer_type= stream_collection.CONSUMER_TYPES.EXCLUSIVE)
    #you can subscribe using consumer_types option.
    subscriber2 = substream_collection.subscribe('test-stream-1', persistent=False, local=True, subscription_name="test-subscription-2")
    
    #receive: read the published messages over stream.
    for i in range(10):
       msg1 = subscriber1.receive()  #Listen on stream for any receiving msg's
       msg2 = subscriber2.receive()
       print("Received message '{}' id='{}'".format(msg1.data(), msg1.message_id())) #Print the received msg over stream
       print("Received message '{}' id='{}'".format(msg2.data(), msg2.message_id()))
       subscriber1.acknowledge(msg1) #Acknowledge the received msg.
       subscriber2.acknowledge(msg2)
    
    #Get the list of subscriptions for a given persistent/non-persistent local/global stream.
    stream_collection.get_stream_subscriptions('test-stream-1', persistent=True, local=False) #for global persistent stream
    
    #get_stream_stats
    stream_collection.get_stream_stats('test-stream-1', persistent=True, local=False) #for global persistent stream
    
    #Skip all messages on a stream subscription
    stream_collection.skip_all_messages_for_subscription('test-stream-1', 'test-subscription-1')
    
    #Skip num messages on a topic subscription
    stream_collection.skip_messages_for_subscription('test-stream-1', 'test-subscription-1', 10)
    
    #Expire messages for a given subscription of a stream.
    #expire time is in seconds
    stream_collection.expire_messages_for_subscription('test-stream-1', 'test-subscription-1', 2)
    
    #Expire messages on all subscriptions of stream
    stream_collection.expire_messages_for_subscriptions('test-stream-1',2)
    
    #Reset subscription to message position to closest timestamp
    #time is in milli-seconds
    stream_collection.reset_message_subscription_by_timestamp('test-stream-1','test-subscription-1', 5)
    
    #Reset subscription to message position closest to given position
    stream_collection.reset_message_for_subscription('test-stream-1', 'test-subscription-1')
    stream_collection.reset_message_subscription_by_position('test-stream-1','test-subscription-1', 4)
    
    #trigger compaction status
    stream_collection.put_stream_compaction_status('test-stream-5')
    
    #get stream compaction status
    stream_collection.get_stream_compaction_status('test-stream-5')
    
    #Clear backlog for all streams on a stream fabric
    stream_collection.clear_streams_backlog()
   
    #Unsubscribes the given subscription on all streams on a stream fabric
    stream_collection.unsubscribe('test-subscription-1')
    
    #delete subscription of a stream
    #stream_collection.delete_stream_subscription('test-stream-1', 'test-subscription-1' ,persistent=True, local=False)

See :ref:`StreamCollection` for API specification.
