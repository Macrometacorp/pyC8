Streams
---------

Macrometa Streams provide realtime pub/sub messaging capabilities for the Macrometa Edge Fabric. They allow client programs to send and receive messages to/from the fabric servers and allow for communication between different fabric components.

**Example:**
 The Simple Way

 .. testcode::
    from c8 import C8Client
    # Initialize the C8 client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443,
                          email='user@example.com', password='hidden')
    
    stream = "teststream"
    
    # Create stream
    print(client.create_stream(stream))

    # Check if stream is created
    print("Has stream: ", client.has_stream(stream))

    # Get all streams
    print("Get Streams: ", client.get_streams())

    # Get Stream Statistics
    print("Stream Stats: ", client.get_stream_stats(stream))
    
    # create producer
    producer = client.create_stream_producer(stream)

    for i in range(10):
      msg1 = "Persistent: Hello from " + "("+ str(i) +")"
      data = {
        "payload" : base64.b64encode(six.b(msg1)).decode("utf-8")
      }
      producer.send(json.dumps(data))

    # create subscriber
    subscriber = client.subscribe(stream=stream, local=False, subscription_name="test-subscription-1")
    for i in range(10):
        m1 = json.loads(subscriber.recv())  #Listen on stream for any receiving msg's
        msg1 = base64.b64decode(m1["payload"])
        print("Received message '{}' id='{}'".format(msg1, m1["messageId"])) #Print the received msg over stream
        subscriber.send(json.dumps({'messageId': m1['messageId']}))#Acknowledge the received msg.

    # Get all stream subscriptions
    print(client.get_stream_subscriptions(stream=stream, local=False))

    # Get stream backlog
    print(client.get_stream_backlog(stream=stream, local=False))

    # Clear Stream Backlog for a subscription
    print(client.clear_stream_backlog(subscription="test-subscription-1"))

    # Clear Stream Backlog
    print(client.clear_streams_backlog())

    # Unsubscribe
    client.unsubscribe("test-subscription-1")

    # Delete Subscription(Will not work after Unsubscribe)
    #client.delete_stream_subscription(stream, "test-subscription-1", local=False)

    # Terminate Stream
    print(client.terminate_stream(stream))


 The object Oriented Way

.. testcode::

    from c8 import C8Client
    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443)
    # Connect to the system fabric of the "mytenant" tenant.
    # This connection is made as the tenant admin using the tenant admin username and password
    tennt = client.tenant(email='mytenant',  password='hidden')
    # Connect to "_system" fabric as root user.
    sys_fabric = tenant.useFabric('_system')

    ######## Stream enumeration/listing and existence checks ########
    # List all streams present on the server for this DB, regardless of whether or not it is persistent and global/local
    streams = sys_fabric.streams()
    print("\nStream listing of all streams in the fabric:")
    print(str(streams))

    # List all persistent local streams.
    print( sys_fabric.persistent_streams(local=True))

    # List all persistent global streams.
    print( sys_fabric.persistent_streams(local=False) )

    # Check if a given stream exists.
    sys_fabric.has_stream('testfabricPersLocal', isCollectionStream=False)

    #Check if a given collection stream exists
    sys_fabric.has_stream('testfabricPersLocal', isCollectionStream=True)

    # Check if a given persistent local stream exists.
    sys_fabric.has_persistent_stream('testfabricPersLocal', local=True)

    # Check if a given persistent global stream exists.
    sys_fabric.has_persistent_stream('testfabricPersGlobal', local=False)

    ######## Stream creation and publish/subscribe messages on stream ########

    #Create a StreamCollection object to invoke stream management functions.
    stream_collection = sys_fabric.stream()

    # Create producer for the given persistent and global/local stream that is created. You can override default compression types/routing modes as shown.
    producer1 = stream_collection.create_producer('test-stream', local=False, compression_type = stream_collection.COMPRESSION_TYPES.LZ4)
    producer2 = stream_collection.create_producer('test-stream-1', local=True, message_routing_mode= stream_collection.ROUTING_MODE.SINGLE_PARTITION)

    #send: publish/send a given message over stream in bytes.
    for i in range(10):
      msg1 = "Persistent: Hello from " + region + "("+ str(i) +")"
      data = {
        "payload" : base64.b64encode(six.b(msg1)).decode("utf-8")
      }
      producer1.send(json.dumps(data))

    # Create a subscriber to the given persistent and global/local stream with the given,
    # subscription name. If no subscription new is provided then a random name is generated based on
    # tenant and fabric information.
    # NOTE: If using producers and subscribers in the same source file, the stream object must be different
     between producers and subscribers.
    stream_collection = sys_fabric.stream()
    subscriber1 = substream_collection.subscribe('test-stream', local=False, subscription_name="test-subscription-1",consumer_type= stream_collection.CONSUMER_TYPES.EXCLUSIVE)

    #receive: read the published messages over stream.
    for i in range(10):
       m1 = json.loads(subscriber1.recv())  #Listen on stream for any receiving msg's
       m2 = json.loads(subscriber2.recv())
       msg1 = base64.b64encode(m1["payload"])
       msg2 = base64.b64encode(m2["payload"])
       print("Received message '{}' id='{}'".format(msg1, m1["messageId"]) #Print the received msg over stream
       print("Received message '{}' id='{}'".format(msg2, m2["messageId"]) #Print the received msg over stream
       subscriber1.send(json.dumps({'messageId': m1['messageId']}))#Acknowledge the received msg.
       subscriber2.send(json.dumps({'messageId': m2['messageId']}))#Acknowledge the received msg. 

    #Get the list of subscriptions for a given persistent local/global stream.
    stream_collection.get_stream_subscriptions('test-stream-1', local=False) #for global persistent stream

    #get_stream_stats
    stream_collection.get_stream_stats('test-stream-1', local=False) #for global persistent stream

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

    #Clear backlog for all streams on a stream fabric
    stream_collection.clear_streams_backlog()

    #Unsubscribes the given subscription on all streams on a stream fabric
    stream_collection.unsubscribe('test-subscription-1')

    #delete subscription of a stream
    #stream_collection.delete_stream_subscription('test-stream-1', 'test-subscription-1' , local=False)

See :ref:`StreamCollection` for API specification.
