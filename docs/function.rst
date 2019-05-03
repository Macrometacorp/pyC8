Functions
-------

Functions lets user package code into containers and then deploy them on geo fabrics.
Once deployed, C8 orchestrates the containers to execute on demand in edge
locations in response to requests from clients.

Here is an example showing how you can manage functions:

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" fabric as tenant admin.
    # This returns an API wrapper for the "test" fabric on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    function = client.function(tenant='mytenant', fabricname='test',
                               username='root', password='passwd')
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='passwd')

    **To create functions with input trigger type as Collection**
    * create a collection "mycollection" into fabric
        collection = fabric.create_collection("mycollection")

    * create local function
        function.create_function(name="localfn", image="Macrometa/hello-world",
                                 triggers=["mycollection"], trigger_type="Collection",
                                 is_local=True)

    * to invoke function "localfn", insert a document into the collection "mycollection"
        collection.insert({"message": "testing functions"})

    **To create functions with input trigger type as Stream**
    * create a local stream "mystream" into fabric
        fabric.create_stream("mystream, local=True)

    * create local function
        function.create_function(name="localfn", image="Macrometa/hello-world",
                                 triggers=["mystream"], trigger_type="Stream",
                                 is_local=True)

    * to invoke function "localfn", create a producer and publish a message
        stream = fabric.stream()
        producer = stream.create_producer("mystream", local=True)
        msg = {"message": "testing functions"}
        producer.send(bytes(json.dumps(msg), 'utf-8'))

    **To delete functions**
    function.delete_function(name="localfn")

    **To get summary of functions**
    response = function.get_function_summary(name="localfn")

    **To update function size and image name for a function**
    function.update_function(name="localfn", image="Macrometa/hello-world",
                             function_size="default")

    **To update input triggers for function**
    function.update_input_triggers_function("localfn", ["stream1"], "stream")

    **To list functions**
    response = function.list_functions()
