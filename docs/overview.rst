Getting Started
---------------

Here is an example showing how **pyC8** client can be used:

.. testcode::

   from c8 import C8Client
   import time
   import warnings
   warnings.filterwarnings("ignore")

   region = "gdn1.macrometa.io"
   demo_tenant = "demotenant@example.com"
   demo_fabric = "demofabric"
   demo_user = "demouser@example.com"
   demo_collection = "employees"
   demo_stream = "demostream"

   #--------------------------------------------------------------
   print("Create demo tenant...")
   client = C8Client(protocol='https', host=region, port=443)
   sys_tenant = client.tenant(email='macrometa_admin_email', password='hidden')

   if not sys_tenant.has_tenant(demo_tenant):
      sys_tenant.create_tenant(demo_tenant, passwd="hidden", dclist="qa1-us-east-1,qa1-us-east-2,qa1-us-east-3") # dclist: list of comma separated region in which tenant has to be created

   #--------------------------------------------------------------

    print("Connect to fabric and get details")
    client = C8Client(protocol='https', host=region, port=443)
    sys_fabric = sys_tenant.useFabric('_system)
    #Returns the list of details of Datacenters
    sys_fabric.dclist(detail=True)

   #--------------------------------------------------------------
   print("Create under demotenant, demofabric, demouser and assign permissions...")
   demotenant = client.tenant(email=demo_tenant, password='hidden')
   fabric = demotenant.useFabric('_system')
   if not demotenant.has_user(demo_user):
     demotenant.create_user(email=demo_user, password='hidden', active=True)

   if not fabric.has_fabric(demo_fabric):
     fabric.create_fabric(name=demo_fabric, dclist=demotenant.dclist(detail=False))

   demotenant.update_permission(username=demo_user, permission='rw', fabric=demo_fabric)

   #--------------------------------------------------------------
   print("Create and populate employees collection in demofabric...")
   fabric = demotenant.useFabric('demo-fabric)
   employees = fabric.create_collection('employees') # Create a new collection named "employees".
   employees.add_hash_index(fields=['email'], unique=True) # Add a hash index to the collection.

   employees.insert({'firstname': 'Jean', 'lastname':'Picard', 'email':'jean.picard@macrometa.io'})
   employees.insert({'firstname': 'James', 'lastname':'Kirk', 'email':'james.kirk@macrometa.io'})
   employees.insert({'firstname': 'Han', 'lastname':'Solo', 'email':'han.solo@macrometa.io'})
   employees.insert({'firstname': 'Bruce', 'lastname':'Wayne', 'email':'bruce.wayne@macrometa.io'})

   #--------------------------------------------------------------
   print("query employees collection...")
   cursor = fabric.c8ql.execute('FOR employee IN employees RETURN employee') # Execute a C8QL query
   docs = [document for document in cursor]
   print(docs)

   #--------------------------------------------------------------
   print("Create global & local streams in demofabric...")
   fabric.create_stream(demo_stream, local=False)
   fabric.create_stream(demo_stream, local=True)

   streams = fabric.streams()
   print("streams:", streams)

   #--------------------------------------------------------------
   #sys_tenant.delete_tenant(demo_tenant)



Example to **query** a given fabric:

.. testcode::

  from c8 import C8Client
  import json
  import warnings
  warnings.filterwarnings("ignore")

  region = "gdn1.macrometa.io"

  #--------------------------------------------------------------
  print("query employees collection...")
  client = C8Client(protocol='https', host=region, port=443)
  tenant = client.tenant(email = 'user@example.com', password='hidden')
  fabric = tenant.useFabric('demo-fabric')
  #get fabric details
  fabric.fabrics_detail()
  cursor = fabric.c8ql.execute('FOR employee IN employees RETURN employee') # Execute a C8QL query
  docs = [document for document in cursor]
  print(docs)



Example for **real-time updates** from a collection in fabric:

.. testcode::

  from c8 import C8Client
  import warnings
  warnings.filterwarnings("ignore")

  region = "gdn1.macrometa.io"

  def callback_fn(event):
      print(event)

  #--------------------------------------------------------------
  print("Subscribe to employees collection...")
  client = C8Client(protocol='https', host=region, port=443)
  tenant = client.tenant(email="user@example.com", password="hidden")
  fabric = tenant.useFabric('demo-fabric')
  fabric.on_change("employees", timeout=10, callback=callback_fn)



Example to **publish** documents to a stream:

.. testcode::

  from c8 import C8Client
  import time
  import warnings
  warnings.filterwarnings("ignore")

  region = "gdn1.macrometa.io"

  #--------------------------------------------------------------
  print("publish messages to stream...")
  client = C8Client(protocol='https', host=region, port=443)
  tenant = client.tenant(email="user@example.com", password="hidden")
  fabric = tenant.useFabric('demo-fabric')  stream = fabric.stream()
  producer = stream.create_producer("demostream", local=False)
  for i in range(10):
      msg = "Hello from " + region + "("+ str(i) +")"
      producer.send(msg.encode('utf-8'))
      time.sleep(10) #sec



Example to **subscribe** documents from a stream:

.. testcode::

   from c8 import C8Client
   import warnings
   warnings.filterwarnings("ignore")

   region = "gdn1.macrometa.io"

   #--------------------------------------------------------------
   print("consume messages from stream...")
   client = C8Client(protocol='https', host=region, port=443)
  tenant = client.tenant(email="user@example.com", password="hidden")
  fabric = tenant.useFabric('demo-fabric')   stream_collection = fabric.stream()
   subscriber = stream_collection.subscribe("demostream",local=False, subscription_name="demosub", consumer_type= stream_collection.CONSUMER_TYPES.EXCLUSIVE)
   #you can subscribe using consumer_types option.
   for i in range(10):
       msg = subscriber.receive()
       print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
       subscriber.acknowledge(msg)



Example: **stream management**:

.. testcode::

    #get_stream_stats
    stream_collection.get_stream_stats('demostream', local=False) #for global persistent stream

    #Skip all messages on a stream subscription
    stream_collection.skip_all_messages_for_subscription('demostream', 'demosub')

    #Skip num messages on a topic subscription
    stream_collection.skip_messages_for_subscription('demostream', 'demosub', 10)

    #Expire messages for a given subscription of a stream.
    #expire time is in seconds
    stream_collection.expire_messages_for_subscription('demostream', 'demosub', 2)

    #Expire messages on all subscriptions of stream
    stream_collection.expire_messages_for_subscriptions('demostream',2)

    #Reset subscription to message position to closest timestamp
    #time is in milli-seconds
    stream_collection.reset_message_subscription_by_timestamp('demostream','demosub', 5)

    #Reset subscription to message position closest to given position
    #stream_collection.reset_message_for_subscription('demostream', 'demosub')

    #stream_collection.reset_message_subscription_by_position('demostream','demosub', 4)

    #Unsubscribes the given subscription on all streams on a stream fabric
    stream_collection.unsubscribe('demosub')

    #delete subscription of a stream
    #stream_collection.delete_stream_subscription('demostream', 'demosub' , local=False)

Workflow of **Spot Collections**

.. testcode::

    from c8 import C8Client

    # Initialize the client for C8DB.
    client = C8Client(protocol='http', host='localhost', port=8529)

    #Step 1: Make one of the regions in the fed as the Spot Region
    # Connect to System admin
    sys_tenant = client.tenant(email=macrometa-admin,  password='hidden')
    #Make REGION-1 as spot-region
    sys_tenant.assign_dc_spot('REGION-1',spot_region=True)

    #Make REGION-2 as spot-region
    sys_tenant.assign_dc_spot('REGION-2',spot_region=True)

    #Step 2: Create a geo-fabric and pass one of the spot regions. You can use the SPOT_CREATION_TYPES for the same. If you use AUTOMATIC, a random spot region will be assigned by the system.
    # If you specify None, a geo-fabric is created without the spot properties. If you specify spot region,pass the corresponding spot region in the spot_dc parameter.
    dcl = sys_tenant.dclist(detail=False)
    tenant = client.tenant(email="user@example.com", password="hidden")
    fabric = tenant.useFabric('demo-fabric')    fabric.create_fabric('spot-geo-fabric', dclist=dcl,spot_creation_type= fabric.SPOT_CREATION_TYPES.SPOT_REGION, spot_dc='REGION-1')

    #Step 3: Create spot collection in 'spot-geo-fabric'
    spot_collection = fabric.create_collection('spot-collection', spot_collection=True)

    #Step 4: Update Spot primary region of the geo-fabric. To change it, we need system admin credentials
    sys_fabric = sys_tenant.useFabric('_system')
    sys_fabric.update_spot_region('mytenant', 'spot-geo-fabric', 'REGION-2')

Example for **restql** operations:

.. testcode::
  from c8 import C8Client
  import json
  import warnings
  warnings.filterwarnings("ignore")

  client = C8Client(protocol='https', host=region, port=443)
  tenant = client.tenant(email="user@example.com", password="hidden")
  fabric = tenant.useFabric('demo-fabric')                   
  #--------------------------------------------------------------
  print("save restql...")
  data = {
    "query": {
      "parameter": {},
      "name": "demo",
      "value": "FOR employee IN employees RETURN employee"
    }
  }
  response = fabric.save_restql(data)
  #--------------------------------------------------------------
  print("execute restql without bindVars...")
  response = fabric.execute_restql("demo")
  #--------------------------------------------------------------
  print("execute restql with bindVars...")
  response = fabric.execute_restql("demo",
                                   {"bindVars": {"name": "mytenant.root"}})
  #--------------------------------------------------------------
  print("get all restql...")
  response = fabric.get_all_restql()
  #--------------------------------------------------------------
  print("update restql...")
  data = {
    "query": {
      "parameter": {},
      "value": "FOR employee IN employees Filter doc.name=@name RETURN employee"
    }
  }
  response = fabric.update_restql("demo", data)
  #--------------------------------------------------------------
  print("delete restql...")
  response = fabric.delete_restql("demo")
