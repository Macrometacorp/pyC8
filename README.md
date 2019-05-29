# PyC8

Welcome to the GitHub page for **pyC8**, a Python driver for the Digital Edge Fabric.

### Features


- Clean Pythonic interface.
- Lightweight.

### Compatibility

- Python versions 3.4, 3.5 and 3.6 are supported.

### Build & Install

To build,

```bash
 $ python setup.py build
```
To install locally,

```bash
 $ python setup.py build
```

### Getting Started

Here is an overview example:

```python
   from c8 import C8Client
   import time
   import warnings
   warnings.filterwarnings("ignore")

   region = "qa1-us-east-1.eng2.macrometa.io"
   demo_tenant = "demo"
   demo_fabric = "demofabric"
   demo_user = "demouser"
   demo_collection = "employees"
   demo_stream = "demostream"

   #--------------------------------------------------------------
   print("Create C8Client Connection...")
   client = C8Client(protocol='https', host=region, port=443)

   #--------------------------------------------------------------
   print("Create under demotenant, demofabric, demouser and assign permissions...")
   demotenant = client.tenant(name=demo_tenant, fabricname='_system', username='root', password='demo')

   if not demotenant.has_user(demo_user):
     demotenant.create_user(username=demo_user, password='demouser', active=True)

   if not demotenant.has_fabric(demo_fabric):
     demotenant.create_fabric(name=demo_fabric, dclist=demotenant.dclist())

   demotenant.update_permission(username=demo_user, permission='rw', fabric=demo_fabric)

   #--------------------------------------------------------------
   print("Create and populate employees collection in demofabric...")
   fabric = client.fabric(tenant=demo_tenant, name=demo_fabric, username=demo_user, password='demouser')
   #get fabric detail
   fabric.fabrics_detail()
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

```

Example to **query** a given fabric:

```python

  from c8 import C8Client
  import json
  import warnings
  warnings.filterwarnings("ignore")

  region = "qa1-us-east-1.ops.aws.macrometa.io"

  #--------------------------------------------------------------
  print("query employees collection...")
  client = C8Client(protocol='https', host=region, port=443)
  fabric = client.fabric(tenant="demotenant", name="demofabric", username="demouser", password='poweruser')
  cursor = fabric.c8ql.execute('FOR employee IN employees RETURN employee') # Execute a C8QL query
  docs = [document for document in cursor]
  print(docs)

```

Example for **real-time updates** from a collection in fabric:

```python

  from c8 import C8Client
  import warnings
  warnings.filterwarnings("ignore")

  region = "qa1-us-east-1.ops.aws.macrometa.io"

  def callback_fn(event):
      print(event)

  #--------------------------------------------------------------
  print("Subscribe to employees collection...")
  client = C8Client(protocol='https', host=region, port=443)
  fabric = client.fabric(tenant="demotenant", name="demofabric", username="demouser", password='poweruser')
  fabric.on_change("employees", callback=callback_fn)

```

Example to **publish** documents to a stream:

```python

  from c8 import C8Client
  import time
  import warnings
  warnings.filterwarnings("ignore")

  region = "qa1-us-east-1.ops.aws.macrometa.io"

  #--------------------------------------------------------------
  print("publish messages to stream...")
  client = C8Client(protocol='https', host=region, port=443)
  fabric = client.fabric(tenant="demotenant", name="demofabric", username="demouser", password='poweruser')
  stream = fabric.stream()
  producer = stream.create_producer("demostream", local=False)
  for i in range(10):
      msg = "Hello from " + region + "("+ str(i) +")"
      producer.send(msg.encode('utf-8'))
      time.sleep(10) # 10 sec

```

Example to **subscribe** documents from a stream:

```python

   from c8 import C8Client
   import warnings
   warnings.filterwarnings("ignore")

   region = "qa1-us-east-1.ops.aws.macrometa.io"

   #--------------------------------------------------------------
   print("consume messages from stream...")
   client = C8Client(protocol='https', host=region, port=443)
   fabric = client.fabric(tenant="demotenant", name="demofabric", username="demouser", password='poweruser')
   stream = fabric.stream()
   #you can subscribe using consumer_types option.
   subscriber = stream.subscribe("demostream", local=False, subscription_name="demosub", consumer_type= stream.CONSUMER_TYPES.EXCLUSIVE)
   for i in range(10):
       msg = subscriber.receive()
       print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
       subscriber.acknowledge(msg)

```

Example: **stream management**:

```python

    stream_collection = fabric.stream()
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

    #trigger compaction status
    stream_collection.put_stream_compaction_status('demostream')

    #get stream compaction status
    stream_collection.get_stream_compaction_status('demostream')

    #Unsubscribes the given subscription on all streams on a stream fabric
    stream_collection.unsubscribe('demosub')

    #delete subscription of a stream
    #stream_collection.delete_stream_subscription('demostream', 'demosub' , local=False)

```

Workflow of **Spot Collections**

```python

from c8 import C8Client

# Initialize the client for C8DB.
client = C8Client(protocol='http', host='localhost', port=8529)

#Step 1: Make one of the regions in the fed as the Spot Region
# Connect to System admin
sys_tenant = client.tenant(name=macrometa-admin, fabricname='_system', username='root', password=macrometa-password)
#Make REGION-1 as spot-region
sys_tenant.assign_dc_spot('REGION-1',spot_region=True)

#Make REGION-2 as spot-region
sys_tenant.assign_dc_spot('REGION-2',spot_region=True)

#Step 2: Create a geo-fabric and pass one of the spot regions. You can use the SPOT_CREATION_TYPES for the same. If you use AUTOMATIC, a random spot region will be assigned by the system.
# If you specify None, a geo-fabric is created without the spot properties. If you specify spot region,pass the corresponding spot region in the spot_dc parameter.
dcl = sys_tenant.dclist()
fabric = client.fabric(tenant='guest', name='_system', username='root', password='guest')
fabric.create_fabric('spot-geo-fabric', dclist=dcl,spot_creation_type= fabric.SPOT_CREATION_TYPES.SPOT_REGION, spot_dc='REGION-1')

#Step 3: Create spot collection in 'spot-geo-fabric'
spot_collection = fabric.create_collection('spot-collection', spot_collection=True)

#Step 4: Update Spot primary region of the geo-fabric. To change it, we need system admin credentials
sys_fabric = client.fabric(tenant=macrometa-admin, name='_system', username='root', password=macrometa-password)
sys_fabric.update_spot_region('guest', 'spot-geo-fabric', 'REGION-2')

```

Example for **restql** operations:

``` python
  from c8 import C8Client
  import json
  import warnings
  warnings.filterwarnings("ignore")

  client = C8Client(protocol='https', host=region, port=443)
  demotenant = client.tenant(name="demo_tenant", fabricname='_system',
                             username='root', password='demo')
  #--------------------------------------------------------------
  print("save restql...")
  data = {
    "query": {
      "parameter": {},
      "name": "demo",
      "value": "FOR employee IN employees RETURN employee"
    }
  }
  response = demotenant.save_restql(data)
  #--------------------------------------------------------------
  print("execute restql without bindVars...")
  response = demotenant.execute_restql("demo")
  #--------------------------------------------------------------
  print("execute restql with bindVars...")
  response = demotenant.execute_restql("demo",
                                       {"bindVars": {"name": "guest.root"}})
  #--------------------------------------------------------------
  print("get all restql...")
  response = demotenant.get_all_restql()
  #--------------------------------------------------------------
  print("update restql...")
  data = {
    "query": {
      "parameter": {},
      "value": "FOR employee IN employees Filter doc.name=@name RETURN employee"
    }
  }
  response = demotenant.update_restql("demo", data)
  #--------------------------------------------------------------
  print("delete restql...")
  response = demotenant.delete_restql("demo")
```
