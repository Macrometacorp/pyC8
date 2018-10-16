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

   region = "qa1-us-east-1.ops.aws.macrometa.io"
   demo_tenant = "demotenant"
   demo_db = "demodb"
   demo_user = "demouser"
   demo_collection = "employees"
   demo_stream = "demostream"

   #--------------------------------------------------------------
   print("Create demo tenant...")
   client = C8Client(protocol='https', host=region, port=443)
   sys_tenant = client.tenant(name='_mm', dbname='_system', username='root', password='poweruser')

   if not sys_tenant.has_tenant(demo_tenant):
       sys_tenant.create_tenant(demo_tenant, passwd="poweruser")

   #--------------------------------------------------------------
   print("Create under demotenant, demodb, demouser and assign permissions...")
   demotenant = client.tenant(name=demo_tenant, dbname='_system', username='root', password='poweruser')

   if not demotenant.has_user(demo_user):
     demotenant.create_user(username=demo_user, password='demouser', active=True)

   if not demotenant.has_database(demo_db):
     demotenant.create_database(name=demo_db, dclist=demotenant.dclist(), realtime=True)

   demotenant.update_permission(username=demo_user, permission='rw', database=demo_db)

   #--------------------------------------------------------------
   print("Create and populate employees collection in demodb...")
   db = client.db(tenant=demo_tenant, name=demo_db, username=demo_user, password='demouser')
   employees = db.create_collection('employees') # Create a new collection named "employees".
   employees.add_hash_index(fields=['email'], unique=True) # Add a hash index to the collection.

   employees.insert({'firstname': 'Jean', 'lastname':'Picard', 'email':'jean.picard@macrometa.io'})
   employees.insert({'firstname': 'James', 'lastname':'Kirk', 'email':'james.kirk@macrometa.io'})
   employees.insert({'firstname': 'Han', 'lastname':'Solo', 'email':'han.solo@macrometa.io'})
   employees.insert({'firstname': 'Bruce', 'lastname':'Wayne', 'email':'bruce.wayne@macrometa.io'})

   #--------------------------------------------------------------
   print("query employees collection...")
   cursor = db.c8ql.execute('FOR employee IN employees RETURN employee') # Execute a C8QL query
   docs = [document for document in cursor]
   print(docs)

   #--------------------------------------------------------------
   print("Create global & local streams in demodb...")
   db.create_stream(demo_stream, persistent=True, local=False)
   db.create_stream(demo_stream, persistent=True, local=True)
   db.create_stream(demo_stream, persistent=False, local=False)
   db.create_stream(demo_stream, persistent=False, local=True)
   streams = db.streams()
   print("streams:", streams)

   #--------------------------------------------------------------
   #sys_tenant.delete_tenant(demo_tenant)

```

Example to **query** a given database:

```python

  from c8 import C8Client
  import json
  import warnings
  warnings.filterwarnings("ignore")

  region = "qa1-us-east-1.ops.aws.macrometa.io"

  #--------------------------------------------------------------
  print("query employees collection...")
  client = C8Client(protocol='https', host=region, port=443)
  db = client.db(tenant="demotenant", name="demodb", username="demouser", password='poweruser')
  cursor = db.c8ql.execute('FOR employee IN employees RETURN employee') # Execute a C8QL query
  docs = [document for document in cursor]
  print(docs)

```

Example for **real-time updates** from a collection in database:

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
  db = client.db(tenant="demotenant", name="demodb", username="demouser", password='poweruser')
  db.on_change(collection="employees", callback=callback_fn)
  
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
  db = client.db(tenant="demotenant", name="demodb", username="demouser", password='poweruser')
  stream = db.stream()
  producer = stream.create_producer(collection="demostream", persistent=True, local=False)
  for i in range(10):
      msg = "Hello from " + region + "("+ str(i) +")"
      producer.send(msg.encode('utf-8'))
      time.sleep(10) #sec
    
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
   db = client.db(tenant="demotenant", name="demodb", username="demouser", password='poweruser')
   stream = db.stream()
   subscriber = stream.subscribe(collection="demostream", persistent=True, local=False, subscription_name="demosub")
   for i in range(10):
       msg = subscriber.receive()
       print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
       subscriber.acknowledge(msg)
    
```

Example: **stream management**:

```python

    #get_stream_stats
    stream_collection.get_stream_stats('demostream', persistent=True, local=False) #for global persistent stream

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

    #Unsubscribes the given subscription on all streams on a stream db
    stream_collection.unsubscribe('demosub')

    #delete subscription of a stream
    #stream_collection.delete_stream_subscription('demostream', 'demosub' ,persistent=True, local=False)

```

Here is another example with **graphs**:

```python

    from c8 import C8Client

    # Initialize the client for C8DB.
    client = C8Client(protocol='http', host='localhost', port=8529)

    # Connect to "test" database as root user.
    db = client.db('test', username='root', password='passwd')

    # Create a new graph named "school".
    graph = db.create_graph('school')

    # Create vertex collections for the graph.
    students = graph.create_vertex_collection('students')
    lectures = graph.create_vertex_collection('lectures')

    # Create an edge definition (relation) for the graph.
    register = graph.create_edge_definition(
        edge_collection='register',
        from_vertex_collections=['students'],
        to_vertex_collections=['lectures']
    )

    # Insert vertex documents into "students" (from) vertex collection.
    students.insert({'_key': '01', 'full_name': 'Anna Smith'})
    students.insert({'_key': '02', 'full_name': 'Jake Clark'})
    students.insert({'_key': '03', 'full_name': 'Lisa Jones'})

    # Insert vertex documents into "lectures" (to) vertex collection.
    lectures.insert({'_key': 'MAT101', 'title': 'Calculus'})
    lectures.insert({'_key': 'STA101', 'title': 'Statistics'})
    lectures.insert({'_key': 'CSC101', 'title': 'Algorithms'})

    # Insert edge documents into "register" edge collection.
    register.insert({'_from': 'students/01', '_to': 'lectures/MAT101'})
    register.insert({'_from': 'students/01', '_to': 'lectures/STA101'})
    register.insert({'_from': 'students/01', '_to': 'lectures/CSC101'})
    register.insert({'_from': 'students/02', '_to': 'lectures/MAT101'})
    register.insert({'_from': 'students/02', '_to': 'lectures/STA101'})
    register.insert({'_from': 'students/03', '_to': 'lectures/CSC101'})

    # Traverse the graph in outbound direction, breadth-first.
    result = graph.traverse(
        start_vertex='students/01',
        direction='outbound',
        strategy='breadthfirst'
    )
```