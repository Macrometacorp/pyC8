# pyC8

Welcome to the GitHub page for **pyC8**, a python driver for Macrometa Digital Edge Fabric.

### Features

- Clean Pythonic interface.
- Lightweight.

### Compatibility

- Python versions 3.4, 3.5 and 3.6 are supported.

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

Example to query a given database:

```python

  from c8 import C8Client
  import json
  import warnings
  warnings.filterwarnings("ignore")

  region = "qa1-us-east-1.ops.aws.macrometa.io"

  print("query employees collection...")

  client = C8Client(protocol='https', host=region, port=443)
  db = client.db(tenant="demotenant", name="demodb", username="demouser", password='poweruser')
  cursor = db.c8ql.execute('FOR employee IN employees RETURN employee') # Execute a C8QL query
  docs = [document for document in cursor]
  print(docs)

```

Example for real-time updates from a collection in database:

```python

  from c8 import C8Client
  import warnings
  warnings.filterwarnings("ignore")

  region = "qa1-us-east-1.ops.aws.macrometa.io"

  def callback_fn(event):
      print(event)

  print("Subscribe to employees collection...")

  client = C8Client(protocol='https', host=region, port=443)
  db = client.db(tenant="demotenant", name="demodb", username="demouser", password='poweruser')
  db.on_change(collection="employees", callback=callback_fn)
  
```

Example to publish documents to a stream:

```python

  from c8 import C8Client
  import time
  import warnings
  warnings.filterwarnings("ignore")

  region = "qa1-us-east-1.ops.aws.macrometa.io"

  print("publish messages to stream...")
  client = C8Client(protocol='https', host=region, port=443)
  db = client.db(tenant="demotenant", name="demodb", username="demouser", password='poweruser')
  producer = db.stream().create_producer(collection="demostream", persistent=True, local=False)

  for i in range(10):
      producer.send(b"Hello from " + region + "("+ i +")")
      time.sleep(1) #sec
    
```

Example to subscribe documents from a stream:

```python

   from c8 import C8Client
   import warnings
   warnings.filterwarnings("ignore")

   region = "qa1-us-east-1.ops.aws.macrometa.io"

   print("consume messages from stream...")

   client = C8Client(protocol='https', host=region, port=443)
   db = client.db(tenant="demotenant", name="demodb", username="demouser", password='poweruser')
   subscriber = db.stream().subscribe(collection="demostream", persistent=True, local=False, subscription_name="demosub")
   subscriber.receive()
    
```

Here is another example with graphs:

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

Example for Stream Collections:

```python
    from c8 import C8Client

    # Initialize the client for C8DB.
    client = C8Client(protocol='http', host='localhost', port=8529)

    sys_tenant = client.tenant(name='_mm', dbname='_system', username='root', password='poweruser')

    # Connect to "_system" database as root user.
    sys_db = client.db(name='_system', username='root', password='poweruser')
    
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
```

### Driver Build

To build,

```bash
 $ python setup.py build
```
To install locally,

```bash
 $ python setup.py build
```
