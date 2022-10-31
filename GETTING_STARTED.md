# Getting Started

The SDK supports three ways for authentication:

1. Using the email and password:

```python
  # Authentication email and password
  client = C8Client(protocol='https', host='gdn.macrometa.io', port=443,
   email="user@example.com", password="XXXXX")
```

2. Using bearer token:

```python
# Authentication with bearer token
client = C8Client(protocol='https', host='gdn.macrometa.io', port=443,
 token=<your token>)
```

3. Using API key:

```python
# Authentication with API key
client = C8Client(protocol='https', host='gdn.macrometa.io', port=443,
 apikey=<your api key>)
```

```python
from c8 import C8Client
import time
import warnings

warnings.filterwarnings("ignore")
federation = "gdn.paas.macrometa.io"
demo_tenant = "mytenant@example.com"
demo_fabric = "_system"
demo_user = "user@example.com"
demo_user_name = "root"
demo_stream = "demostream"
collname = "employees"
# --------------------------------------------------------------
print("Create C8Client Connection...")
client = C8Client(protocol='https', host=federation, port=443,
                  email=demo_tenant, password='XXXXX',
                  geofabric=demo_fabric)

# --------------------------------------------------------------
print("Create Collection and insert documents")

if client.has_collection(collname):
    print("Collection exists")
else:
    client.create_collection(name=collname)

# List all Collections
coll_list = client.get_collections()
print(coll_list)

# Filter collection based on collection models DOC/KV/DYNAMO
colls = client.get_collections(collection_model='DOC')
print(colls)

# Get Collecion Handle and Insert
coll = client.get_collection(collname)
coll.insert(
    {'firstname': 'John', 'lastname': 'Berley', 'email': 'john.berley@macrometa.io'})

# insert document
client.insert_document(collection_name=collname,
                       document={'firstname': 'Jean', 'lastname': 'Picard',
                                 'email': 'jean.picard@macrometa.io'})
doc = client.get_document(collname, "John")
print(doc)

# insert multiple documents
docs = [
    {'firstname': 'James', 'lastname': 'Kirk', 'email': 'james.kirk@macrometa.io'},
    {'firstname': 'Han', 'lastname': 'Solo', 'email': 'han.solo@macrometa.io'},
    {'firstname': 'Bruce', 'lastname': 'Wayne', 'email': 'bruce.wayne@macrometa.io'}
]

client.insert_document(collection_name=collname, document=docs)

# insert documents from file
client.insert_document_from_file(collection_name=collname,
                                 csv_filepath="/home/user/test.csv")

# Add a hash Index
client.add_hash_index(collname, fields=['email'], unique=True)

# --------------------------------------------------------------

# Query a collection
print("query employees collection...")
query = 'FOR employee IN employees RETURN employee'
cursor = client.execute_query(query)
docs = [document for document in cursor]
print(docs)

# --------------------------------------------------------------
print("Delete Collection...")
# Delete Collection
client.delete_collection(name=collname)
```

## Returning all data records inside the collection via batches
The Macrometa GDN has a default limit on how many documents can be returned per query. Usually, the default limit is 1,000 documents per query (This default limit is subject to changes).
The following method should be used to retrieve data from a collection in batches despite this default limit of 1,000 documents per query.

You need to specify the collection name in the `collection_name` parameter
```python
client.get_all_documents(collection_name="employees")
```

## Returning all data records returned by any query via batches
The Macrometa GDN has a default limit on how many documents can be returned per query. Usually, the default limit is 1,000 documents per query (This default limit is subject to changes). 
The following method should be used to retrieve all the data from any query via batches despite this default limit of 1,000 documents per query.

You need to specify your query in the `query` parameter
```python
client.get_all_batches(query="FOR doc IN employees FILTER doc.email LIKE '%macrometa.io' RETURN doc")
```

# Query Workers
A query worker is set of named, parameterized C8QL queries stored in GDN that you can run from a dedicated REST endpoint. The query worker will be created automatically globally and is available from the region closest to the user. Refer [Query Workers](https://macrometa.com/docs/queryworkers/query-workers)

## Operations for query workers -

### Create a query worker
In the following example a query worker named `insertRecord` is created which inserts documents into the **employees** collection.
```python
# Query to be saved with bind variable `docs`
insert_query = "FOR doc in @docs INSERT {'firstname': doc.firstname, 'lastname': doc.lastname, 'email': doc.email, 'zipcode': doc.zipcode, _key': doc._key} IN employees"

# Query worker object with name insertRecord
insert_data = {
    "query": {
        "name": "insertRecord",
        "value": insert_data_query,
    }
}

# Create query worker `insertRecord`
client.create_restql(data=insert_data)
```

### Import query workers
In the following example query workers named `getRecords` and `updateRecord` are created.
```python
get_data_query = "FOR doc IN employees RETURN doc"
update_data_query = "FOR doc IN employees FILTER doc.firstname == 'James' UPDATE doc with { zipcode: '33333' } IN employees"

# Query worker objects with names, getRecords and updateRecord
get_data = {"name": "getRecords", "value": get_data_query}
update_data = {"name": "updateRecord", "value": update_data_query}
queries = [get_data, update_data]

# Import query workers `getRecords` and `updateRecord`
client.import_restql(queries=queries)
```

### Execute a query worker
In the following example an already created query worker `insertRecord` is executed with bind variable `docs`.
```python
docs = [
    {
        "_key": "James",
        "firstname": "James",
        "lastname": "Kirk",
        "email": "james.kirk@macrometa.io",
        "zipcode": "22222",
    },
    {
        "_key": "John",
        "firstname": "John",
        "lastname": "Doe",
        "email": "john.doe@macrometa.io",
        "zipcode": "11111",
    }
]

# Execute query worker `insertRecord` with bind variable `docs`
client.execute_restql(name="insertRecord", data={"bindVars": {"docs": docs}})
```

### Execute query worker in batches and read next batch
Just like queries, query worker can also be executed in batches. In the following example query worker `getRecords` is executed with batch size equal to 2.
```python
# Execute query worker `getRecords` with batch size equal to 2
resp = client.execute_restql(name="getRecords", data={"bindVars": {}, "batchSize": 2})
# Get cursor id
id = resp["id"]
# Read next batch from cursor
client.read_next_batch_restql(id=id)
```

### Retrieve query workers
All existing query workers can be viewed.
```python
client.get_restqls()
```

### Update a query worker
In the following example `insertRecord` query worker is updated with a new query.
```python
updated_insert_query = "INSERT {'_key': 'barry.allen@macrometa.io', 'firstname': 'Barry', 'lastname': 'Allen'} IN employees"

# Query worker object
updated_insert_data = {
    "query": {
        "value": updated_insert_query,
    }
}

# Update query worker `insertRecord`
client.update_restql(name="insertRecord", data=updated_insert_data)
```

### Delete a query worker
In the following example query worker, `insertRecord` is deleted.
```python
client.delete_restql(name="insertRecord")
```

## Truncate a collection
All the data records inside the collection can be removed by truncating the collection. In the following example all the data inside **employees** collection is removed.
```python
col = client.collection(collection_name="employees")
col.truncate()
```

## Get real-time updates from a collection
Example to get real-time updates from **employees** collection:
:::note
Enable the 'Stream' parameter within the collection to get real-time updates if not already enabled
:::
```python
def callback_fn(event):
    print(event)

# Get real-time updates from collection
client.on_change(collection="employees", callback=callback_fn)
```

Stream can be enabled for a collection via any of the following:
1. While creating the collection itelf by passing the stream parameter as true:
```python
client.create_collection(name="employees", sync=False, edge=False, local_collection=False, stream=True)
```

Or

2. By updating the collection `properties` with `has_stream` as True after the collection is already created:
```python
client.update_collection_properties(collection_name="employees", has_stream=True)
```

## Delete a collection
In the following example, **employees** collection is deleted.
```python
client.delete_collection(name="employees")
```

# Streams
Streams are flows of data in GDN to capture data in motion. Messages are sent via streams by publishers to consumers who then do something with the message. Refer [Streams](https://macrometa.com/docs/streams/)

## Basic operations for streams -

### Create a stream
In the following example a stream named `quickStart` is created globally (as local is set to False). 
```python
client.create_stream(stream="quickStart", local=False)
```

### Retrieve all streams
All existing streams can be viewed.
```python
client.get_streams()
```

### Create a producer to send a message
In the following example, a [producer](https://macrometa.com/docs/streams/producers) is created for `global` stream named `quickStart`.
```python
# Create producer
producer = client.create_stream_producer(stream="quickStart", local=False)
msg = "Hello from user"
producer.send(msg)
# To close producer connection use `producer.close()`
```

### Create a subscriber to receive message
In the following example, a [subscriber](https://macrometa.com/docs/streams/consumers) with [subscription](https://macrometa.com/docs/streams/subscriptions) name `topic_1` is created for `global` stream named `quickStart`.
```python
# Create subscriber
subscriber = client.subscribe(
    stream="quickStart", local=False, subscription_name="topic_1"
)
# Subscriber reading message
m1 = json.loads(subscriber.recv())
# Decoding the received message
msg1 = base64.b64decode(m1["payload"]).decode("utf-8")
# Acknowledge the received msg.
subscriber.send(
    json.dumps({"messageId": m1["messageId"]})
)
# To close subscriber connection use `subscriber.close()`
```
A complete pub-sub example can be found [here](https://macrometa.com/docs/streams/pub-sub-streams)

### Delete a subscription from a particular stream
In the following example, a [subscription](https://macrometa.com/docs/streams/subscriptions) named `topic_1` is removed from a `global` stream named `quickStart` (set local parameter as false if the stream is local).
:::note
In case the stream is a local stream:
Append `c8locals.` instead of `c8globals.` to the stream name.
Set local parameter as true.
:::
```python
client.delete_stream_subscription(stream="c8globals.quickStart", subscription="topic_1", local=False)
```

### Delete a subscription from all streams
In the following example, a [subscription](https://macrometa.com/docs/streams/subscriptions) named `topic_1` is removed from all `global` streams (set local parameter as true to remove subscription from all local streams).
```python
client.unsubscribe(subscription="topic_1", local=False)
```

### Delete a stream
In the following example a `global` stream named `quickStart` is deleted.
:::note
Append `c8locals.` instead of `c8globals.` to the stream name in case the stream is a local stream.
:::
```python
client.delete_stream(stream="c8globals.quickStart")
```

# Stream Workers
Macrometa GDN allows you to integrate streaming data and take appropriate actions. Most stream processing use cases involve collecting, analyzing, and integrating or acting on data generated during business activities by various sources.
Refer [Stream Workers](https://macrometa.com/docs/cep/) and [Stream Worker Query Guide](https://macrometa.com/docs/cep/query-guide/)

## Basic operations for stream workers -

### Validate a stream worker
Validate the stream worker for syntax errors before saving.
To understand more about the stream worker definition please refer to the [Stream Worker Query Guide](https://macrometa.com/docs/cep/query-guide/)

In the following example a stream worker named `Sample-Cargo-App` with the following definition is validated.
```python
stream_app_definition = """
    @App:name('Sample-Cargo-App')
    @App:qlVersion("2")
    @App:description('Basic stream application to demonstrate reading data from input stream and store it in the collection. The stream and collections are automatically created if they do not already exist.')
    /**
    Testing the Stream Application:
        1. Open Stream SampleCargoAppDestStream in Console. The output can be monitored here.
        2. Upload following data into SampleCargoAppInputTable C8DB Collection
            {"weight": 1}
            {"weight": 2}
            {"weight": 3}
            {"weight": 4}
            {"weight": 5}
        3. Following messages would be shown on the SampleCargoAppDestStream Stream Console
            [2021-08-27T14:12:15.795Z] {"weight":1}
            [2021-08-27T14:12:15.799Z] {"weight":2}
            [2021-08-27T14:12:15.805Z] {"weight":3}
            [2021-08-27T14:12:15.809Z] {"weight":4}
            [2021-08-27T14:12:15.814Z] {"weight":5}
    */

    -- Create Table SampleCargoAppInputTable to process events.
    CREATE SOURCE SampleCargoAppInputTable WITH (type = 'database', collection = "SampleCargoAppInputTable", collection.type="doc", replication.type="global", map.type='json') (weight int);

    -- Create Stream SampleCargoAppDestStream
    CREATE SINK SampleCargoAppDestStream WITH (type = 'stream', stream = "SampleCargoAppDestStream", replication.type="local") (weight int);

    -- Data Processing
    @info(name='Query')
    INSERT INTO SampleCargoAppDestStream
    SELECT weight
    FROM SampleCargoAppInputTable;
    """
# Validate a stream application
client.validate_stream_app(data=stream_app_definition)
```

### Create a stream worker
Create the stream worker with the specified stream worker defintion on the specified list of regions. If list of regions is not specified then the stream worker is created on the nearest region.

In the following example a stream worker named `Sample-Cargo-App` with the definition specified in the validate stream worker example is created.
```python
regions = []
client.create_stream_app(data=stream_app_definition, dclist=regions)
```

### Enable or Disable a stream worker
Enable/Disable an already created specified stream worker.
In the following example a stream worker named `Sample-Cargo-App` w is first enabled then disabled.
```python
# Activate a stream application
client.activate_stream_app(streamapp_name="Sample-Cargo-App", activate=True)
# Deactivate a stream application
client.activate_stream_app(streamapp_name="Sample-Cargo-App", activate=False)
```

### Retrieve stream workers
Lists all existing stream workers.
```python
client.retrieve_stream_app()
```

### Update a stream worker
An already existing stream worker can be updated with a new definition on the specified list of regions. If list of regions is not specified then the stream worker is created on the nearest region. Once updated you can enable/disbale the stream worker again as you want.

In the following example we update the definition of an existing stream worker `Sample-Cargo-App` with the following and activate it.
```python
# To operate on created apps, you need to create an instance of the app
app = client._fabric.stream_app("Sample-Cargo-App")

# Update the app using
updated_definition = """
@App:name('Sample-Cargo-App')
@App:qlVersion("2")
@App:description('Basic stream application to demonstrate reading data from input stream and store it in the collection. Thestream and collections are automatically created if they do not already exist.')
/**
    Testing the Stream Application:
    1. Open Stream SampleCargoAppDestStream in Console. The output can be monitored here.
    2. Upload following data into SampleCargoAppInputTable C8DB Collection
        {"weight": 1}
        {"weight": 2}
        {"weight": 3}
        {"weight": 4}
        {"weight": 5}
    3. Following messages would be shown on the `SampleCargoAppDestStream` Stream Console.
        [2021-08-27T14:12:15.795Z] {"weight":1}
        [2021-08-27T14:12:15.799Z] {"weight":2}
        [2021-08-27T14:12:15.805Z] {"weight":3}
        [2021-08-27T14:12:15.809Z] {"weight":4}
        [2021-08-27T14:12:15.814Z] {"weight":5}
    4. Following messages would be stored into SampleCargoAppDestTable
        {"weight":1}
        {"weight":2}
        {"weight":3}
        {"weight":4}
        {"weight":5}
*/
-- Defines Table SampleCargoAppInputTable
CREATE SOURCE SampleCargoAppInputTable WITH (type = 'database', collection = "SampleCargoAppInputTable", collection.type="doc",replication.type="global", map.type='json') (weight int);

-- Define Stream SampleCargoAppDestStream
CREATE SINK SampleCargoAppDestStream WITH (type = 'stream', stream = "SampleCargoAppDestStream", replication.type="local") (weight int);
-- Defining a Destination table to dump the data from the stream
CREATE STORE SampleCargoAppDestTable WITH (type = 'database', stream = "SampleCargoAppDestTable") (weight int);

-- Data Processing
@info(name='Query')
INSERT INTO SampleCargoAppDestStream
SELECT weight
FROM SampleCargoAppInputTable;

-- Data Processing
@info(name='Dump')
INSERT INTO SampleCargoAppDestTable
SELECT weight
FROM SampleCargoAppInputTable;
"""

regions = []
app.update(data=updated_definition, dclist=regions)
# Update needs some wait time
time.sleep(5)
# Activate the updated stream worker
app.change_state(active=True)
```

### Get sample stream workers
Get the detailed list of smaple stream workers.
```python
# Get stream application samples
client.get_stream_app_samples()
```

### Delete a stream worker
In the following example, stream worker `Sample-Cargo-App` is deleted.
```python
# Delete a stream application
client.delete_stream_app(streamapp_name="Sample-Cargo-App")
```
