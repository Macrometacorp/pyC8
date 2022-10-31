# Getting Started

## Auth - Connect to GDN
The first step to start using GDN is establishing a connection to [gdn.paas.macrometa.io](https://gdn.paas.macrometa.io/). When this code executes, it initializes the server connection to your nearest region. You can access your Macrometa GDN account using several methods, such as [API keys](https://macrometa.com/docs/account-management/api-keys/), [User authentication](https://macrometa.com/docs/account-management/auth/user-auth), [Token-based authentication (JWT)](https://macrometa.com/docs/account-management/auth/jwts). For this example the method used is API key.

```python
from c8 import C8Client
client = C8Client(protocol='https', host='gdn.paas.macrometa.io', port=443, apikey='your api key')
```
## Create a collection
A [Collection](https://macrometa.com/docs/collections/) consists of documents, and they can be local(only stores data in one region) or global(replicates data and maintains consistency across regions). Macrometa offers several types of collections such as [Document Store](https://macrometa.com/docs/collections/documents/), [Key-Value Store](https://macrometa.com/docs/collections/keyvalue/), [Dynamo Table](https://macrometa.com/docs/collections/dynamo/create-dynamo-table) and [Graph-Edge collection](https://macrometa.com/docs/collections/graphs/). For this example we are going to create a [Document Store](https://macrometa.com/docs/collections/documents/).

```python
client.create_collection(name='employees', sync=False, edge=False, local_collection=False, stream=False)
```
# CRUD Operations for Document Store collection
A [document collection](https://macrometa.com/docs/collections/documents/) is a NoSQL database that stores data in JSON format (JavaScript Object Notation). Unlike traditional Relational Database Management Systems, document databases do not require a schema or a pre-defined structure with fixed tables and attributes. 
## Insert Document
Documents are stored in [collections](https://macrometa.com/docs/collections/). Documents with completely different structures can be stored in the same collection.

**Inserting documents in 'employees' collection:**

[Add documents to collection](https://macrometa.com/docs/collections/documents/add-document)
```python
docs = [
    {'_key': 'Han', 'firstname': 'Han', 'lastname': 'Solo', 'email': 'han.solo@macrometa.io', 'age': 35,
     'role': 'Manager'},
    {'_key': 'Bruce', 'firstname': 'Bruce', 'lastname': 'Wayne', 'email': 'bruce.wayne@macrometa.io', 'age': 40,
     'role': 'Developer', 'phone': '1-999-888-9999'},
    {'_key': 'Jon', 'firstname': 'Jon', 'lastname': 'Doe', 'email': 'jon.doe@macrometa.io', 'age': 25,
     'role': 'Manager'},
    {'_key': 'Zoe', 'firstname': 'Zoe', 'lastname': 'Hazim', 'email': 'zoe.hazim@macrometa.io', 'age': 20,
     'role': 'Director'},
    {'_key': 'Emma', 'firstname': 'Emma', 'lastname': 'Watson', 'email': 'emma.watson@macrometa.io', 'age': 25,
     'role': 'Director'}
]
client.insert_document(collection_name='employees', document=docs)
```

## Read Document
This example demonstrates how to retrieve a particular document with its *_key*.

**Retreive an specific document with it's _key value:**
```python
client.get_document(collection='employees', document={'_key': 'Han'})
```

## Update Document
The update operation can partially update a document in a collection. The document must contain the attributes and values to be updated and the _key attribute to identify the document to be updated. For this example the email for *Han* will be updated.

```python
client.update_document(collection_name='employees', document={'_key': 'Han', 'email': 'han@updated_macrometa.io'})
```

## Delete Document
The [delete](https://macrometa.com/docs/collections/documents/document-store-data#delete-document) operation is used to remove a document from a collection. For this example *Han* will be removed from the **employees** collection.
```python
client.delete_document(collection_name='employees', document={'_key': 'Han'})
```

## Working with C8QL
Working with data can be complex. CRUD operations usually need more logic or conditions to reach the desired results. At Macrometa the [C8 query language (C8QL)](https://macrometa.com/docs/queryworkers/c8ql/) can be used to create, retrieve, modify and delete data that are stored in the Macrometa geo-distributed fast data platform.
**Check out the [operators](https://macrometa.com/docs/queryworkers/c8ql/) and [examples](https://macrometa.com/docs/queryworkers/c8ql/examples/) in Macrometa**

Let's [FILTER](https://macrometa.com/docs/queryworkers/c8ql/operations/filter) results by **role** and **age** and return a custom object using [ C8QL](https://macrometa.com/docs/queryworkers/c8ql/):
```python
query = "FOR doc IN employees FILTER doc.role == 'Manager' FILTER doc.age > 30 RETURN {'Name':doc.firstname,'Last Name':doc.lastname,'Email':doc.email}"
cursor = client.execute_query(query=query)
docs = [document for document in cursor]
```

It might not always be necessary to return all documents that a FOR loop would normally return. In those cases, you can limit the number of documents with a [LIMIT()](https://macrometa.com/docs/queryworkers/c8ql/operations/limit) operation. For this case, it will be used along with the [SORT()](https://macrometa.com/docs/queryworkers/c8ql/operations/sort) operation.
The first 2 documents are returned with the below query.
```python
query = "FOR doc IN employees SORT doc.age LIMIT 2 RETURN doc"
cursor = client.execute_query(query=query)
docs = [document for document in cursor]
```
Another function usually used with [LIMIT is OFFSET](https://macrometa.com/docs/cep/query-guide/query#limit--offset). The offset value specifies how many elements from the result shall be skipped. It must be 0 or greater. The count value specifies how many elements should be at most included in the result.
```python
query = "FOR doc IN employees SORT doc.age LIMIT 2, 4 RETURN doc"
cursor = client.execute_query(query=query)
docs = [document for document in cursor]
```

[Grouping](https://macrometa.com/docs/queryworkers/c8ql/examples/grouping) results is a common operation when retrieving data. To group results by arbitrary criteria, C8QL provides the [COLLECT](https://macrometa.com/docs/queryworkers/c8ql/operations/collect) keyword. [COLLECT](https://macrometa.com/docs/queryworkers/c8ql/operations/collect) will perform a grouping, but no aggregation. In the following example let's group by **age** and return only the **first name** of the employee using the  expansion operator [*].

```python
query = "FOR doc IN employees COLLECT age = doc.age INTO employeesByAge RETURN {age, employee: employeesByAge[*].doc.firstname}"
cursor = client.execute_query(query=query)
docs = [document for document in cursor]
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
