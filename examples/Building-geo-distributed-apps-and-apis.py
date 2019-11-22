import time

from c8 import C8Client
import pprint
import random
import threading

# Variables
service_url = "gdn1.macrometa.io" # The request will be automatically routed to closest location.
user_mail = "guest1@macrometa.io"
user_pwd = "guest1"
collection_name = "employees" + str(random.randint(1, 10000))
stream_name = "stream"+ str(random.randint(1, 10000))

print("\n---------------------------\n")
print("Connect to C8 (Macrometa fast data platform)...")
print(service_url)
client = C8Client(protocol='https', host=service_url, port=443) # Automatically routed to nearest region.

print("Get access to geo fabric...")
tenant = client.tenant(user_mail, user_pwd)
fabric = tenant.useFabric("testfabric")
pprint.pprint("Connected to region: {}".format(fabric.localdc(detail=False)))
print("List of regions geo fabric is replicated currently: {}".format(fabric.dclist(detail=False)))

print("\n-----------GEO REPLICATED COLLECTIONs ----------------\n")
# Create a geo-replicated Collection
print("Creating geo-replicated collection...")
employees = fabric.create_collection(collection_name)

print("\n------------ PULL MODEL ---------------\n")
# Insert documents to the collection
print("Inserting documents to the collection...")
employees.insert({'_key':'Jean', 'firstname': 'Jean', 'lastname':'Picard', 'email':'jean.picard@macrometa.io'})
employees.insert({'_key':'James', 'firstname': 'James', 'lastname':'Kirk', 'email':'james.kirk@macrometa.io'})
employees.insert({'_key': 'Han', 'firstname': 'Han', 'lastname':'Solo', 'email':'han.solo@macrometa.io'})

# Retrieve documents from the collection... PULL Model
query_string = "FOR employee IN {} RETURN employee".format(collection_name)
print("Read documents from the collection. Query: {}".format(query_string))
cursor = fabric.c8ql.execute(query_string) # Execute C8QL query
docs = [document for document in cursor]
pprint.pprint(docs)

print("\n------------ PUSH MODEL ---------------\n")
# Receive documents from the collection in realtime... PUSH Model
def create_callback():
    def callback_fn(event):
        print("realtime: {}".format(event))
        return

    fabric.on_change(collection_name, callback=callback_fn)

print("Register callback to get changes in realtime from collection: {}".format(collection_name))
rt_thread = threading.Thread(target=create_callback)
rt_thread.start()
time.sleep(2)
print("Callback registered for collection: {}".format(collection_name))

print("Insert documents to the collection...")
employees.insert({'_key':'John', 'firstname': 'John', 'lastname':'Wayne', 'email':'john.wayne@macrometa.io'})
employees.insert({'_key':'Clark', 'firstname': 'Clark', 'lastname':'Kent', 'email':'clark.kent@macrometa.io'})
employees.insert({'_key': 'Bruce', 'firstname': 'Bruce', 'lastname':'Wayne', 'email':'bruce.wayne@macrometa.io'})

print("Wait to close the callback...")
rt_thread.join()

print("\n------------ GEO REPLICATED STREAMS ---------------\n")
# Geo Stream Operations...
print("Create local & geo-replicated global streams...")
fabric.create_stream(stream_name, local=True) # stream local to the region.
# fabric.create_stream(stream_name, local=False) # geo-replicated stream available in all regions.

print("Get all available local streams...")
streams = fabric.persistent_streams(local=True)
pprint.pprint(streams)

print("\n------------ GLOBAL REAL-TIME STREAMING ---------------\n")

print("Create subscriber to our stream...")
def create_subscriber():
    print("Subscribe to stream: {}".format(stream_name))
    stream = fabric.stream()
    subscriber = stream.subscribe(stream_name, local=True, subscription_name="subscriber1", consumer_type=stream.CONSUMER_TYPES.EXCLUSIVE)
    print("Subscriber receiving documents ...")
    for i in range(10):
        msg = subscriber.receive()
        print("Received...Stream:'{}', Body:'{}' ID:'{}'".format(msg.topic_name(), msg.data(), msg.message_id()))
        subscriber.acknowledge(msg)

subscriber_thread = threading.Thread(target=create_subscriber)
subscriber_thread.start()

print("Create producer on stream: {}".format(stream_name))
stream = fabric.stream()
producer = stream.create_producer(stream_name, local=True)

print("Publish 10 messages to stream...")
for i in range(10):
    msg = "Hello from  user--" +  "("+ str(i) +")"
    producer.send(msg.encode('utf-8'))
producer.close()
print("Publish messages done...")

print("Wait for subscriber to consume all messages...")
subscriber_thread.join() # Wait for subscriber to consume all messages.

print("Subscriber done...All messages received")

print("\n------------ DONE ---------------\n")
