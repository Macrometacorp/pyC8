from c8 import C8Client
import random
import threading
import time

# Variables
service_url = "gdn1.macrometa.io" # The request will be automatically routed to closest location.
user_mail = "user@example.com"
user_password = "hidden"
geo_fabric = "testfabric"
stream_name = "stream"+ str(random.randint(1, 10000))

def create_subscriber():
    print("Subscribe to stream: {}".format(stream_name))
    stream = fabric.stream()
    subscriber = stream.subscribe(stream_name, local=True, subscription_name="subscriber1",
                                  consumer_type=stream.CONSUMER_TYPES.EXCLUSIVE)
    for i in range(10):
        msg = subscriber.receive()
        print("Received: Msg_Body:'{}' Msg_ID:'{}'".format(msg.data(), msg.message_id()))
        subscriber.acknowledge(msg)

if __name__ == '__main__':

    print("\n ------- CONNECTION SETUP  ------")
    print("user: {}, geofabric:{}".format(user_mail, geo_fabric))
    client = C8Client(protocol='https', host=service_url, port=443) # Automatically routed to nearest region.
    tenant = client.tenant(user_mail, user_password)
    fabric = tenant.useFabric(geo_fabric)

    print("Availabile regions....")
    dclist = fabric.dclist(detail=True)
    for dc in dclist:
        print(" region: {}".format(dc["name"]))
    print("Connected to closest region...\tregion: {}".format(fabric.localdc(detail=False)))

    print("\n ------- CREATE STREAM  (local/geo-replicated) ------")
    fabric.create_stream(stream_name, local=True)  # set local=False for geo-replicated stream available in all regions.
    print("Created stream: {}".format(stream_name))
    time.sleep(5)  # to account for network latencies in replication

    print("\n ------- CREATE SUBSCRIBER  ------")
    subscriber_thread = threading.Thread(target=create_subscriber)
    subscriber_thread.start()

    print("\n ------- CREATE PRODUCER  ------")
    print("Create producer on stream: {}".format(stream_name))
    producer = fabric.stream().create_producer(stream_name, local=True)
    print(producer.__dict__)
    print("\n ------- PUBLISH MESSAGES  ------")
    print("Publish 10 messages to stream: {}".format(stream_name))
    for i in range(10):
        print(i)
        msg = "Hello from  user--" + "(" + str(i) + ")"
        try:
            producer.send(msg.encode("utf-8"))
        except Exception as e:
            m = "Producer failed to send message due to Pulsar Error - %s" % e
            print(m)
            
    producer.close()
    print("Publish messages done...")

    print("Wait for subscriber to consume all messages...")
    subscriber_thread.join()  # Wait for subscriber to consume all messages.
    print("\n ------- DONE  ------")
