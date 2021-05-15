from c8 import C8Client
import random
import threading
import time
import json
import base64

import six

# Variables
global_url = "sankalp-ap-west.eng.macrometa.io" # The request will be automatically routed to closest location.
email = "mm@macrometa.io"
password = "Macrometa123!@#"
geo_fabric = "_system"
stream_name ="sankalpStream"

def create_subscriber():
    print("\n ------- SUBSCRIBE TOPIC  ------")

    print("Subscribe to stream: {}".format(stream_name))
    subscriber1 = client.subscribe(stream=stream_name, local=False, subscription_name="subscriber1",
    consumer_type=client.CONSUMER_TYPES.EXCLUSIVE)

    #receive: read the published messages over stream.
    for i in range(10):
        response = json.loads(subscriber1.recv())
        msg = base64.b64decode(response["payload"])
        print("Received Message:", msg)
        if response["messageId"]:
            # print("Acknowledging msg: ", response["messageId"])
            subscriber1.send(json.dumps(
                {"payload": base64.b64encode(six.b(
                    response["messageId"])).decode("utf-8")}))

if __name__ == '__main__':

    print("\n ------- CONNECTION SETUP  ------")
    print("user: {}, geofabric:{}".format(email, geo_fabric))
    print("\n1. CONNECT: federation: {},  user: {}".format(global_url, email))
    client = C8Client(protocol='https', host=global_url, port=443,
                    email=email, password=password,
                    geofabric=geo_fabric)

    print("\n ------- CREATE STREAM  (local/geo-replicated) ------")
    #client.create_stream('wstream', local=False)  # set local=False for geo-replicated stream available in all regions.
    #print("Created stream: {}".format(stream_name))
    #time.sleep(10)  # to account for network latencies in replication

    print("\n ------- CREATE SUBSCRIBER  ------")
    subscriber_thread = threading.Thread(target=create_subscriber)
    subscriber_thread.start()

    print("\n ------- CREATE PRODUCER  ------")
    print("Create producer on stream: {}".format(stream_name))
    producer = client.create_stream_producer(stream_name, local=False)
    print(producer.__dict__)
    print("\n ------- PUBLISH MESSAGES  ------")
    print("Publish 10 messages to stream: {}".format(stream_name))
    for i in range(10):
        print(i)
        msg = "Hello from  user--" + "(" + str(i) + ")"
        data = {
                "payload": base64.b64encode(six.b(msg)).decode("utf-8"),
        }
        try:
            producer.send(json.dumps(data))
            response =  json.loads(producer.recv())
            if response['result'] == 'ok':
              print('Message published successfully')
            else:
              print('Failed to publish message:', response)
        except Exception as e:
            m = "Producer failed to send message due to Pulsar Error - %s" % e
            print(m)

    producer.close()
    print("Publish messages done...")

    print("Wait for subscriber to consume all messages...")
    subscriber_thread.join()  # Wait for subscriber to consume all messages.
    print("\n ------- DONE  ------")