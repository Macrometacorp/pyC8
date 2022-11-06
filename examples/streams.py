import base64
import json

from c8 import C8Client

# Creating client instance with API KEY
client = C8Client(protocol="https", host="gdn.paas.macrometa.io", port=443, apikey="")

# Create a stream
client.create_stream(stream="quickStart", local=False)

# Retrieve all streams
client.get_streams()

# Create producer
producer = client.create_stream_producer(stream="quickStart", local=False)
msg = "Hello from user"
producer.send(msg)
# To close producer connection use `producer.close()`
producer.close()

# Create subscriber
subscriber = client.subscribe(
    stream="quickStart", local=False, subscription_name="topic_1"
)
# Subscriber reading message
m1 = json.loads(subscriber.recv())
# Decoding the received message
msg1 = base64.b64decode(m1["payload"]).decode("utf-8")
# Acknowledge the received msg.
subscriber.send(json.dumps({"messageId": m1["messageId"]}))
# To close subscriber connection use `subscriber.close()`
subscriber.close()

# Delete stream subscription for topic
client.delete_stream_subscription(
    stream="c8globals.quickStart", subscription="topic_1", local=False
)

# Unsubscribe for topic
client.unsubscribe(subscription="topic_1", local=False)

# Delete stream
client.delete_stream(stream="c8globals.quickStart")
