from c8 import C8Client
import time
import base64
import six
import json
import warnings

warnings.filterwarnings("ignore")

print("--- Connecting to C8")
client = C8Client(protocol='https', host='sankalp-ap-west.eng.macrometa.io', port=443,
                  email='mm@macrometa.io', password='Macrometa123!@#',
                  geofabric='_system')
# --------------------------------------------------------------
print("publish messages to stream...")
demo_stream = 'sankalpStream'
producer = client.create_stream_producer(demo_stream, local=False)

for i in range(10):
    msg1 = "Persistent: Hello from " + "(" + str(i) + ")"
    data = {
        "payload": base64.b64encode(six.b(msg1)).decode("utf-8")
    }
    producer.send(json.dumps(data))
