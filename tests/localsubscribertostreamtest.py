from c8 import C8Client
import time
import base64
import six
import json
import warnings

warnings.filterwarnings("ignore")

print("--- Connecting to C8")
# Simple Way
client = C8Client(protocol='https', host='sankalp-ap-west.eng.macrometa.io', port=443,
                  email='mm@macrometa.io', password='Macrometa123!@#',
                  geofabric='_system')
# --------------------------------------------------------------
demo_stream = 'sankalpStream'
subscriber = client.subscribe(stream="demostream", local=False, subscription_name="test-sankalp-subscription-1")
for i in range(10):
    print("In ", i)
    m1 = json.loads(subscriber.recv())  # Listen on stream for any receiving msg's
    msg1 = base64.b64decode(m1["payload"])
    print("Received message '{}' id='{}'".format(msg1, m1["messageId"]))  # Print the received msg over stream
    subscriber.send(json.dumps({'messageId': m1['messageId']}))  # Acknowledge the received msg.
