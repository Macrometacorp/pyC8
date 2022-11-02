import sys

from c8 import C8Client

# Creating client instance with API KEY
client = C8Client(protocol="https", host="eng.paas.macrometa.io", port=443, apikey="")

# Creating global employees collection
client.create_collection(
    name="employees", sync=False, edge=False, local_collection=False, stream=True
)


# Callback used for on_change method
def callback_fn(event):
    sys.stdout.write(str(event))


# Get real-time updates from collection
client.on_change(collection="employees", callback=callback_fn)

# Delete employees collection
client.delete_collection(name="employees")
