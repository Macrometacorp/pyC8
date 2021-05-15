from c8 import C8Client
import threading
import pprint
import time

# Variables - URLs
global_url = "sankalp-ap-west.eng.macrometa.io"


# Variables - DB
email = "mm@macrometa.io"
password = "Macrometa123!@#"
geo_fabric = "_system"
collection_name = "ddos"

# Variables - Data
data = [
    {"ip": "10.1.1.1", "action": "block", "rule": "blacklistA"},
    {"ip": "20.1.1.2", "action": "block", "rule": "blacklistA"},
    {"ip": "30.1.1.3", "action": "block", "rule": "blacklistB"},
    {"ip": "40.1.1.4", "action": "block", "rule": "blacklistA"},
    {"ip": "50.1.1.5", "action": "block", "rule": "blacklistB"},
  ]

pp = pprint.PrettyPrinter(indent=4)

if __name__ == '__main__':

  # Step1: Open connection to GDN. You will be routed to closest region.
  print("\n1. CONNECT: federation: {},  user: {}".format(global_url, email))
  client = C8Client(protocol='https', host=global_url, port=443,
                    email=email, password=password,
                    geofabric=geo_fabric)

  # Step2: Create a collection if not exists
  print("\n2. CREATE_COLLECTION: region: {},  collection: {}".format(global_url, collection_name))
  if client.has_collection(collection_name):
      collection = client.collection(collection_name)
  else:
      collection = client.create_collection(collection_name)

  # Subscriber to receive events when changes are made to collection.
  def create_callback():
    def callback_fn(event):
        pp.pprint(event)
        return
    client.on_change(collection_name, callback=callback_fn)

  # Step3: Subscribe to receive documents in realtime (PUSH model)
  print("\n3. SUBSCRIBE_COLLECTION: region: {},  collection: {}".format(global_url, collection_name))
  rt_thread = threading.Thread(target=create_callback)
  rt_thread.start()
  time.sleep(2)
  print("Callback registered for collection: {}".format(collection_name))

  # Step4: Subscribe to receive documents in realtime (PUSH model)
  print("\n4. INSERT_DOCUMENTS: region: {},  collection: {}".format(global_url, collection_name))
  client.insert_document(collection_name, document=data)

  # Step5: Wait to close the callback.
  print("\n5. Waiting to close callback")
  rt_thread.join()

  # Step6: Delete data.
  #print("\n6. DELETE_DATA: region: {}, collection: {}".format(global_url, collection_name))
  #collection.truncate()
  #fabric.delete_collection(collection_name)