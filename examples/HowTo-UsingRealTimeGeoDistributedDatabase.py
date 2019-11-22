from c8 import C8Client
import random
import threading
import time

# Variables
service_url = "gdn1.macrometa.io" # The request will be automatically routed to closest location.
guest_mail = "guest1@macrometa.io"
guest_password = "guest1"
geo_fabric = "testfabric"
collection_name = "employees" + str(random.randint(1, 10000))

def create_callback():
    def callback_fn(event):
        print("received... document:{}".format(event))
        return
    fabric.on_change(collection_name, callback=callback_fn)

if __name__ == '__main__':

    print("\n ------- CONNECTION SETUP  ------")
    print("user: {}, geofabric:{}".format(guest_mail, geo_fabric))
    client = C8Client(protocol='https', host=service_url, port=443) # Automatically routed to nearest region.
    tenant = client.tenant(guest_mail, guest_password)
    fabric = tenant.useFabric(geo_fabric)

    print("Availabile regions....")
    dclist = fabric.dclist(detail=True)
    for dc in dclist:
        print(" region: {}".format(dc["name"]))
    print("Connected to closest region...\tregion: {}".format(fabric.localdc(detail=False)))

    print("\n ------- CREATE GEO-REPLICATED COLLECTION  ------")
    employees = fabric.create_collection(collection_name)
    print("Created collection: {}".format(collection_name))
    time.sleep(2)  # to account for network latencies in replication

    print("\n ------- SUBSCRIBE TO CHANGES  ------")
    # Receive documents from the collection in realtime... PUSH Model
    rt_thread = threading.Thread(target=create_callback)
    rt_thread.start()
    time.sleep(2)
    print("Callback registered for collection: {}".format(collection_name))

    print("\n ------- INSERT DOCUMENTS  ------")
    print("Inserting 3 documents to the collection...")
    employees.insert({'_key':'John', 'firstname': 'John', 'lastname':'Wayne', 'email':'john.wayne@macrometa.io'})
    employees.insert({'_key':'Clark', 'firstname': 'Clark', 'lastname':'Kent', 'email':'clark.kent@macrometa.io'})
    employees.insert({'_key': 'Bruce', 'firstname': 'Bruce', 'lastname':'Wayne', 'email':'bruce.wayne@macrometa.io'})
  
    print("Wait to close the callback...")
    rt_thread.join()

    print("\n ------- DONE  ------")
