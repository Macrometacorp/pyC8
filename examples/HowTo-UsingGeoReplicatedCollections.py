# flake8: noqa
import multiprocessing
import random
import time

from c8 import C8Client

# Variables
fed_url = "gdn1.macrometa.io"
user_password = "hidden"
user_mail = "user@example.com"
geo_fabric = "testfabric"
collection_name = "person" + str(random.randint(1, 10000))

# Insert data into geofabric collection from the given region.


def insert_document(region, data):
    client = C8Client(protocol="https", host=region, port=443)
    tenant = client.tenant(user_mail, user_password)
    fabric = tenant.useFabric(geo_fabric)
    collection = fabric.collection(collection_name)
    collection.insert(data)


if __name__ == "__main__":

    print("\n ------- CONNECTION SETUP  ------")
    print("user: {}, geofabric:{}".format(user_mail, geo_fabric))
    client = C8Client(protocol="https", host=fed_url, port=443)
    tenant = client.tenant(user_mail, user_password)
    fabric = tenant.useFabric(geo_fabric)

    print("Available regions....")
    dclist = fabric.dclist(detail=True)
    for dc in dclist:
        print(" region: {}".format(dc["name"]))
    print(
        "Connected to closest region...\tregion: {}".format(
            fabric.localdc(detail=False)
        )
    )

    print("\n ------- CREATE GEO-REPLICATED COLLECTION  ------")
    person = fabric.create_collection(collection_name)
    print("Created collection: {}".format(collection_name))
    time.sleep(5)  # to account for network latencies in replication

    threads = []
    data = [
        {"firstname": "Peter", "lastname": "Parker", "City": "NewYork"},
        {"firstname": "Bruce", "lastname": "Wayne", "City": "Gotham"},
        {"firstname": "Clark", "lastname": "Kent", "City": "Manhatten"},
        {"firstname": "Ned", "lastname": "Stark", "City": "Winterfell"},
        {"firstname": "Tywin", "lastname": "Lannister", "City": "Kings Landing"},
        {"firstname": "Al", "lastname": "Simmons", "City": "SanJose"},
        {"firstname": "Wade", "lastname": "Wilson", "City": "NewYork"},
        {"firstname": "Jenny", "lastname": "Sparks", "City": "SanFrancisco"},
    ]

    print("\n ------- WRITE DATA LOCALLY IN EACH REGION  ------")
    print("From each region, inserting one data record in parallel....")
    counter = 0
    print(len(data))
    for dc in dclist:
        if counter < len(data):
            print("In", dc)
            print("***", counter)
            print("region: {}, document:{}".format(dc["name"], data[counter]))
            process = multiprocessing.Process(
                target=insert_document,
                args=(
                    dc["tags"]["url"],
                    data[counter],
                ),
            )
            threads.append(process)
            process.start()
            counter += 1

    for thread in threads:
        thread.join()
    time.sleep(5)  # to account for network latencies in replication

    print("\n ------- READ DATA LOCALLY IN EACH REGION  ------")
    documents = {}
    for dc in dclist:
        batches = []
        query = "FOR doc in {} RETURN doc".format(collection_name)
        resp = fabric.c8ql.execute(query)
        for batch in resp.batch():
            batches.append(batch)
        documents[dc["tags"]["url"]] = batches
        print("region: {}, \n documents:{} \n".format(dc["name"], documents))

    print("\n ------- DONE  ------")
