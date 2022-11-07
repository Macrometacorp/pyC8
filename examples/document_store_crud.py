from c8 import C8Client

# Creating client instance with API KEY
client = C8Client(
    protocol="https", host="gdn.paas.macrometa.io", port=443, apikey="<your API key>"
)

# Creating global employees collection
client.create_collection(
    name="employees", sync=False, edge=False, local_collection=False, stream=False
)

# Documents to insert in employees collection
docs = [
    {
        "_key": "Han",
        "firstname": "Han",
        "lastname": "Solo",
        "email": "han.solo@macrometa.io",
        "age": 35,
        "role": "Manager",
    },
    {
        "_key": "Bruce",
        "firstname": "Bruce",
        "lastname": "Wayne",
        "email": "bruce.wayne@macrometa.io",
        "age": 40,
        "role": "Developer",
        "phone": "1-999-888-9999",
    },
    {
        "_key": "Jon",
        "firstname": "Jon",
        "lastname": "Doe",
        "email": "jon.doe@macrometa.io",
        "age": 25,
        "role": "Manager",
    },
    {
        "_key": "Zoe",
        "firstname": "Zoe",
        "lastname": "Hazim",
        "email": "zoe.hazim@macrometa.io",
        "age": 20,
        "role": "Director",
    },
    {
        "_key": "Emma",
        "firstname": "Emma",
        "lastname": "Watson",
        "email": "emma.watson@macrometa.io",
        "age": 25,
        "role": "Director",
    },
]

# Insert document, can add a single document or a object with many documents
client.insert_document(collection_name="employees", document=docs)

# Get documents with _key equals to Han
client.get_document(collection="employees", document={"_key": "Han"})

# Update the email field for document with _key equals to Han
client.update_document(
    collection_name="employees",
    document={"_key": "Han", "email": "han@updated_macrometa.io"},
)

# Delete document with _key equals to Han
client.delete_document(collection_name="employees", document={"_key": "Han"})

# Delete employees collection
client.delete_collection(name="employees")
