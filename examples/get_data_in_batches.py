import math

from c8 import C8Client

# Creating client instance with API KEY
client = C8Client(
    protocol="https", host="gdn.paas.macrometa.io", port=443, apikey="<your API key>"
)

# Create a collection
collection_name = "result"
client.create_collection(collection_name)

# Insert 2000 ramdon documents in collection
query = "FOR doc IN 1..2000 INSERT {{value: doc}} INTO {}".format(collection_name)
cursor = client.execute_query(query=query)

fabric = client._fabric
# Get the document count of the collection to calculate iterations
document_count = fabric.collection(collection_name).count()
iterations = int(math.ceil(document_count / 1000))
data = []

for i in range(iterations):
    a = i * 1000
    query = "FOR doc IN {} LIMIT {}, {} RETURN doc".format(collection_name, a, 1000)
    cursor = fabric.c8ql.execute(query, count=True, batch_size=1000)
    data.append(cursor.batch())

# Clean the data
flat_data = [item for sublist in data for item in sublist]

# Delete collection
client.delete_collection(collection_name)
