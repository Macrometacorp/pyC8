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

# Return all documents in the collection
docs_collection = client.get_all_documents(collection_name=collection_name)

# Return all documents returned by a query
docs_query = client.get_all_batches(
                query="FOR doc IN {} FILTER doc._key > 0 RETURN doc".format(collection_name)
             )

# Delete collection
client.delete_collection(collection_name)
