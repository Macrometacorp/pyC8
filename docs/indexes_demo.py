from c8.client import C8Client

# Initialize the C8 Data Fabric client.
client = C8Client(protocol='https', host='india2-ap-southeast-2.dev.aws.macrometa.io', port=443)

# For the "mytenant" tenant, connect to "test" fabric as tenant admin.
# This returns an API wrapper for the "test" fabric on tenant 'mytenant'
# Note that the 'mytenant' tenant should already exist.
db = client.fabric(tenant='tp', name='db1', username='root', password='poweruser')

cities = db.create_collection('citieuereruus')

# List the indexes in the collection.
print(cities.indexes())

# Add a new hash index on document fields "continent" and "country".
index = cities.add_hash_index(fields=['continent', 'country'], unique=True)

# Add new fulltext indexes on fields "continent" and "country".
index = cities.add_fulltext_index(fields=['continent'])
index = cities.add_fulltext_index(fields=['country'])

# Add a new skiplist index on field 'population'.
index = cities.add_skiplist_index(fields=['population'], sparse=False)

# Add a new geo-spatial index on field 'coordinates'.
index = cities.add_geo_index(fields=['coordinates'])

# Add a new persistent index on fields 'currency'.
index = cities.add_persistent_index(fields=['currency'], sparse=True)

# Delete the last index from the collection.
cities.delete_index(index['id'])