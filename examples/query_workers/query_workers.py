import time

from c8 import C8Client

# Creating client instance with API KEY
client = C8Client(
    protocol="https", host="gdn.paas.macrometa.io", port=443, apikey="<your API key>"
)

client.create_collection(
    name="employees", sync=False, edge=False, local_collection=False, stream=False
)

# Query to be saved with bind variable `docs`
insert_query = "FOR doc in @docs INSERT {'firstname': doc.firstname, 'lastname': doc.lastname, 'email': doc.email, 'zipcode': doc.zipcode, '_key': doc._key} IN employees"

# Query worker object with name insertRecord
insert_data = {"query": {"name": "insertRecord", "value": insert_query}}

# Create query worker `insertRecord`
client.create_restql(data=insert_data)

# Delay for the query worker to be created
time.sleep(2)

# Documents to insert
docs = [
    {
        "_key": "James",
        "firstname": "James",
        "lastname": "Kirk",
        "email": "james.kirk@macrometa.io",
        "zipcode": "22222",
    },
    {
        "_key": "John",
        "firstname": "John",
        "lastname": "Doe",
        "email": "john.doe@macrometa.io",
        "zipcode": "11111",
    },
]

# Execute query worker `insertRecord` with bind variable `docs`
client.execute_restql(name="insertRecord", data={"bindVars": {"docs": docs}})

# Create query workers with import function
get_data_query = "FOR doc IN employees RETURN doc"
update_data_query = "FOR doc IN employees FILTER doc.firstname == 'James' UPDATE doc with { zipcode: '33333' } IN employees"

# Query worker objects with names, getRecords and updateRecord
get_data = {"name": "getRecords", "value": get_data_query}
update_data = {"name": "updateRecord", "value": update_data_query}
queries = [get_data, update_data]

# Import query workers `getRecords` and `updateRecord`
client.import_restql(queries=queries)

# Delay for the query worker to be created
time.sleep(2)

# Execute query worker `getRecords` with batch size equal to 2
resp = client.execute_restql(name="getRecords", data={"bindVars": {}, "batchSize": 1})

# Get cursor id
id = resp["id"]

# Read next batch from cursor
client.read_next_batch_restql(id=id)

# Get all RestQL queries
client.get_restqls()

updated_insert_query = "INSERT {'_key': 'barry.allen@macrometa.io', 'firstname': 'Barry', 'lastname': 'Allen'} IN employees"

# Query worker object to update existing RestQL
updated_insert_data = {"query": {"value": updated_insert_query}}

# Update query worker `insertRecord`
client.update_restql(name="insertRecord", data=updated_insert_data)

# Delete RestQL queries
client.delete_restql(name="insertRecord")
client.delete_restql(name="getRecords")
client.delete_restql(name="updateRecord")

# Delete Collection
client.delete_collection(name="employees")
