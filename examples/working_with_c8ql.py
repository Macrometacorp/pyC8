from c8 import C8Client

# Creating client instance with API KEY
client = C8Client(protocol="https", host="gdn.paas.macrometa.io", port=443, apikey="")

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

# Example of the use of FILTER and Operators
query = "FOR doc IN employees FILTER doc.role == 'Manager' FILTER doc.age > 30 RETURN {'Name':doc.firstname,'Last Name':doc.lastname,'Email':doc.email}"
cursor = client.execute_query(query=query)
docs = [document for document in cursor]

# Example of the use of SORT and LIMIT
query = "FOR doc IN employees SORT doc.age LIMIT 2 RETURN doc"
cursor = client.execute_query(query=query)
docs = [document for document in cursor]

# Example of the use of OFFSET, LIMIT
query = "FOR doc IN employees SORT doc.age LIMIT 2, 4 RETURN doc"
cursor = client.execute_query(query=query)
docs = [document for document in cursor]

# Example of how to use COLLECT to group by age
query = "FOR doc IN employees COLLECT age = doc.age INTO employeesByAge RETURN {age, employee: employeesByAge[*].doc.firstname}"
cursor = client.execute_query(query=query)
docs = [document for document in cursor]

# Delete employees collection
client.delete_collection(name="employees")
