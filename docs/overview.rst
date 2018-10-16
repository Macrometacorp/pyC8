Getting Started
---------------

Here is an example showing how **pyC8** client can be used:

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "_system" database as root user.
    # This returns an API wrapper for "_system" database.
    sys_db = client.db(name='tenant1', dbname='_system', username='root', password='passwd')

    # Create a new database named "test" if it does not exist.
    if not sys_db.has_database('test'):
        sys_db.create_database('test')

    # Connect to "test" database as tenant admin.
    # This returns an API wrapper for "test" database.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Create a new collection named "students" if it does not exist.
    # This returns an API wrapper for "students" collection.
    if db.has_collection('students'):
        students = db.collection('students')
    else:
        students = db.create_collection('students')

    # Add a hash index to the collection.
    students.add_hash_index(fields=['name'], unique=False)

    # Truncate the collection.
    students.truncate()

    # Insert new documents into the collection.
    students.insert({'name': 'jane', 'age': 19})
    students.insert({'name': 'josh', 'age': 18})
    students.insert({'name': 'jake', 'age': 21})

    # Execute a C8QL query. This returns a result cursor.
    cursor = db.c8ql.execute('FOR doc IN students RETURN doc')

    # Iterate through the cursor to retrieve the documents.
    student_names = [document['name'] for document in cursor]
