Collections
-----------

A **collection** contains :doc:`documents <document>`. It is uniquely identified
by its name which must consist only of hyphen, underscore and alphanumeric
characters. There are three types of collections in pyC8:

* **Standard Collection:** contains regular documents.
* **Vertex Collection:** contains vertex documents for graphs. See
  :ref:`here <vertex-collections>` for more details.
* **Edge Collection:** contains edge documents for graphs. See
  :ref:`here <edge-collections>` for more details.

Here is an example showing how you can manage standard collections:

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # List all collections in the database.
    db.collections()

    # Create a new collection named "students" if it does not exist.
    # This returns an API wrapper for "students" collection.
    if db.has_collection('students'):
        students = db.collection('students')
    else:
        students = db.create_collection('students')

    # Retrieve collection properties.
    students.name
    students.db_name
    students.count()

    # Perform various operations.
    students.truncate()
    students.configure(journal_size=3000000)

    # Delete the collection.
    db.delete_collection('students')

See :ref:`StandardDatabase` and :ref:`StandardCollection` for API specification.
