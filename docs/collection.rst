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

    # For the "mytenant" tenant, connect to "test" fabric as tenant admin.
    # This returns an API wrapper for the "test" fabric on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    tenant = client.tenant(email='mytenant@example.com', password='tenant-password')
    fabric = tenant.useFabric('test')
    # List all collections in the fabric.
    fabric.collections()

    # Create a new collection named "students" if it does not exist.
    # This returns an API wrapper for "students" collection.
    if fabric.has_collection('students'):
        students = fabric.collection('students')
    else:
        students = fabric.create_collection('students')

    # To insert data from a csv file
    # path to csv file should be an absolute path
    students.insert_from_file("~/data.csv")

    # Retrieve collection properties.
    students.name
    students.fabric_name
    students.count()

    # Perform various operations.
    students.truncate()
    students.configure(journal_size=3000000)

    # Delete the collection.
    fabric.delete_collection('students')

See :ref:`StandardFabric` and :ref:`StandardCollection` for API specification.
