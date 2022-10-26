Search Examples
---------------

.. code-block:: python

    from c8 import C8Client

    client = C8Client(protocol='https', host='smoke1.eng.macrometa.io', port=443,
    email="user@example.com", password="hidden")

    # Set Search
    is_success = client.set_search("test",False,"name")
    print("Is Success:", is_success)

    # Create a new View
    client.create_view("testView")

    # List all Views
    print(client.list_all_views())

    # Get View Info
    print(client.get_view_info("testView"))

    # Get View Properties
    print(client.get_view_properties("testView"))

    # Rename View
    print(client.rename_view("testView", "testViewNew"))


    # Delete View
    print(client.delete_view("testViewNew"))

    # Create Analyzer
    print(client.create_analyzer(name="testAnalyzer", analyzer_type="identity", properties={"locale":"nl.utf-8","case":"lower"}))

    # Analyzer list
    print(client.get_list_of_analyzer())

    # Get Analyzer Definition
    print(client.get_analyzer_definition("testAnalyzer"))

    # Delete Analyzer
    print(client.delete_analyzer("testAnalyzer"))
