
..testcode::

    from c8 import C8Client

    client = C8Client(protocol='https', host='smoke1.eng.macrometa.io', port=443,
     email="guest@macrometa.io", password="guest")

    # Set Search
    is_success = client.set_search("test",False,"name")
    print("Is Success:", is_success)

    # Create a new View
    client.create_view("xyz")

    # List all Views
    print(client.list_all_views())

    # Get View Info
    print(client.get_view_info("xyz"))

    # Get View Properties
    print(client.get_view_properties("xyz"))

    # Rename View
    print(client.rename_view("xyz", "xyznew"))


    # Delete View
    print(client.delete_view("xyznew"))

    # Create Analyzer
    print(client.create_analyzer(name="xyz", analyzer_type="identity", properties="abc"))

    # Analyzer list
    print(client.get_list_of_analyzer())

    # Get Analyzer Definition
    print(client.get_analyzer_definition("xyz"))

    # Delete Analyzer
    print(client.delete_analyzer("xyz"))
