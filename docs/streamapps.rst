StreamApps
-----------

A **StreamApps** contains a :`definition`. 
Here is an example showing how you can manage standard StreamApps:

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443)

    # For the "mytenant" tenant, connect to "test" fabric as tenant admin.
    # This returns an API wrapper for the "test" fabric on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    tenant = client.tenant(email='mytenant@example.com', password='tenant-password')
    fabric = tenant.useFabric('test')
    
    # List all stream apps in the fabric.
    result = fabric.retrive_stream_app()
    print(result)

    # To get sample stream applications in the fabric useFabric
    result = fabric.get_samples_stream_app()

    # To validate a stream application definition use
    data = {
        "definition": "@App:name(\'Sample-Cargo-App\')\\n\\n-- Stream\\ndefine stream srcCargoStream (weight int);\\n\\n-- Table\\ndefine table destCargoTable (weight int, totalWeight long);\\n\\n-- Data Processing\\n@info(name=\'Query\')\\nselect weight, sum(weight) as totalWeight\\nfrom srcCargoStream\\ninsert into destCargoTable;",
        "regions": []
    }
    result = fabric.validate_stream_app(data)
    print(result)

    # Create a new stream app named "Sample-Cargo-App" using following definition:.
    data = {
        "definition": "@App:name(\'Sample-Cargo-App\')\\n\\n-- Stream\\ndefine stream srcCargoStream (weight int);\\n\\n-- Table\\ndefine table destCargoTable (weight int, totalWeight long);\\n\\n-- Data Processing\\n@info(name=\'Query\')\\nselect weight, sum(weight) as totalWeight\\nfrom srcCargoStream\\ninsert into destCargoTable;",
        "regions": []
    }
    result = fabric.validate_stream_app(data)
    print(result)

    # To operate on created apps, you need to create an instance of the app
    app = fabric.stream_app("Sample-Cargo-App")

    # Now you can get stream app details using
    result = app.get()
    print(result)

    # Update the app using
    data = {
        "definition": "@App:name(\'Sample-Cargo-App\')\\n\\n-- Stream\\ndefine stream srcCargoStream (weight int);\\n\\n-- Table\\ndefine table destCargoTable (weight int, totalWeight long);\\n\\n-- Data Processing\\n@info(name=\'Query\')\\nselect weight, sum(weight) as totalWeight\\nfrom srcCargoStream\\ninsert into destCargoTable;",
        "regions": ["asia-east1","asia-north1"]
    }
    result = fabric.update(data)
    print(result)

    # Enable / Disable app using change_state function
    # pass True to enable and False to disable the app
    result = app.change_state(True)
    print(result)

    # You can delete the app using
    result = app.delete()
    print(result)

    # fire query on app using
    q = "some query" 
    result = app.query(q)
    print(result)