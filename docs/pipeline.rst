Pipelines
-----------

C8Pipelines is a high performance and resilient stream processor, able to
connect various sources and sinks and perform arbitrary actions, transformations
and filters on payloads. It is easy to deploy and monitor, and ready to drop
into your pipeline either as a static binary or a docker image.

Here is an example showing how you can manage pipelines:

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443)

    # For the "mytenant" tenant, connect to "test" fabric as tenant admin.
    # This returns an API wrapper for the "test" fabric on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    fabric = client.fabric(tenant='mytenant', name='test', username='root', password='hidden')

    # Create collection which works as input to pipeline
    fabric.create_collection("testCollection")

    # Create single pipeline
    dclist = fabric.dclist(detail=False)
    payload = {
        "name": "pipeline1",
        "regions": dclist,
        "enabled": true,
        "config": {
            "input": {
                "type": "c8db",
                "c8db": {
                    "name": "collection_name"
                }
            },
            "output": {
                "type": "c8streams",
                "c8streams": {
                    "name": "stream_name",
                    "local": true
                }
            }
        }
    }
    fabric.create_pipeline(payload)

    # List all pipelines in the fabric.
    fabric.get_all_pipelines()

    # Get details of "pipeline1"
    fabric.get_pipeline("pipeline1")

    # Update pipeline
    payload = {
        "regions": dclist,
        "enabled": true,
        "config": {
            "input": {
                "type": "c8db",
                "c8db": {
                    "name": "collection_name"
                }
            },
            "output": {
                "type": "c8streams",
                "c8streams": {
                    "name": "stream_name",
                    "local": true
                }
            }
        }
    }
    fabric.update_pipeline("pipeline1", payload)

    # Delete pipeline
    fabric.delete_pipeline("pipeline1")
