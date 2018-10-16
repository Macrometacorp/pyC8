Pregel
------

pyC8 supports **Pregel**, an C8 Data Fabric module for distributed iterative
graph processing. For more information, refer to `C8 Data Fabric manual`_.

.. _C8 Data Fabric manual: http://www.macrometa.co

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to "test" database as tenant admin.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the Pregel API wrapper.
    pregel = db.pregel

    # Start a new Pregel job in "school" graph.
    job_id = db.pregel.create_job(
        graph='school',
        algorithm='pagerank',
        store=False,
        max_gss=100,
        thread_count=1,
        async_mode=False,
        result_field='result',
        algorithm_params={'threshold': 0.000001}
    )

    # Retrieve details of a Pregel job by ID.
    job = pregel.job(job_id)

    # Delete a Pregel job by ID.
    pregel.delete_job(job_id)

See :ref:`Pregel` for API specification.
