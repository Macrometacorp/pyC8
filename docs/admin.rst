Server Administration
---------------------

pyC8 provides operations for server administration and monitoring.
Most of these operations can only be performed by :doc:`tenant <tenant>` admin users 
via the ``_system`` database which is available inside each tenant.

For information on operations related to administration of tenant users, see :doc:`user`.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to the system database of the "mytenant" tenant.
    # This connection is made as the tenant admin using the tenant admin username and password
    tennt = client.tenant(name='mytenant', dbname='_system', username='root', password='root_pass')

    # Connect to the tenant's system database as the tenant admin
    sys_db = client.db(tenant='mytenant', name='_system', username='root', password='root_pass')

    # Retrieve the server version.
    sys_db.version()

    # Retrieve the server details.
    sys_db.details()

    # Get the list of Fabric Edge Locations for this tenant
    dcl = tennt.dclist()

    # Find out which Edge Location this tenant connection refers to
    local_dc = tennt.dclist_local()




See :ref:`StandardDatabase` for API specification.
