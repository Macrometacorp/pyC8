Server Administration
---------------------

pyC8 provides operations for server administration and monitoring.
Most of these operations can only be performed by :doc:`tenant <tenant>` admin users
via the ``_system`` fabric which is available inside each tenant.

For information on operations related to administration of tenant users, see :doc:`user`.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to the system fabric of the "mytenant" tenant.
    # This connection is made as the tenant admin using the tenant admin username and password
    tennt = client.tenant(email='tenant_email', password='tenant_password')

    # Connect to the tenant's system fabric as the tenant admin
    sys_fabric = tennt.useFabric('fabric-name)

    # Retrieve the server version.
    sys_fabric.version()

    # Retrieve the server details.
    sys_fabric.details()

    # Get the list of Fabric Edge Locations for this tenant
    # to get only DC names
    dcl = tennt.dclist(detail=False)

    # to get DC details
    dcl = tennt.dclist(detail=True)

    # Find out which Edge Location this tenant connection refers to
    # to get only name for local DC
    local_dc = tennt.localdc(detail=False)

    # to get all details for local DC
    local_dc = tennt.localdc(detail=True)

See :ref:`Tenant` and :ref:`StandardFabric` for API specification.
