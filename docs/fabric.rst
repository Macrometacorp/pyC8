Fabrics
---------

Each :doc:`tenant <tenant>` on the C8 Data Fabric server can have an arbitrary number of **fabrics**. Each fabric has its own set of :doc:`collections <collection>`, :doc:`graphs <graph>` and :doc:`streams <stream>`.

For each tenant, there is a special fabric named ``_system``, which cannot be dropped and provides operations for managing users, permissions and other fabrics. Most of the operations can only be executed by admin users. See :doc:`user` for more information.

Each fabric in the C8 Fabric can be replicated to one or more additional edge Locations in the fabric. If a change is made to such a replicated fabric in one edge Location, that change will be automatically propagated to, and visible in, all other
Edge Locations to which that fabric has been replicated.

Each fabric in the C8 Fabric can be set to publish changes in realtime
to any clients which are connected to that fabric. 

If the ``Realtime`` option is enabled for a fabric, then any clients with connections to that fabric will receive changes via a ``push-based`` mechanism rather than having to continuously poll the fabric for any changes which may have occurred. This python driver can listen in realtime to changes in a realtime-enabled fabric by calling the ``fabric.on_change()`` function for the fabric referred to by the ``fabric`` object.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "_system" fabric as tenant admin.
    # This returns an API wrapper for the "_system" fabric on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    sys_fabric = client.fabric(tenant='mytenant', name='_system', username='root', password='passwd')

    # List all fabrics in the 'mytenant' tenant
    sys_fabric.fabrics()

    # Create a new fabric named "test" if it does not exist.
    # Only the tenant admin has access to it at time of its creation.
    if not sys_fabric.has_fabric('test'):
        sys_fabric.create_fabric('test')

    #get fabric details
    sys_fabric.fabrics_detail()

    # Delete the fabric.
    sys_fabric.delete_fabric('test')

    # Create a new fabric named "test" along with a new set of users.
    # Only "jane", "john", "jake" and the tenant admin have access to it.
    if not sys_fabric.has_fabric('test'):
        sys_fabric.create_fabric(
            name='test',
            users=[
                {'username': 'jane', 'password': 'foo', 'active': True},
                {'username': 'john', 'password': 'bar', 'active': True},
                {'username': 'jake', 'password': 'baz', 'active': True},
            ],
        )

    # Connect to the new "test" fabric as user "jane".
    fabric = client.fabric(tenant='mytenant', name='test', username='jane', password='foo')

    # Retrieve various fabric and server information.
    fabric.name
    fabric.username
    fabric.collections()
    fabric.graphs()

    # Delete the fabric. Note that the new users will remain.
    sys_fabric.delete_fabric('test')

    # Get the list of edge locations for the 'mytenant' tenant.
    # We do this as the tenant admin.
    tennt = client.tenant(name=tenant_name, fabricname='_system', username='root', password='root_pass')
    dcl = tennt.dclist()

    # Create a new fabric which is replicated to all Fabric Edge Locations,
    # and also enable realtime updates on this fabric.
    # Only the tenant admin can perform this action.
    sys_fabric.create_fabric('demofabric', dclist=dcl, realtime=True)


See :ref:`C8Client` and :ref:`StandardFabric` for API specification.
