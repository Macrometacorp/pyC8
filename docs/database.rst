Databases
---------

Each :doc:`tenant <tenant>` on the C8 Data Fabric server can have an arbitrary number of **databases**. Each database has its own set of :doc:`collections <collection>`, :doc:`graphs <graph>` and :doc:`streams <stream>`.

For each tenant, there is a special database named ``_system``, which cannot be dropped and provides operations for managing users, permissions and other databases. Most of the operations can only be executed by admin users. See :doc:`user` for more information.

Each database in the C8 Fabric can be replicated to one or more additional edge Locations in the fabric. If a change is made to such a replicated database in one edge Location, that change will be automatically propagated to, and visible in, all other
Edge Locations to which that database has been replicated.

Each database in the C8 Fabric can be set to publish changes in realtime
to any clients which are connected to that database. 

If the ``Realtime`` option is enabled for a database, then any clients with connections to that database will receive changes via a ``push-based`` mechanism rather than having to continuously poll the database for any changes which may have occurred. This python driver can listen in realtime to changes in a realtime-enabled database by calling the ``db.on_change()`` function for the database referred to by the ``db`` object.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "_system" database as tenant admin.
    # This returns an API wrapper for the "_system" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    sys_db = client.db(tenant='mytenant', name='_system', username='root', password='passwd')

    # List all databases in the 'mytenant' tenant
    sys_db.databases()

    # Create a new database named "test" if it does not exist.
    # Only the tenant admin has access to it at time of its creation.
    if not sys_db.has_database('test'):
        sys_db.create_database('test')

    # Delete the database.
    sys_db.delete_database('test')

    # Create a new database named "test" along with a new set of users.
    # Only "jane", "john", "jake" and the tenant admin have access to it.
    if not sys_db.has_database('test'):
        sys_db.create_database(
            name='test',
            users=[
                {'username': 'jane', 'password': 'foo', 'active': True},
                {'username': 'john', 'password': 'bar', 'active': True},
                {'username': 'jake', 'password': 'baz', 'active': True},
            ],
        )

    # Connect to the new "test" database as user "jane".
    db = client.db(tenant='mytenant', name='test', username='jane', password='foo')

    # Retrieve various database and server information.
    db.name
    db.username
    db.collections()
    db.graphs()

    # Delete the database. Note that the new users will remain.
    sys_db.delete_database('test')

    # Get the list of edge locations for the 'mytenant' tenant.
    # We do this as the tenant admin.
    tennt = client.tenant(name=tenant_name, dbname='_system', username='root', password='root_pass')
    dcl = tennt.dclist()

    # Create a new database which is replicated to all Fabric Edge Locations,
    # and also enable realtime updates on this database.
    # Only the tenant admin can perform this action.
    sys_db.create_database('demodb', dclist=dcl, realtime=True)


See :ref:`C8Client` and :ref:`StandardDatabase` for API specification.
