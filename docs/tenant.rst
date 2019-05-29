Tenants
---------

The C8 Global Edge Fabric follows a multitenancy model. A tenant is a group of users who share a common isolated enviroment within the Edge Fabric, and one tenant cannot access the information of another.

Each tenant has a tenant admin called `root` who has privileges to create fabrics, users, streams and functions within that tenant. The tenant admin can create users and fabrics within that tenant, and can define different privileges for each tenant user for the fabrics, collections and other information contained within the tenant.

Note that tenants **can only be created and deleted by the Macrometa superadmin.** A tenant admin cannot create, delete or access other tenants.

Each tenant on the C8  Fabric server can have an arbitrary number of geofabrics. Each tenant fabric has its own set of collections, graphs, streams and functions. For each tenant, there is a special fabric named ``_system``, which cannot be dropped and which provides operations for managing users, permissions and other geofabrics.

These operations can only be executed by the tenant admin. Tenant users who have been given the relevant access privileges by the tenant admin may create collections, streams, functions and documents within tenant fabrics to which the users have been given read/write access.

**Example:**

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # Connect to the system fabric of the "mytenant" tenant.
    # This connection is made as the tenant admin using the tenant admin username and password
    tennt = client.tenant(name=tenant_name, fabricname='_system', username='root', password='root_pass')

    # Create a new tenant user. This operation can only be performed by the tenant admin.
    tennt.create_user(username='demouser', password='demo_pass', active=True)

    # Get the list of Fabric Edge Locations for this tenant
    dcl = tennt.dclist()

    # Connect to the tenant's system fabric as the tenant admin
    sys_fabric = client.fabric(tenant='mytenant', name='_system', username='root', password='root_pass')

    # Create a new fabric within the tenant. This operation can only be performed by the tenant admin.
    # The fabric will be replicated to all Fabric Edge Locations specified in the 'dclist' param.
    # The 'demouser' tenant user created above will be given read permissions to the fabric.
    if not sys_fabric.has_fabric('demofabric'):
        sys_fabric.create_fabric('demofabric', dclist=dcl, users=[{'username':'demouser','password':'demo_pass','active':True}])

    # Delete a fabric. This operation can only be performed by the tenant admin.
    sys_fabric.delete_fabric('demofabric')


    # save restql
    data = {
        "query": {
            "parameter": {},
            "name": "demo",
            "value": "FOR employee IN employees RETURN employee"
        }
    }
    response = demotenant.save_restql(data)

    # execute restql without bindVars
    response = demotenant.execute_restql("demo")

    # execute restql with bindVars
    response = demotenant.execute_restql("demo",
                                         {"bindVars": {"name": "guest.root"}})

    # get all restql
    response = demotenant.get_all_restql()

    # update restql
    data = {
        "query": {
            "parameter": {},
            "value": "FOR employee IN employees Filter doc.name=@name RETURN employee"
        }
    }
    response = demotenant.update_restql("demo", data)

    # delete restql
    response = demotenant.delete_restql("demo")


See :ref:`C8Client`, :ref:`Tenant` and :ref:`StandardFabric` for API specification.
