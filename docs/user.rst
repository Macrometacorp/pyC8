Users and Permissions
---------------------

pyC8 provides operations for managing :doc:`tenant <tenant>` users and permissions. Most of
these operations can only be performed by tenant admins via the ``_system`` tenant fabric.

**Example:**
The Simple Way

.. testcode::

    from c8 import C8Client

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443)
    print("Has User: ", client.has_user('root'))

    # Get all Users
    print("Users: ", client.get_users())
    # Get information about a user
    print("User: ", client.get_user("root"))
    # Get User Permissions
    print("Permissions: ", client.get_permissions("root"))
    print("Permission: ", client.get_permission("root", "_system"))
    # Create a User
    client.create_user(username='testuser', email='test@test.com', password='hidden',
                        active=True)
    print("TestUser: ", client.get_user("testuser"))

    # Update User
    client.update_user(username='testuser', password='hidden',
                    active=False)
    # Delete User
    client.delete_user(username='testuser')

The Object Oriented Way

.. testcode::

    from c8 import C8Client

	# Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='gdn1.macrometa.io', port=443)

    # For the "mytenant" tenant, connect to "_system" fabric as tenant admin.
    # This returns an API wrapper for the "_system" fabric on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    tennt = client.tenant(email="my-tenant", password="hidden")
    fabric = tenant.useFabric('_system')
    # List all tenant users.
    tennt.users()

    # Create a new user.
    tennt.create_user(
        username='johndoe@gmail.com',
        password='first_password',
        active=True,
        extra={'team': 'backend', 'title': 'engineer'}
    )

    # Check if a user exists.
    tennt.has_user('johndoe@gmail.com')

    # Retrieve details of a user.
    tennt.user('johndoe@gmail.com')

    # Update an existing user.
    tennt.update_user(
        username='johndoe@gmail.com',
        password='second_password',
        active=True,
        extra={'team': 'frontend', 'title': 'engineer'}
    )

    # Replace an existing user.
    tennt.replace_user(
        username='johndoe@gmail.com',
        password='third_password',
        active=True,
        extra={'team': 'frontend', 'title': 'architect'}
    )

    # Retrieve user permissions for all fabrics and collections.
    tennt.permissions('johndoe@gmail.com')

    # Retrieve user permission for "test" fabric.
    tennt.permission(
        username='johndoe@gmail.com',
        fabric='test'
    )

    # Retrieve user permission for "students" collection in "test" fabric.
    tennt.permission(
        username='johndoe@gmail.com',
        fabric='test',
        collection='students'
    )

    # Update user permission for "test" fabric.
    tennt.update_permission(
        username='johndoe@gmail.com',
        permission='rw',
        fabric='test'
    )

    # Update user permission for "students" collection in "test" fabric.
    tennt.update_permission(
        username='johndoe@gmail.com',
        permission='ro',
        fabric='test',
        collection='students'
    )

    # Reset user permission for "test" fabric.
    tennt.reset_permission(
        username='johndoe@gmail.com',
        fabric='test'
    )

    # Reset user permission for "students" collection in "test" fabric.
    tennt.reset_permission(
        username='johndoe@gmail.com',
        fabric='test',
        collection='students'
    )

See :ref:`Tenant` and :ref:`StandardFabric` for API specification.
