from c8.client import C8Client

# Initialize the C8 Data Fabric client.
client = C8Client(protocol='https', host='india2-ap-southeast-2.dev.aws.macrometa.io', port=443)

tennt = client.tenant(name='_mm', fabricname='_system', username='root', password='poweruser')

# List all tenant users.
tennt.users()

# Create a new user.
tennt.create_user(
    username='ddssd@gmail.com',
    password='first_password',
    active=True,
    extra={'team': 'backend', 'title': 'engineer'}
)

# Check if a user exists.
tennt.has_user('ddssd@gmail.com')

# Retrieve details of a user.
tennt.user('ddssd@gmail.com')

# Update an existing user.
tennt.update_user(
    username='ddssd@gmail.com',
    password='second_password',
    active=True,
    extra={'team': 'frontend', 'title': 'engineer'}
)

# Replace an existing user.
tennt.replace_user(
    username='ddssd@gmail.com',
    password='third_password',
    active=True,
    extra={'team': 'frontend', 'title': 'architect'}
)

# Retrieve user permissions for all databases and collections.
tennt.permissions('ddssd@gmail.com')

# Retrieve user permission for "test" database.
tennt.permission(
    username='ddssd@gmail.com',
    fabric='test'
)

# Retrieve user permission for "students" collection in "test" database.
tennt.permission(
    username='ddssd@gmail.com',
    fabric='test',
    collection='students'
)

#Update user permission for "test" database.
tennt.update_permission(
    username='ddssd@gmail.com',
    permission='rw',
    fabric='test'
)

#Update user permission for "students" collection in "test" database.
tennt.update_permission(
    username='ddssd@gmail.com',
    permission='ro',
    fabric='test',
    collection='students'
)

# Reset user permission for "test" database.
tennt.reset_permission(
    username='ddssd@gmail.com',
    fabric='test'
)

# Reset user permission for "students" collection in "test" database.
tennt.reset_permission(
    username='ddssd@gmail.com',
    fabric='test',
    collection='students'
)