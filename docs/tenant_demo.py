from c8.client import C8Client

# Initialize the C8 Data Fabric client.
client = C8Client(protocol='https', host='india2-ap-southeast-2.dev.aws.macrometa.io', port=443)

# For the "mytenant" tenant, connect to "test" fabric as tenant admin.
# This returns an API wrapper for the "test" fabric on tenant 'mytenant'
# Note that the 'mytenant' tenant should already exist.
db = client.fabric(tenant='tp', name='db1', username='root', password='poweruser')
sys_db = client.fabric(tenant='_mm', name='_system', username='root', password='poweruser')
tennt = client.tenant(name="_mm", fabricname='_system', username='root', password='poweruser')

# Create a new tenant user. This operation can only be performed by the tenant admin.
tennt.create_user(username='p3', password='demo_pass', active=True)

# Get the list of Fabric Edge Locations for this tenant
dcl = tennt.dclist()

# Connect to the tenant's system database as the tenant admin
sys_db = client.fabric(tenant='_mm', name='_system', username='root', password='poweruser')

# Create a new database within the tenant. This operation can only be performed by the tenant admin.
# The database will be replicated to all Fabric Edge Locations specified in the 'dclist' param.
# The 'demouser' tenant user created above will be given read permissions to the database.
print("***************", sys_db.has_fabric("demodb"))
if not sys_db.has_fabric('demodb'):
    sys_db.create_fabric('demodb', dclist=dcl,
                           users=[{'username': 'p3', 'password': 'demo_pass', 'active': True}], realtime=True)

# Delete a database. This operation can only be performed by the tenant admin.
sys_db.delete_fabric('demodb')