from c8 import C8Client

######## CONSTANTS - Some to be changed ########

### Change these as per the demo fed setup
HOST= '[::1]'
PORT=8530
DCLIST="dc1,dc2"
REALTIME=True
SYSTEM_ROOT_USERNAME = 'root'
SYSTEM_ROOT_USER_PASSWORD = ''

### Not much need to change these
SYSTEM_TENANT = '_mm'
SYSTEM_DB = '_system'
DEMOTENANT = 'demo'
DBNAME='demo'
DBUSER='demouser'
USERPASS='demouser'
USER_FRIENDLYNAME='Demo User'
USERS_OBJ = [{"username":DBUSER, "password":USERPASS, "active":True, "extra":{"name":USER_FRIENDLYNAME}}]

DEMODB_COLLECTIONS = [
    'intrusions',
    'trades',
    'demo_queries'
]

# Use update_user and add the query def to the "extra" field, with this format:
# {"queries":[{"name":"alldocs","parameter":{},"value":"for doc in test return doc"}]}
PRESEEDED_QUERIES = {"name":USER_FRIENDLYNAME, 
            "queries":[
                {
                    "name":"Demo: Add 100 documents to "+COLLECTION_DEMOQUERIES,
                    "parameter":{},
                    "value":'FOR i IN 1..10 INSERT { name: CONCAT(@user_prefix, i), gender: (i % 2 == 0 ? "f" : "m"), likes: ROUND(RAND()*100), follows: ROUND(RAND() * 100) } INTO '+COLLECTION_DEMOQUERIES
                },
                {
                    "name":"Demo: Key-sorted documents in "+COLLECTION_DEMOQUERIES,
                    "parameter":{},
                    "value":'FOR doc IN '+COLLECTION_DEMOQUERIES+' SORT doc._key RETURN "Key":doc._key, "Name":doc.name, "Gender":doc.gender, "Likes":doc.likes, "Follows":doc.follows'
                },
                {
                    "name":"Demo: Modify documents in "+COLLECTION_DEMOQUERIES,
                    "parameter":{},
                    "value":'FOR doc IN '+COLLECTION_DEMOQUERIES+' UPDATE { _key: doc._key, gender:@gender, likes:@likes, follows:@follows }'+COLLECTION_DEMOQUERIES
                },
                {
                    "name":"Demo: Remove documents in "+COLLECTION_DEMOQUERIES,
                    "parameter":{},
                    "value":'FOR doc IN '+COLLECTION_DEMOQUERIES+' REMOVE doc IN '+COLLECTION_DEMOQUERIES
                },

            ]
          }

######## END : CONSTANTS - Some to be changed ########

# Initialize the client for C8
print('Creating connection...')
client = C8Client(protocol='http', host=HOST, port=PORT)

## Use new tenant
print("Creating client connection to system tenant '"+SYSTEM_TENANT+"'...")
sys_tenant = client.tenant(name=SYSTEM_TENANT, dbname=SYSTEM_DB, username=SYSTEM_ROOT_USERNAME, password=SYSTEM_ROOT_USER_PASSWORD)


print("Getting DC list...")
dcl = sys_tenant.dclist()
print('Sys tenant DC List is: '+str(dcl))

# Create new tenant if required
if not sys_tenant.has_tenant(DEMOTENANT):
	print("Creating new tenant '"+DEMOTENANT+"', this may take a while...")
	sys_tenant.create_tenant(nt)
	print("Check: Has newly added '"+DEMOTENANT+"' tenant (true): "+str(sys_tenant.has_tenant(DEMOTENANT)))

## Use new tenant
print("Creating client connection to new tenant '"+nt+"'...")
tennt = client.tenant(name=nt, dbname='_system', username='root', password='')

## New tenant user funcs
print("'"+nt+"' tenant's user list: "+str(tennt.users()))
print("'"+nt+"' tenant has user 'root' (true): "+str(tennt.has_user('root')))
print("'"+nt+"' tenant properties for user 'root': "+str(tennt.user('root')))

# Add new tenant user


# Create demo user, if needed
if not sys_tenant.has_user(DBUSER):
    print('Creating new user "'+DBUSER+'" to use for demo guest DB...')
    sys_db.create_database(DBNAME, dclist=DCLIST, realtime=REALTIME)
    # TODO : call update_permission on demo DB for demo user
else:
    print('User "'+DBUSER+'", already exists, no need to create.')


# Create a new database for demo, if needed
if not sys_db.has_database(DBNAME):
    print('Creating new guest DB "'+DBNAME+'" to hold demo data...')
    sys_db.create_database(DBNAME, dclist=DCLIST, realtime=REALTIME)
else:
    print('Guest DB "'+DBNAME+'" to hold demo data already exists. No need to create it.')


# call update_permission on demo DB for demo user
sys_db.update_permission(DBUSER, 'rw', DBNAME)


# TODO : Create collections in the demo db
# Connect to "test" database as root user.
print('Connecting to guest DB "'+DBNAME+'"...')
db = client.db(name=DBNAME, username=SYSTEM_ROOT_USERNAME, password=SYSTEM_ROOT_USER_PASSWORD)

# Create a new collection named "students".
print('Creating student collection...')
students = db.create_collection('students')


# TODO : preseed queries

sys_db.update_user(DBUSER, extra=QUERIES)

# TODO : preseed functions
