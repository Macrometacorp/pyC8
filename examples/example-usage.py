from c8 import C8Client

# Initialize the client connection to C8
print('Creating C8 connection...')
client = C8Client(protocol='http', host='c8.kartik-c2a-ca-central-1.dev.aws.macrometa.io', port=30005)

################ TENANT & USER OPERATIONS ####################
print("Connecting to main system tenant/db...")
sys_tenant = client.tenant(name='_mm', dbname='_system', username='root', password='poweruser')
sys_tenant.tenants()
print("Has _mm tenant (true): "+str(sys_tenant.has_tenant('_mm')))
print("Has notexist tenant (false): "+str(sys_tenant.has_tenant('notexist')))


print("Getting DC list...")
dcl = sys_tenant.dclist()
print('DC List is: '+str(dcl))

# Create new tenant
nt = 'firefly'
print("Creating new tenant '"+nt+"', this may take a while...")
sys_tenant.create_tenant(nt)
print("Check: Has newly added '"+nt+"' tenant (true): "+str(sys_tenant.has_tenant(nt)))

## Use new tenant
print("Creating client connection to new tenant '"+nt+"'...")
tennt = client.tenant(name=nt, dbname='_system', username='root', password='')

## New tenant user funcs
print("'"+nt+"' tenant's user list: "+str(tennt.users()))
print("'"+nt+"' tenant has user 'root' (true): "+str(tennt.has_user('root')))
print("'"+nt+"' tenant properties for user 'root': "+str(tennt.user('root')))

# Add new tenant user
tu = "captain_mal"
print("Creating new user in '"+nt+"' tenant: "+tu)
tennt.create_user(username=tu, password='', active=True)
print("'"+nt+"' tenant has user '"+tu+"' (true): "+str(tennt.has_user(tu)))
# Update tenant user - set a password
print("Updating '"+nt+"' tenant user '"+tu+"' with non-empty password and friendy display name...")
tennt.update_user(username=tu, password='Inara Serra', extra = {"name":"Captain Malcolm Reynolds"})
print("'"+nt+"' tenant properties for user '"+tu+"' after UPDATE: "+str(tennt.user(tu)))

# Wait for keystroke. Uncomment if needed.
#input("Press Enter to continue...")

# Replace the user - unset password, set active=False
print("Replacing '"+nt+"' tenant user '"+tu+"' with empty password and empty friendy display name, active=False...")
tennt.replace_user(username=tu, password='', active=False, extra = {})
print("'"+nt+"' tenant properties for user '"+tu+"' after REPLACE: "+str(tennt.user(tu)))

# Delete tenant user
print("DELETING user in '"+nt+"' tenant: "+tu)
tennt.delete_user(tu)
print("'"+nt+"' tenant has user '"+tu+"' (false): "+str(tennt.has_user(tu)))


# TODO : Tenant user permissions changes

## Delete newly added tenant
print("Deleting tenant '"+nt+"'...")
sys_tenant.delete_tenant(nt)
print("Has deleted '"+nt+"' tenant (false): "+str(sys_tenant.has_tenant(nt)))


# Wait for keystroke. Uncomment if needed.
#input("Press Enter to continue...")


############# DB, COLLECTION AND DOCUMENT OPERATIONS ##################
# Connect to "_system" database as root user.
print('Connecting to C8DB...')
sys_db = client.db(name='_system', username='root', password='')
print("sys_db object: "+str(sys_db))

# Create a new database named "test".
print('Creating new guest DB to hold example data (using dclist="'+str(dcl)+'"...')
sys_db.create_database('test', dclist=dcl)

# Connect to "test" database as root user.
print('Connecting to new guest DB "test"...')
db = client.db(name='test', username='root', password='')

# Create a new collection named "students".
print('Creating student collection...')
students = db.create_collection('students')

# Add a hash index to the collection.
print('Adding hash index to student collection on the name field...')
students.add_hash_index(fields=['name'], unique=True)

# Insert new documents into the collection.
# Insert vertex documents into "students" (from) vertex collection.
print('Inserting new documents into student collection...')
students.insert({'_key': 'STUD01', 'name': 'Jean-Luc Picard'})
students.insert({'_key': 'STUD02', 'name': 'James T. Kirk'})
students.insert({'_key': 'STUD03', 'name': 'Han Solo'})

# Execute a C8QL query and iterate through the result cursor.
print('Executing C8QL query to get student names from the documents we just inserted...')
cursor = db.c8ql.execute('FOR doc IN students RETURN doc')
student_names = [document['name'] for document in cursor]

print("Student names inserted: " + str(student_names))

# Wait for keystroke. Uncomment if needed.
#input("Press Enter to continue...")

##### GRAPHS
print("Creating student->lecture registration graph...")
# Create a new graph named "school".
print('    Creating empty graph...')
graph = db.create_graph('school')

# Create vertex collections for the graph.
print('    Setting vertex collections for graph...')
students = graph.create_vertex_collection('students')
lectures = graph.create_vertex_collection('lectures')

# Create an edge definition (relation) for the graph.
print('    Creating edge definition for graph...')
register = graph.create_edge_definition(
    edge_collection='register',
    from_vertex_collections=['students'],
    to_vertex_collections=['lectures']
)

# Insert vertex documents into "lectures" (to) vertex collection.
print('    Inserting vertex documents into vertex collection "lectures"...')
lectures.insert({'_key': 'MAT101', 'title': 'Calculus'})
lectures.insert({'_key': 'STA101', 'title': 'Statistics'})
lectures.insert({'_key': 'CSC101', 'title': 'Algorithms'})

# Insert edge documents into "register" edge collection.
print('    Inserting edge documents into edge collection for vertex collections "students" and "lectures"...')
register.insert({'_from': 'students/STUD01', '_to': 'lectures/MAT101'})
register.insert({'_from': 'students/STUD01', '_to': 'lectures/STA101'})
register.insert({'_from': 'students/STUD01', '_to': 'lectures/CSC101'})
register.insert({'_from': 'students/STUD02', '_to': 'lectures/MAT101'})
register.insert({'_from': 'students/STUD02', '_to': 'lectures/STA101'})
register.insert({'_from': 'students/STUD03', '_to': 'lectures/CSC101'})

# Traverse the graph in outbound direction, breadth-first.
print('    Traversing graph...')
result = graph.traverse(
    start_vertex='students/STUD01',
    direction='outbound',
    strategy='breadthfirst'
)

print("Student graph traversal result: ")
print(result)
