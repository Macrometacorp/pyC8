from c8.client import C8Client
client = C8Client(protocol='https', host='india2-ap-southeast-2.dev.aws.macrometa.io', port=443)
fabric = client.fabric(tenant='tp', name='db1', username='root', password='poweruser')

# Get the total document count in "students" collection.
document_count = fabric.collection('students').count()

# Execute a C8QL query normally (without using transactions).
cursor1 = fabric.c8ql.execute('FOR doc IN students RETURN doc', batch_size=1)

# Execute the same C8QL query in a transaction.
txn_fabric = fabric.begin_transaction()
job = txn_fabric.c8ql.execute('FOR doc IN students RETURN doc', batch_size=1)
txn_fabric.commit()
cursor2 = job.result()

# The first cursor acts as expected. Its current batch contains only 1 item
# and it still needs to fetch the rest of its result set from the server.
assert len(cursor1.batch()) == 1
assert cursor1.has_more() is True

# The second cursor is pre-loaded with the entire result set, and does not
# require further communication with C8 Data Fabric server. Note that value of
# parameter "batch_size" was ignored.
assert len(cursor2.batch()) == document_count
assert cursor2.has_more() is False