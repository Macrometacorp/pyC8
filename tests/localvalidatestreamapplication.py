from c8 import C8Client
print("--- Connecting to C8")
# Simple Way
client = C8Client(protocol='https', host='sankalp-ap-west.eng.macrometa.io', port=443,
                  email='mm@macrometa.io', password='Macrometa123!@#',
                  geofabric='_system')