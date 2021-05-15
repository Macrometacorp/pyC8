from c8 import C8Client

print("--- Connecting to C8")
# Simple Way
client = C8Client(protocol='https', host='sankalp-ap-west.eng.macrometa.io', port=443,
                  email='mm@macrometa.io', password='Macrometa123!@#',
                  geofabric='_system')

# To use advanced options
print("Connection estiablished...")
print(client)
print("Get geo fabric details...")
print(client.get_fabric_details())

demo_stream = 'sankalpStream'  #Name of the Stream
print(client.create_stream(demo_stream, local=False))
#print(client.create_stream(demo_stream, local=True))

print("Get Streams: ", client.get_streams())