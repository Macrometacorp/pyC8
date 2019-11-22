from c8 import C8Client
import random

# Variables
fed_url = "gdn1.macrometa.io"
guest_password = "guest1"
guest_mail = "guest1@macrometa.io"
geo_fabric = "testfabric"
collection_name = "addresses" + str(random.randint(1, 10000))

# RESTQLs
value = "INSERT {'firstname':@firstname, 'lastname':@lastname, 'email':@email, 'zipcode':@zipcode, '_key': 'abc'} IN %s" % collection_name
parameter = {"firstname": "", "lastname": "", "email": "", "zipcode": ""}

insert_data = {"query": {"name": "insertRecord", "parameter": parameter, "value": value}} 
get_data = {"query": {"name": "getRecords", "value": "FOR doc IN %s RETURN doc" % collection_name}}
update_data = {"query": {"name": "updateRecord", "value": "UPDATE 'abc' WITH { \"lastname\": \"cena\" } IN %s" % collection_name }}
delete_data= {"query": {"name": "deleteRecord", "value": "REMOVE 'abc' IN %s" % collection_name}}
get_count = {"query": {"name": "countRecords", "value": "RETURN COUNT(FOR doc IN %s RETURN 1)" % collection_name}}

if __name__ == '__main__':

    print("\n ------- CONNECTION SETUP  ------")
    print("tenant: {}, geofabric:{}".format(guest_mail, geo_fabric))
    client = C8Client(protocol='https', host=fed_url, port=443)
    tenant = client.tenant(guest_mail, guest_password)
    fabric = tenant.useFabric(geo_fabric)

    print("Availabile regions....")
    dclist = fabric.dclist(detail=False)
    for dc in dclist:
        print("region: {}".format(dc))
    print("Connected to closest region...\tregion: {}".format(fabric.localdc(detail=False)))

    print("\n ------- CREATE GEO-REPLICATED COLLECTION  ------")
    employees = fabric.create_collection(collection_name)
    print("Created collection: {}".format(collection_name))

    print("\n ------- CREATE RESTQLs  ------")
    fabric.save_restql(insert_data)  # name: insertRecord
    fabric.save_restql(get_data)  # name: getRecords
    fabric.save_restql(update_data)  # name: updateRecord
    fabric.save_restql(delete_data)  # name: deleteRecord
    fabric.save_restql(get_count)  # name: countRecords
    print("Created RESTQLs:{}".format(fabric.get_all_restql()))

    print("\n ------- EXECUTE RESTQLs ------")
    print("Insert data....")
    response = fabric.execute_restql(
        "insertRecord",
        {"bindVars": {"firstname": "john", "lastname": "doe",
                      "email": "john.doe@macrometa.io", "zipcode": "511037"}})
    print("Get data....")
    response = fabric.execute_restql("getRecords")
    print("Update data....")
    response = fabric.execute_restql("updateRecord")
    print("Get data....")
    response = fabric.execute_restql("getRecords")
    print("Count records....")
    response = fabric.execute_restql("countRecords")
    print("Delete data....")
    response = fabric.execute_restql("deleteRecord")

    print("\n ------- DELETE RESTQLs ------")
    fabric.delete_restql("insertRecord")
    fabric.delete_restql("getRecords")
    fabric.delete_restql("updateRecord")
    fabric.delete_restql("countRecords")
    fabric.delete_restql("deleteRecord")

    print("\n ------- DONE  ------")
