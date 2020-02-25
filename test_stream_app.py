# to import from local pyc8 uncomment following two lines and 
# specify path of your local pyc8 code
# import sys
# sys.path.append('<path-to-your-local/pyC8>')

from c8 import C8Client

# Variables
fed_url = "qa1.eng3.macrometa.io"
guest_password = "Macrometa123!@#"
guest_mail = "mm@macrometa.io"
geo_fabric = "_system"

if __name__ == "__main__":
    client = C8Client(protocol='https', host=fed_url, port=443)
    
    tenant = client.tenant(guest_mail, guest_password)
    fabric = tenant.useFabric(geo_fabric)
    dclist = fabric.dclist(detail=False)

    print(dclist)
    
    # data to pass to stream app apis
    data = {
        "definition": "@App:name('Sample-Cargo-App')\r\n\r\n-- Stream\r\ndefine stream srcCargoStream (weight int);\r\n\r\n-- Table\r\ndefine table destCargoTable (weight int, totalWeight long);\r\n\r\n-- Data Processing\r\n@info(name='Query')\r\nselect weight, sum(weight) as totalWeight\r\nfrom srcCargoStream\r\ninsert into destCargoTable;",
        "regions":dclist
    }

    # stream apps apis added in fabric
    # res = fabric.get_samples_stream_app() -- worked
    # res = fabric.validate_stream_app(data) -- worked

    # stream apps apis added in stream apps class
    app = fabric.stream_app("Sample-Cargo-App")
    # res = fabric.create_stream_app(data) -- worked
    # res = app.delete() -- worked
    # res = app.update(data) -- worked
    # res = app.change_state(True) -- not worked
    res = app.query("") # -- no sample data to test this 
    print(res)