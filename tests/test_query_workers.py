from __future__ import absolute_import, unicode_literals
import time

from c8.exceptions import (
    RestqlCreateError,
    RestqlImportError,
    RestqlListError,
    RestqlExecuteError,
    RestqlUpdateError,
    RestqlCursorError,
    RestqlDeleteError
)
from tests.helpers import (
    assert_raises
)

def test_restql_methods(client, tst_fabric_name, col):
    client._tenant.useFabric(tst_fabric_name)

    input_docs = [
        { "_key": "james.kirk@mafabriccrometa.io", "firstname": "James", "lastname": "Kirk", "email": "james.kirk@mafabriccrometa.io", "zipcode": "12312"},
        { "_key": "han.solo@macrfabricometa.io", "firstname": "Han", "lastname": "Solo", "email": "han.solo@macrfabricometa.io", "zipcode": "12311"},
        { "_key": "bruce.wayne@mfabricacrometa.io", "firstname": "Bruce", "lastname": "Wayne", "email": "bruce.wayne@mfabricacrometa.io", "zipcode": "12345" }
    ]
    update_keys = ["james.kirk@mafabriccrometa.io", "bruce.wayne@mfabricacrometa.io"]
    update_key_value = {
        "bruce.wayne@mfabricacrometa.io": { "key": "bruce.wayne@mfabricacrometa.io", "zipcode": "22222" },
        "james.kirk@mafabriccrometa.io": { "key": "james.kirk@mafabriccrometa.io", "zipcode": "55555"}
    }
    insert_data_query = (
        f"FOR doc in @InputDocs INSERT {{'firstname':doc.firstname, 'lastname':doc.lastname, 'email':doc.email, 'zipcode':doc.zipcode, '_key': doc._key}} IN {col.name}"
    )
    get_data_query = f"FOR doc IN {col.name} RETURN doc"
    update_data_query = (
        f"FOR i IN {col.name} FILTER i._key IN @updateKeys UPDATE i with {{ zipcode: (i._key == @updateKeyValue[i._key].key) ? @updateKeyValue[i._key].zipcode : i.zipcode }} IN {col.name}"
    )
    updated_insert_query = (
        f"INSERT {{'_key': 'barry.allen@macrometa.io', 'value': 'Barry Allen'}} IN {col.name}"
    )

    insert_data = {
        "query": {
            "name": "insertRecord",
            "value": insert_data_query,
        }
    }
    updated_insert_data = {
        "query": {
            "value": updated_insert_query,
        }
    }

    get_data = {"name": "getRecords", "value": get_data_query}
    update_data = {"name": "updateRecord", "value": update_data_query}
    queries = [get_data, update_data]

    # Create and import queries
    client.create_restql(insert_data)
    client.import_restql(queries)
    time.sleep(2)

    resp = str(client.get_restqls())
    assert "getRecords" in resp
    assert "updateRecord" in resp
    assert "insertRecord" in resp

    client.execute_restql(
        "insertRecord", {"bindVars": {"InputDocs": input_docs}}
    )

    resp = client.execute_restql("getRecords")
    result = resp['result']
    assert input_docs[0]['_key'] == result[0]['_key']
    assert input_docs[1]['_key'] == result[1]['_key']

    client.execute_restql(
        "updateRecord",
        {"bindVars": {"updateKeys": update_keys, "updateKeyValue": update_key_value}}
    )
    resp = client.execute_restql("getRecords")
    result = resp['result']
    assert result[1]['zipcode'] == "22222"
    assert result[2]['zipcode'] == "55555"

    # Updating restqls
    client.update_restql("insertRecord", updated_insert_data)
    time.sleep(2)
    client.execute_restql("insertRecord")
    resp = client.execute_restql(
        "getRecords",
        {"bindVars": {}, "batchSize": 2}
    )

    # Read next batch from cursor
    id = resp['id']
    resp = client.read_next_batch_restql(id)
    assert resp['result'][1]['_key'] == "barry.allen@macrometa.io"

    # Deleting RestQls
    assert client.delete_restql("insertRecord") is True
    assert client.delete_restql("getRecords") is True
    assert client.delete_restql("updateRecord") is True
    assert col.truncate() is True

def test_restql_exceptions(client, tst_fabric_name, col, bad_fabric_name):
    client._tenant.useFabric(tst_fabric_name)

    update_data_query = (
        f"FOR i IN {col.name} FILTER i._key IN @updateKeys UPDATE i with {{ zipcode: (i._key == @updateKeyValue[i._key].key) ? @updateKeyValue[i._key].zipcode : i.zipcode }} IN {col.name}"
    )
    updated_insert_query = (
        f"INSERT {{'_key': 'barry.allen@macrometa.io', 'value': 'Barry Allen'}} IN {col.name}"
    )

    insert_data = {
        "query": {
            "name": "insertRecord"
        }
    }
    updated_insert_data = {
        "query": {
            "value": updated_insert_query,
        }
    }

    get_data = {"name": "getRecords"}
    update_data = {"name": "updateRecord", "value": update_data_query}
    queries = [get_data, update_data]

    # Create query with empty value
    with assert_raises(RestqlCreateError) as err:
        client.create_restql(insert_data)
    assert err.value.http_code == 400

    with assert_raises(RestqlImportError) as err:
        client.import_restql(queries)
    assert err.value.http_code == 400

    resp = str(client.get_restqls())
    assert "getRecords" not in resp
    assert "updateRecord" not in resp
    assert "insertRecord" not in resp

    # Get restqls from invalid fabric
    client._tenant.useFabric(bad_fabric_name)
    with assert_raises(RestqlListError):
        client.get_restqls()

    # Delete restql from invalid fabric
    with assert_raises(RestqlDeleteError):
        client.delete_restql("insertRecord")

    client._tenant.useFabric(tst_fabric_name)

    # Execute non existing restql
    with assert_raises(RestqlExecuteError) as err:
        client.execute_restql("getRecords")
    assert err.value.http_code == 400

    # Update non existing restql
    with assert_raises(RestqlUpdateError) as err:
        client.update_restql("insertRecord", updated_insert_data)
    assert err.value.http_code == 500

    # Read next batch from non existing cursor
    with assert_raises(RestqlCursorError) as err:
        client.read_next_batch_restql(1)
    assert err.value.http_code == 404
