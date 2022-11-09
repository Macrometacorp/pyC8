from __future__ import absolute_import, unicode_literals

import time

import websocket

from c8.exceptions import (
    BillingAccessLevel,
    ClearBillingAccessLevel,
    ClearCollectionAccessLevel,
    ClearDataBaseAccessLevel,
    ClearStreamAccessLevel,
    CollectionAccessLevel,
    CollectionCreateError,
    CollectionListError,
    CreateAPIKey,
    DataBaseAccessLevel,
    DocumentGetError,
    DocumentInsertError,
    GetAPIKeys,
    GetAttributes,
    ListDataBases,
    ListStreams,
    RemoveAllAttributes,
    RemoveAPIKey,
    RemoveAttribute,
    SetBillingAccessLevel,
    SetCollectionAccessLevel,
    SetDataBaseAccessLevel,
    SetStreamAccessLevel,
    StreamAccessLevel,
    UpdateAttributes,
)
from tests.helpers import (
    assert_raises,
    extract,
    generate_apikey_id,
    generate_col_name,
    generate_fabric_name,
    generate_stream_name,
)


def test_apikey_management(client):
    # Create apikey
    apikey_id = generate_apikey_id()
    resp = client.create_api_key(apikey_id)
    assert resp["error"] is False
    apikey = resp["key"]

    assert apikey_id in extract("keyid", client.list_all_api_keys())

    resp = client.get_api_key(apikey_id)
    assert resp["error"] is False

    user = client.tenant(apikey=apikey)
    sys_fabric = user.useFabric("_system")
    apiKeys = sys_fabric.api_keys(apikey_id)

    tst_fabric_name = generate_fabric_name()
    col_name_1 = generate_col_name()
    col_name_2 = generate_col_name()

    sys_fabric.create_fabric(name=tst_fabric_name, users=["root"])

    # Test update permission (fabric level) to none and verify access
    resp = apiKeys.set_database_access_level(tst_fabric_name, "none")
    assert resp["error"] is False
    assert apiKeys.get_database_access_level(tst_fabric_name) == "none"

    tst_fabric = user.useFabric(tst_fabric_name)
    with assert_raises(CollectionCreateError) as err:
        tst_fabric.create_collection(col_name_1)
    assert err.value.http_code == 401
    with assert_raises(CollectionListError) as err:
        tst_fabric.collections()
    assert err.value.http_code == 401

    # Test update permission (fabric level) to read only and verify access
    resp = apiKeys.set_database_access_level(tst_fabric_name, "ro")
    assert resp["error"] is False
    assert apiKeys.get_database_access_level(tst_fabric_name) == "ro"
    with assert_raises(CollectionCreateError) as err:
        tst_fabric.create_collection(col_name_1)
    assert err.value.http_code == 403
    assert col_name_1 not in extract("name", tst_fabric.collections())

    resp = apiKeys.list_accessible_databases()
    assert resp["_system"] == "rw"
    assert resp[tst_fabric_name] == "ro"

    # Test reset permission (fabric level) and verify access
    assert apiKeys.clear_database_access_level(tst_fabric_name) is True
    assert apiKeys.get_database_access_level(tst_fabric_name) == "none"
    with assert_raises(CollectionCreateError) as err:
        tst_fabric.create_collection(col_name_1)
    assert err.value.http_code == 401
    with assert_raises(CollectionListError) as err:
        tst_fabric.collections()
    assert err.value.http_code == 401

    # Test update permission (fabric level) and verify access
    apiKeys.set_database_access_level(tst_fabric_name, "rw")
    assert apiKeys.get_database_access_level(tst_fabric_name) == "rw"
    assert tst_fabric.create_collection(col_name_1) is not None
    assert col_name_1 in extract("name", tst_fabric.collections())
    assert tst_fabric.create_collection(col_name_2) is not None
    assert col_name_2 in extract("name", tst_fabric.collections())

    col_1 = tst_fabric.collection(col_name_1)
    col_2 = tst_fabric.collection(col_name_2)

    resp = apiKeys.list_accessible_collections(tst_fabric_name)
    assert resp[col_name_1] == "rw"
    assert resp[col_name_2] == "rw"

    # Verify that user has read and write access to the collection
    assert isinstance(col_1.insert({}), dict)
    assert isinstance(col_2.insert({}), dict)

    # Test update permission (collection level) to read only and verify access
    apiKeys.set_collection_access_level(col_name_1, tst_fabric_name, "ro")
    assert apiKeys.get_collection_access_level(col_name_1, tst_fabric_name) == "ro"
    assert isinstance(col_1.export(), list)
    with assert_raises(DocumentInsertError) as err:
        col_1.insert({})
    assert err.value.http_code == 403
    assert isinstance(col_2.insert({}), dict)

    # Test update permission (collection level) to none and verify access
    resp = apiKeys.set_collection_access_level(col_name_1, tst_fabric_name, "none")
    assert resp["error"] is False
    with assert_raises(DocumentGetError) as err:
        col_1.export()
    assert err.value.http_code == 500
    with assert_raises(DocumentInsertError) as err:
        col_1.insert({})
    assert err.value.http_code == 403
    assert isinstance(col_2.export(), list)
    assert isinstance(col_2.insert({}), dict)

    # Test reset permission (collection level)
    assert apiKeys.clear_collection_access_level(col_name_1, tst_fabric_name) is True
    assert apiKeys.get_collection_access_level(col_name_1, tst_fabric_name) == "rw"
    assert isinstance(col_1.insert({}), dict)
    assert isinstance(col_2.insert({}), dict)

    # Test permissions (stream level)
    stream_name_1 = generate_stream_name()
    stream_name_2 = generate_stream_name()
    tst_fabric.create_stream(stream_name_1)
    tst_fabric.create_stream(stream_name_2)

    stream_1 = f"c8globals.{stream_name_1}"
    stream_2 = f"c8globals.{stream_name_2}"

    # Test update permission (stream level) to read only and read write and verify
    # access
    apiKeys.set_stream_access_level(stream_1, tst_fabric_name, "ro")
    assert apiKeys.get_stream_access_level(stream_1, tst_fabric_name) == "ro"
    apiKeys.set_stream_access_level(stream_2, tst_fabric_name, "rw")
    assert apiKeys.get_stream_access_level(stream_2, tst_fabric_name) == "rw"
    stream = tst_fabric.stream()
    time.sleep(2)
    with assert_raises(websocket._exceptions.WebSocketBadStatusException) as err:
        stream.create_producer(stream_name_1)
    assert "401 Unauthorized" in str(err.value)
    stream.subscribe(stream_name_1)

    stream.create_producer(stream_name_2)
    stream.subscribe(stream_name_2)
    resp = apiKeys.list_accessible_streams(tst_fabric_name)
    assert resp[stream_1] == "ro"
    assert resp[stream_2] == "rw"

    # Test reset permission (stream level) and delete stream
    assert apiKeys.clear_stream_access_level(stream_1, tst_fabric_name) is True
    assert apiKeys.get_stream_access_level(stream_1, tst_fabric_name) == "rw"
    time.sleep(2)
    stream.create_producer(stream_name_1)
    stream.create_producer(stream_name_2)
    assert tst_fabric.delete_stream(stream_1) is True
    assert tst_fabric.delete_stream(stream_2) is True

    client._fabric.delete_fabric(tst_fabric_name)

    # Basic tests for billing access level management
    resp = apiKeys.set_billing_access_level("rw")
    assert resp["error"] is False
    resp = apiKeys.get_billing_access_level()
    assert resp["billing"] == "rw"
    resp = apiKeys.clear_billing_access_level()
    assert resp["error"] is False
    resp = apiKeys.get_billing_access_level()
    assert resp["billing"] == "none"

    assert client.remove_api_key(apikey_id) is True


def test_attributes(client):
    # Create apikey
    apikey_id = generate_apikey_id()
    resp = client.create_api_key(apikey_id)
    assert resp["error"] is False
    apikey = resp["key"]

    assert apikey_id in extract("keyid", client.list_all_api_keys())

    resp = client.get_api_key(apikey_id)
    assert resp["error"] is False

    user = client.tenant(apikey=apikey)
    sys_fabric = user.useFabric("_system")
    apiKeys = sys_fabric.api_keys(apikey_id)

    # Tests for user attributes management
    resp = apiKeys.update_attributes('{"foo": "bar", "boo": "baz", "soo": "kon"}')
    assert resp["error"] is False
    resp = apiKeys.get_attributes()
    assert resp["result"] == {"foo": "bar", "boo": "baz", "soo": "kon"}

    resp = apiKeys.update_attributes('{"foo": "baz", "boo": "bar"}')
    assert resp["error"] is False
    resp = apiKeys.get_attributes()
    assert resp["result"] == {"foo": "baz", "boo": "bar", "soo": "kon"}

    resp = apiKeys.remove_attribute("soo")
    assert resp["error"] is False
    resp = apiKeys.get_attributes()
    assert resp["result"] == {"foo": "baz", "boo": "bar"}

    resp = apiKeys.remove_all_attributes()
    assert resp["error"] is False
    resp = apiKeys.get_attributes()
    assert resp["result"] == {}

    assert client.remove_api_key(apikey_id) is True


def test_permission_exceptions(client):
    apikey_id = generate_apikey_id()
    col_name_1 = generate_col_name()
    sys_fabric = client._tenant.useFabric("_system")

    assert client.create_collection(col_name_1) is not None

    # Test invalid apikey
    bad_apikey_id = "apikey-test%"
    with assert_raises(CreateAPIKey) as err:
        client.create_api_key(bad_apikey_id)
    assert err.value.http_code == 400

    with assert_raises(GetAPIKeys) as err:
        client.get_api_key(bad_apikey_id)
    assert err.value.http_code == 404

    with assert_raises(RemoveAPIKey) as err:
        client.remove_api_key(bad_apikey_id)
    assert err.value.http_code == 404

    # Test that with invalid apikey, permissions (fabric level) should not be accessible
    with assert_raises(ListDataBases) as err:
        client.list_accessible_databases(apikey_id)
    assert err.value.http_code == 404

    with assert_raises(DataBaseAccessLevel) as err:
        client.get_database_access_level(apikey_id, sys_fabric.name)
    assert err.value.http_code == 404

    with assert_raises(ClearDataBaseAccessLevel) as err:
        client.clear_database_access_level(apikey_id, sys_fabric.name)
    assert err.value.http_code == 404

    with assert_raises(SetDataBaseAccessLevel) as err:
        client.set_database_access_level(apikey_id, sys_fabric.name, "rw")
    assert err.value.http_code == 404

    # Test that with invalid apikey, permissions (collection level) should not be
    # accessible
    with assert_raises(CollectionAccessLevel) as err:
        client.list_accessible_collections(apikey_id)
    assert err.value.http_code == 404

    with assert_raises(CollectionAccessLevel) as err:
        client.get_collection_access_level(apikey_id, col_name_1)
    assert err.value.http_code == 404

    with assert_raises(SetCollectionAccessLevel) as err:
        client.set_collection_access_level(apikey_id, col_name_1, sys_fabric.name, "rw")
    assert err.value.http_code == 404

    with assert_raises(ClearCollectionAccessLevel) as err:
        client.clear_collection_access_level(apikey_id, col_name_1)
    assert err.value.http_code == 404

    # Test that with invalid apikey, permissions (stream level) should not be accessible
    with assert_raises(ListStreams) as err:
        client.list_accessible_streams(apikey_id)
    assert err.value.http_code == 404

    stream_name_1 = generate_stream_name()
    sys_fabric.create_stream(stream_name_1)

    stream_1 = f"c8globals.{stream_name_1}"

    with assert_raises(StreamAccessLevel) as err:
        client.get_stream_access_level(apikey_id, stream_1)
    assert err.value.http_code == 404

    with assert_raises(SetStreamAccessLevel) as err:
        client.set_stream_access_level(apikey_id, stream_1, sys_fabric.name, "rw")
    assert err.value.http_code == 404

    with assert_raises(ClearStreamAccessLevel) as err:
        client.clear_stream_access_level(apikey_id, stream_1)
    assert err.value.http_code == 404

    # Test that with invalid apikey, permissions (billing level) should not be
    # accessible
    with assert_raises(BillingAccessLevel) as err:
        client.get_billing_access_level(apikey_id)
    assert err.value.http_code == 404

    with assert_raises(SetBillingAccessLevel) as err:
        client.set_billing_access_level(apikey_id, "rw")
    assert err.value.http_code == 404

    with assert_raises(ClearBillingAccessLevel) as err:
        client.clear_billing_access_level(apikey_id)
    assert err.value.http_code == 404

    # Test that with invalid apikey, attributes should not be accessible
    with assert_raises(GetAttributes) as err:
        client.get_attributes(apikey_id)
    assert err.value.http_code == 404

    with assert_raises(UpdateAttributes) as err:
        client.update_attributes(apikey_id, '{"foo": "bar"}')
    assert err.value.http_code == 404

    with assert_raises(RemoveAllAttributes) as err:
        client.remove_all_attributes(apikey_id)
    assert err.value.http_code == 404

    with assert_raises(RemoveAttribute) as err:
        client.remove_attribute(apikey_id, "foo")
    assert err.value.http_code == 404

    time.sleep(0.5)
    assert sys_fabric.delete_stream(stream_1) is True
    assert sys_fabric.delete_collection(col_name_1) is True
