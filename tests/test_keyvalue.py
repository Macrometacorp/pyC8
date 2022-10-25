from __future__ import absolute_import, unicode_literals

from c8.exceptions import (
    CreateCollectionError,
    DeleteCollectionError,
    DeleteEntryForKey,
    GetCountError,
    GetKeysError,
    GetKVError,
    GetValueError,
    InsertKVError,
    ListCollections,
    RemoveKVError,
)
from tests.helpers import assert_raises, extract, generate_col_name


def test_keyvalue_methods(client, tst_fabric_name):
    client._tenant.useFabric(tst_fabric_name)
    col_name = generate_col_name()
    key_values = [
        {"_key": "1", "value": "foo"},
        {"_key": "2", "value": "bar"},
        {"_key": "3", "value": "foobar"},
    ]

    assert client.create_collection_kv(col_name) is True
    assert client.has_collection_kv(col_name) is True
    assert col_name in extract("name", client.get_collections_kv())

    client.insert_key_value_pair(col_name, key_values)
    resp = client.get_key_value_pairs(col_name)
    assert resp["result"][0]["value"] == "foo"

    resp = client.get_key_value_pairs(col_name, offset=1, limit=1)
    assert resp["result"][0]["value"] == "bar"

    resp = client.get_value_for_key(col_name, "1")
    assert resp["value"] == "foo"

    resp = client.get_keys(col_name)
    assert resp[0] == "1"
    assert resp[1] == "2"
    assert resp[2] == "3"

    assert client.get_kv_count(col_name) == 3
    client.delete_entry_for_keys(col_name, key_values)
    assert client.get_kv_count(col_name) == 0

    client.insert_key_value_pair(col_name, key_values)
    assert client.delete_entry_for_key(col_name, "3") is True
    assert client.get_kv_count(col_name) == 2

    assert client.remove_key_value_pairs(col_name) is True
    assert client.get_kv_count(col_name) == 0
    resp = client.get_key_value_pairs(col_name, offset=0, limit=2)
    assert resp["result"] == []

    assert client.delete_collection_kv(col_name) is True
    assert client.has_collection_kv(col_name) is False


def test_keyvalue_exceptions(client, tst_fabric_name, bad_fabric_name):
    bad_col_name = generate_col_name()
    col_name = generate_col_name()
    key_values = [{"_key": "1"}, {"_key": "2"}, {"_key": "3"}]

    # Tests with bad fabric (non existing)
    client._tenant.useFabric(bad_fabric_name)

    with assert_raises(CreateCollectionError):
        client.create_collection_kv(bad_col_name)

    with assert_raises(ListCollections):
        client.has_collection_kv(bad_col_name)

    with assert_raises(ListCollections):
        client.get_collections_kv()

    # Tests with good fabric but bad collection
    client._tenant.useFabric(tst_fabric_name)
    with assert_raises(DeleteCollectionError) as err:
        client.delete_collection_kv(col_name)
    assert err.value.http_code == 404

    client.create_collection_kv(col_name)

    # Tests with missing value
    with assert_raises(InsertKVError) as err:
        client.insert_key_value_pair(col_name, key_values)
    assert err.value.http_code == 403

    # Tests with invalid collection
    with assert_raises(GetKVError) as err:
        client.get_key_value_pairs(bad_col_name)
    assert err.value.http_code == 404

    with assert_raises(GetValueError) as err:
        client.get_value_for_key(bad_col_name, "1")
    assert err.value.http_code == 404

    with assert_raises(GetKeysError) as err:
        client.get_keys(bad_col_name)
    assert err.value.http_code == 404

    with assert_raises(GetCountError) as err:
        client.get_kv_count(bad_col_name)
    assert err.value.http_code == 404

    with assert_raises(DeleteEntryForKey) as err:
        client.delete_entry_for_keys(bad_col_name, key_values)
    assert err.value.http_code == 404

    with assert_raises(DeleteEntryForKey) as err:
        client.delete_entry_for_key(bad_col_name, "3")
    assert err.value.http_code == 404

    with assert_raises(RemoveKVError) as err:
        client.remove_key_value_pairs(bad_col_name)
    assert err.value.http_code == 404
