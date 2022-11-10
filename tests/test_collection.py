# flake8: noqa
from __future__ import absolute_import, unicode_literals

import json
import os

from c8.collection import StandardCollection
from c8.exceptions import (
    CollectionCreateError,
    CollectionDeleteError,
    CollectionFindError,
    CollectionImportFromFileError,
    CollectionListError,
    CollectionPropertiesError,
)
from tests.helpers import assert_raises, extract, generate_col_name


def test_get_collection_information(client, col, tst_fabric_name):
    tst_fabric = client._tenant.useFabric(tst_fabric_name)
    collection = tst_fabric.collection(col.name)
    # Test get information about collection
    get_col_info = collection.get_collection_information()
    assert get_col_info["error"] is False
    assert get_col_info["name"] == collection.name
    with assert_raises(CollectionFindError):
        tst_fabric.collection(generate_col_name()).get_collection_information()


def test_collection_figures(client, col, tst_fabric_name):
    # Test get properties
    tst_fabric = client._tenant.useFabric(tst_fabric_name)
    collection = tst_fabric.collection(col.name)
    get_col_properties = collection.collection_figures()
    assert get_col_properties["name"] == collection.name
    assert get_col_properties["isSystem"] is False
    with assert_raises(CollectionFindError):
        tst_fabric.collection(generate_col_name()).collection_figures()


def test_collection_attributes(client, col, tst_fabric):
    assert col.context in ["default", "async", "batch", "transaction"]
    assert col.tenant_name == client._tenant.name
    assert col.fabric_name == tst_fabric.name
    assert col.name.startswith("test_collection") is True
    assert repr(col) == "<StandardCollection {}>".format(col.name)


# def test_collection_misc_methods(col, tst_fabric):
#     # Test get properties
#     get_col_properties = tst_fabric.collection(col.name).collection_figures()
#     assert get_col_properties["name"] == col.name
#     assert get_col_properties["isSystem"] is False
#     # Test get properties with bad collection
#     with assert_raises(CollectionFindError):
#         tst_fabric.collection(generate_col_name()).collection_figures()
#
#     # Test configure properties
#     prev_sync = get_col_properties["waitForSync"]
#     prev_has_stream = get_col_properties["hasStream"]
#
#     properties = tst_fabric.update_collection_properties(
#         collection_name=col.name, has_stream=True, wait_for_sync=True
#     )
#     assert properties["name"] == col.name
#     assert properties["isSystem"] is False
#     assert properties["waitForSync"] is not prev_sync
#     assert properties["hasStream"] is not prev_has_stream
#
#     properties = tst_fabric.update_collection_properties(
#         collection_name=col.name, wait_for_sync=False
#     )
#     assert properties["name"] == col.name
#     assert properties["isSystem"] is False
#     assert properties["waitForSync"] is False
#     assert properties["hasStream"] is True
#
#     # Test configure properties with bad collection
#     with assert_raises(CollectionPropertiesError) as err:
#         tst_fabric.update_collection_properties(
#             collection_name=generate_col_name(), wait_for_sync=True
#         )
#     assert err.value.error_code == 1203
#
#     # Test preconditions
#     doc_id = col.name + "/" + "foo"
#     tst_fabric.collection(col.name).insert({"_id": doc_id})
#     assert len(col) == 1
#
#     # Test truncate collection
#     assert col.truncate() is True
#     assert len(col) == 0


# def test_collection_management(tst_fabric, client, bad_fabric):
#     # Test create collection
#     col_name = generate_col_name()
#     assert tst_fabric.has_collection(col_name) is False
#
#     col = tst_fabric.create_collection(
#         name=col_name,
#         sync=False,
#         edge=False,
#         user_keys=True,
#         key_increment=None,
#         key_offset=None,
#         key_generator="autoincrement",
#         shard_fields=None,
#         index_bucket_count=None,
#         sync_replication=None,
#         enforce_replication_factor=None,
#         spot_collection=False,
#         local_collection=False,
#         is_system=False,
#         stream=False,
#     )
#     assert tst_fabric.has_collection(col_name) is True
#
#     get_col_properties = tst_fabric.collection(col.name).collection_figures()
#     if col.context != "transaction":
#         assert "id" in get_col_properties
#     assert get_col_properties["name"] == col_name
#     assert get_col_properties["waitForSync"] is False
#     assert get_col_properties["isSystem"] is False
#     assert get_col_properties["keyOptions"]["type"] == "autoincrement"
#     assert get_col_properties["keyOptions"]["allowUserKeys"] is True
#     assert get_col_properties["keyOptions"]["increment"] == 1
#     assert get_col_properties["keyOptions"]["offset"] == 0
#
#     # Test create duplicate collection
#     with assert_raises(CollectionCreateError) as err:
#         tst_fabric.create_collection(col_name)
#     assert err.value.error_code == 1207
#
#     # Test list collections
#     assert col_name in extract("name", tst_fabric.collections())
#     bad = client._tenant.useFabric(bad_fabric)
#     # Test list collections with bad fabric
#     with assert_raises(CollectionListError):
#         bad.collections()
#
#     # Test get collection object
#     test_col = tst_fabric.collection(col.name)
#     assert isinstance(test_col, StandardCollection)
#     assert test_col.name == col.name
#
#     test_col = tst_fabric[col.name]
#     assert isinstance(test_col, StandardCollection)
#     assert test_col.name == col.name
#
#     # Test delete collection
#     assert tst_fabric.delete_collection(col_name, system=False) is True
#     assert col_name not in extract("name", tst_fabric.collections())
#
#     # Test drop missing collection
#     with assert_raises(CollectionDeleteError) as err:
#         tst_fabric.delete_collection(col_name)
#     assert err.value.error_code == 1203
#     assert tst_fabric.delete_collection(col_name, ignore_missing=True) is False


def test_insert_from_file(client, tst_fabric_name, col):
    absolute_path = os.path.dirname(__file__)
    json_path = os.path.join(absolute_path, "files/data.json")
    csv_path = os.path.join(absolute_path, "files/data.csv")
    invalid_file_path = os.path.join(absolute_path, "files/data")
    file = open(json_path)
    documents = json.load(file)

    client._tenant.useFabric(tst_fabric_name)
    client.insert_document_from_file(collection_name=col.name, filepath=json_path)

    data = client.collection(collection_name=col.name).export(limit=len(documents))
    entries = ("_id", "_key", "_rev")
    for doc in data:
        for key in entries:
            if key in doc:
                del doc[key]

    assert documents == data
    col.truncate()

    client.insert_document_from_file(collection_name=col.name, filepath=csv_path)
    data = client.collection(collection_name=col.name).export(limit=len(documents))

    assert len(data) == len(documents)
    col.truncate()

    with assert_raises(CollectionImportFromFileError) as err:
        client.insert_document_from_file(
            collection_name=col.name, filepath=invalid_file_path
        )
    assert (
        str(err)
        == "<ExceptionInfo CollectionImportFromFileError('Invalid file') tblen=3>"
    )
    file.close()


def test_all_documents(client, col, tst_fabric_name):
    document_count = 2003
    client._tenant.useFabric(tst_fabric_name)
    client.execute_query(
        query="FOR doc IN 1..{} INSERT {{value:doc}} INTO {}".format(
            document_count, col.name
        )
    )
    resp = client.get_all_documents(collection_name=col.name)

    assert document_count == len(resp)
    for i in range(len(resp)):
        assert resp[i]["value"] == i + 1
    col.truncate()

    document_count = 11
    client.execute_query(
        query="FOR doc IN 1..{} INSERT {{value:doc}} INTO {}".format(
            document_count, col.name
        )
    )
    resp = client.get_all_documents(collection_name=col.name)
    assert document_count == len(resp)
    for i in range(len(resp)):
        assert resp[i]["value"] == i + 1
