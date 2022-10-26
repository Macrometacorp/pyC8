from __future__ import absolute_import, unicode_literals

from c8.collection import StandardCollection
from c8.exceptions import (
    CollectionCreateError,
    CollectionDeleteError,
    CollectionFindError,
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
        tst_fabric.get_collection_information(collection_name=generate_col_name())


def test_collection_figures(client, col, tst_fabric_name):
    # Test get properties
    tst_fabric = client._tenant.useFabric(tst_fabric_name)
    collection = tst_fabric.collection(col.name)
    get_col_properties = collection.collection_figures()
    assert get_col_properties["name"] == collection.name
    assert get_col_properties["isSystem"] is False
    with assert_raises(CollectionFindError):
        tst_fabric.collection_figures(collection_name=generate_col_name())


def test_collection_attributes(client, col, tst_fabric):
    assert col.context in ["default", "async", "batch", "transaction"]
    assert col.tenant_name == client._tenant.name
    assert col.fabric_name == tst_fabric.name
    assert col.name.startswith("test_collection") is True
    assert repr(col) == "<StandardCollection {}>".format(col.name)


def test_collection_misc_methods(col, tst_fabric):
    # Test get properties
    get_col_properties = tst_fabric.collection_figures(collection_name=col.name)
    assert get_col_properties["name"] == col.name
    assert get_col_properties["isSystem"] is False
    # Test get properties with bad collection
    with assert_raises(CollectionFindError):
        tst_fabric.collection_figures(collection_name=generate_col_name())

    # # Test configure properties
    prev_sync = get_col_properties["waitForSync"]
    prev_has_stream = get_col_properties["hasStream"]

    properties = tst_fabric.update_collection_properties(
        collection_name=col.name, has_stream=True, wait_for_sync=True
    )
    assert properties["name"] == col.name
    assert properties["isSystem"] is False
    assert properties["waitForSync"] is not prev_sync
    assert properties["hasStream"] is not prev_has_stream

    properties = tst_fabric.update_collection_properties(
        collection_name=col.name, wait_for_sync=False
    )
    assert properties["name"] == col.name
    assert properties["isSystem"] is False
    assert properties["waitForSync"] is False
    assert properties["hasStream"] is True

    # Test configure properties with bad collection
    with assert_raises(CollectionPropertiesError) as err:
        tst_fabric.update_collection_properties(
            collection_name=generate_col_name(), wait_for_sync=True
        )
    assert err.value.error_code == 1203

    # Test preconditions
    doc_id = col.name + "/" + "foo"
    tst_fabric.collection(col.name).insert({"_id": doc_id})
    assert len(col) == 1

    # Test truncate collection
    assert col.truncate() is True
    assert len(col) == 0


def test_collection_management(sys_fabric, client, bad_fabric):
    # Test create collection
    sys_fabric = client._tenant.useFabric("_system")
    col_name = generate_col_name()
    assert sys_fabric.has_collection(col_name) is False

    col = sys_fabric.create_collection(
        name=col_name,
        sync=False,
        edge=False,
        user_keys=True,
        key_increment=None,
        key_offset=None,
        key_generator="autoincrement",
        shard_fields=None,
        index_bucket_count=None,
        sync_replication=None,
        enforce_replication_factor=None,
        spot_collection=False,
        local_collection=False,
        is_system=False,
        stream=False,
    )
    assert sys_fabric.has_collection(col_name) is True

    get_col_properties = sys_fabric.collection_figures(collection_name=col.name)
    if col.context != "transaction":
        assert "id" in get_col_properties
    assert get_col_properties["name"] == col_name
    assert get_col_properties["waitForSync"] is False
    assert get_col_properties["isSystem"] is False
    assert get_col_properties["keyOptions"]["type"] == "autoincrement"
    assert get_col_properties["keyOptions"]["allowUserKeys"] is True
    assert get_col_properties["keyOptions"]["increment"] == 1
    assert get_col_properties["keyOptions"]["offset"] == 0

    # Test create duplicate collection
    with assert_raises(CollectionCreateError) as err:
        sys_fabric.create_collection(col_name)
    assert err.value.error_code == 1207

    # Test list collections
    assert col_name in extract("name", sys_fabric.collections())
    bad = client._tenant.useFabric(bad_fabric)
    # Test list collections with bad fabric
    with assert_raises(CollectionListError):
        bad.collections()

    # Test get collection object
    sys_fabric = client._tenant.useFabric("_system")
    test_col = sys_fabric.collection(col.name)
    assert isinstance(test_col, StandardCollection)
    assert test_col.name == col.name

    test_col = sys_fabric[col.name]
    assert isinstance(test_col, StandardCollection)
    assert test_col.name == col.name

    # Test delete collection
    assert sys_fabric.delete_collection(col_name, system=False) is True
    assert col_name not in extract("name", sys_fabric.collections())

    # Test drop missing collection
    with assert_raises(CollectionDeleteError) as err:
        sys_fabric.delete_collection(col_name)
    assert err.value.error_code == 1203
    assert sys_fabric.delete_collection(col_name, ignore_missing=True) is False
