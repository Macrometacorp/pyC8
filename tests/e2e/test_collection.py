import pytest


@pytest.mark.vcr
def test_create_collection_endpoint(client, tst_fabric_name):
    collection_name = "test_collection_e2e_collection_1"
    client._tenant.useFabric(tst_fabric_name)
    col = client.create_collection(collection_name)
    assert repr(col) == "<StandardCollection {}>".format(col.name)
    assert col.tenant_name == client._tenant.name
    assert col.name.startswith("test_collection") is True


@pytest.mark.vcr
def test_collection_truncate(sys_fabric, col):
    assert sys_fabric.collection(col.name).truncate() is True


@pytest.mark.vcr
def test_collection_doc_count(col, docs):
    col.insert_many(docs)
    assert col.count() == len(docs)


@pytest.mark.vcr
def test_collection_has_doc(sys_fabric, col, docs):
    doc_id = col.name + "/" + "foo"
    col.insert({"_id": doc_id})
    assert sys_fabric.collection(col.name).has(doc_id) is True


@pytest.mark.vcr
def test_collection_export(col, docs):
    col.insert_many(docs)
    cursor = col.export()
    assert cursor == docs

    cursor = col.export(offset=2)
    assert len(cursor) == (len(docs) - 2)

    cursor = col.export(limit=2)
    assert len(cursor) == 2


@pytest.mark.vcr
def test_collection_indexes(client, tst_fabric_name):
    collection_name = "test_collection_e2e_collection_2"
    client._tenant.useFabric(tst_fabric_name)
    col = client.create_collection(collection_name)
    fields = ["lat", "lng"]

    geo_index = col.add_geo_index(fields=fields)
    assert geo_index["fields"] == fields
    assert geo_index["type"] == "geo"
    assert col.delete_index(geo_index["name"]) is True

    fields = ["value"]
    hash_index = col.add_hash_index(fields=fields)
    assert hash_index["fields"] == fields
    assert hash_index["type"] == "hash"
    assert col.delete_index(hash_index["name"]) is True

    fields = ["text"]
    fulltext_index = col.add_fulltext_index(fields=fields, min_length=10)
    assert fulltext_index["fields"] == fields
    assert fulltext_index["type"] == "fulltext"
    assert fulltext_index["min_length"] == 10
    assert col.delete_index(fulltext_index["name"]) is True

    fields = ["to_skip"]
    skiplist_index = col.add_skiplist_index(fields=fields)
    assert skiplist_index["fields"] == fields
    assert skiplist_index["type"] == "skiplist"
    assert col.delete_index(skiplist_index["name"]) is True

    fields = ["key"]
    persistent_index = col.add_persistent_index(fields=fields)
    assert persistent_index["fields"] == fields
    assert persistent_index["type"] == "persistent"
    assert col.delete_index(persistent_index["name"]) is True

    fields = ["ttl"]
    ttl_index = col.add_ttl_index(fields=fields, expireAfter=60000)
    assert ttl_index["fields"] == fields
    assert ttl_index["type"] == "ttl"
    assert col.delete_index(ttl_index["name"]) is True


@pytest.mark.vcr
def test_delete_collection_endpoint(client, tst_fabric_name):
    collection_name = "test_collection_e2e_collection_3"
    client._tenant.useFabric(tst_fabric_name)
    client.create_collection(collection_name)
    assert client.delete_collection(collection_name) is True


@pytest.mark.vcr
def test_has_collection_endpoint(client, tst_fabric_name):
    collection_name = collection_name = "test_collection_e2e_collection_4"
    client._tenant.useFabric(tst_fabric_name)
    client.create_collection(collection_name)
    assert True is client.has_collection(collection_name)
    assert False is client.has_collection("test_collection_e2e_collection_5")


@pytest.mark.vcr
def test_collection_figures(client, tst_fabric_name):
    collection_name = "test_collection_e2e_collection_6"
    fab = client._tenant.useFabric(tst_fabric_name)
    col = client.create_collection(collection_name, key_generator="autoincrement")
    get_col_properties = fab.collection(col.name).collection_figures()
    if col.context != "transaction":
        assert "id" in get_col_properties
    assert get_col_properties["name"] == col.name
    assert get_col_properties["waitForSync"] is False
    assert get_col_properties["isSystem"] is False
    assert get_col_properties["keyOptions"]["type"] == "autoincrement"
    assert get_col_properties["keyOptions"]["allowUserKeys"] is True
    assert get_col_properties["keyOptions"]["increment"] == 1
    assert get_col_properties["keyOptions"]["offset"] == 0
