from tests.helpers import generate_col_name, clean_doc


def test_create_collection_endpoint(client):
    collection_name = generate_col_name()
    col = client.create_collection(collection_name)
    assert repr(col) == '<StandardCollection {}>'.format(col.name)
    assert col.tenant_name == client._tenant.name
    assert col.name.startswith('test_collection') == True


def test_collection_truncate(sys_fabric, col):
    assert sys_fabric.collection(col.name).truncate() == True


def test_collection_doc_count(sys_fabric, col, docs):
    col.insert_many(docs)
    assert col.count() == len(docs)


def test_collection_has_doc(sys_fabric, col, docs):
    doc_id = col.name + '/' + 'foo'
    col.insert({'_id': doc_id})
    assert sys_fabric.collection(col.name).has(doc_id) == True


def test_collection_export(col, docs):
    col.insert_many(docs)
    cursor = col.export()
    assert cursor == docs

    cursor = col.export(offset=2)
    assert len(cursor) == (len(docs) - 2)

    cursor = col.export(limit=2)
    assert len(cursor) == 2


def test_collection_indexes(col):
    fields = ['lat', 'lng']
    geo_index = col.add_geo_index(fields=fields)
    assert geo_index['fields'] == fields
    assert geo_index['type'] == 'geo'
    assert col.delete_index(geo_index['name']) == True

    fields = ['value']
    hash_index = col.add_hash_index(fields=fields)
    assert hash_index['fields'] == fields
    assert hash_index['type'] == 'hash'
    assert col.delete_index(hash_index['name']) == True

    fields = ['text']
    fulltext_index = col.add_fulltext_index(fields=fields, min_length=10)
    assert fulltext_index['fields'] == fields
    assert fulltext_index['type'] == 'fulltext'
    assert fulltext_index['min_length'] == 10
    assert col.delete_index(fulltext_index['name']) == True

    fields = ['to_skip']
    skiplist_index = col.add_skiplist_index(fields=fields)
    assert skiplist_index['fields'] == fields
    assert skiplist_index['type'] == 'skiplist'
    assert col.delete_index(skiplist_index['name']) == True

    fields = ['key']
    persistent_index = col.add_persistent_index(fields=fields)
    assert persistent_index['fields'] == fields
    assert persistent_index['type'] == 'persistent'
    assert col.delete_index(persistent_index['name']) == True

    fields = ['ttl']
    ttl_index = col.add_ttl_index(fields=fields, expireAfter=60000)
    assert ttl_index['fields'] == fields
    assert ttl_index['type'] == 'ttl'
    assert col.delete_index(ttl_index['name']) == True


def test_delete_collection_endpoint(client):
    collection_name = generate_col_name()
    client.create_collection(collection_name)
    assert client.delete_collection(collection_name) == True


def test_has_collection_endpoint(client):
    collection_name = generate_col_name()
    client.create_collection(collection_name)
    assert True == client.has_collection(collection_name)
    assert False == client.has_collection(generate_col_name())


def test_collection_figures(sys_fabric, client):
    collection_name = generate_col_name()
    col = client.create_collection(collection_name, key_generator='autoincrement')
    fab = client._tenant.useFabric('_system')
    get_col_properties = fab.collection_figures(collection_name=col.name)
    if col.context != 'transaction':
        assert 'id' in get_col_properties
    assert get_col_properties['name'] == col.name
    assert get_col_properties['waitForSync'] is False
    assert get_col_properties['isSystem'] is False
    assert get_col_properties['keyOptions']['type'] == 'autoincrement'
    assert get_col_properties['keyOptions']['allowUserKeys'] is True
    assert get_col_properties['keyOptions']['increment'] == 1
    assert get_col_properties['keyOptions']['offset'] == 0
