from __future__ import absolute_import, unicode_literals
from c8.exceptions import (
    IndexCreateError,
    IndexDeleteError,
)
from tests.helpers import assert_raises, extract


def test_add_index(col):
    fields = ['attr1']
    # Test create skiplist index
    data = {'type': 'skiplist', 'fields': fields, 'unique': False, 'sparse': True, 'deduplicate': True}
    resp = col.add_index(data)

    expected_index = {
        'sparse': True,
        'type': 'skiplist',
        'fields': ['attr1'],
        'unique': False,
        'deduplicate': True,
        'new': True
    }
    for key, value in expected_index.items():
        assert resp[key] == value

    resp.pop('new', None)
    assert resp in col.indexes()

    # Test if unique set to true in global collection
    data = {'type': 'skiplist', 'fields': fields, 'unique': True, 'sparse': True, 'deduplicate': True}
    with assert_raises(IndexCreateError) as err:
        col.add_index(data)
    assert err.value.error_code == 400

    # Test if unique set to false in global collection
    data = {'type': 'hash', 'fields': fields, 'unique': False, 'sparse': True, 'deduplicate': True}
    resp = col.add_index(data)
    expected_index = {
        'selectivity': 1,
        'sparse': True,
        'type': 'hash',
        'fields': ['attr1'],
        'unique': False,
        'deduplicate': True,
        'new': True,
    }
    for key, value in expected_index.items():
        assert resp[key] == value

    resp.pop('new', None)
    assert resp in col.indexes()

    # Test add_ttl_index
    data = {'type': 'ttl', 'fields': fields, 'expireAfter': 0,
            'inBackground': False}
    resp = col.add_index(data)
    # Test add ttl index result
    expected_index = {
        'sparse': True,
        'type': 'ttl',
        'fields': ['attr1'],
        'unique': False,
        'expireAfter': 0,
        'selectivity': 1,
    }

    for key, value in expected_index.items():
        assert resp[key] == value

    resp.pop('new', None)
    assert resp in col.indexes()
    assert col.delete_index(resp['name']) == True

    # Test add_ttl_index with 2 fields
    fields = ['attr1', 'attr2']
    data = {'type': 'ttl', 'fields': fields, 'expireAfter': 0, 'inBackground': False}
    with assert_raises(IndexCreateError) as err:
        col.add_index(data)
    assert err.value.error_code == 10


def test_get_index(col):
    index = col.get_index('primary')
    expected_index = {
        'id': f'{col.name}/0',
        'sparse': False,
        'type': 'primary',
        'fields': ['_key'],
        'unique': True,
        'name': 'primary',
    }
    assert expected_index['sparse'] == index['sparse']
    assert expected_index['type'] == index['type']
    assert expected_index['fields'] == index['fields']
    assert expected_index['unique'] == index['unique']
    assert expected_index['name'] == index['name']


def test_list_indexes(col):
    # Test default primary index
    expected_index = {
        'id': '0',
        'selectivity': 1,
        'sparse': False,
        'type': 'primary',
        'fields': ['_key'],
        'unique': True,
        'name': 'primary'
    }
    indexes = col.indexes()
    assert isinstance(indexes, list)
    assert expected_index in indexes


def test_add_hash_index(col):
    fields = ['attr1', 'attr2']
    # Test if unique set to true in global collection
    with assert_raises(IndexCreateError) as err:
        col.add_hash_index(fields=fields, unique=True, sparse=True, deduplicate=True)
    assert err.value.error_code == 400

    # Test if unique set to false in global collection
    result = col.add_hash_index(fields=fields, unique=False, sparse=True, deduplicate=True)
    expected_index = {
        'selectivity': 1,
        'sparse': True,
        'type': 'hash',
        'fields': ['attr1', 'attr2'],
        'unique': False,
        'deduplicate': True,
        'new': True,
    }
    for key, value in expected_index.items():
        assert result[key] == value

    result.pop('new', None)
    assert result in col.indexes()


def test_add_skiplist_index(col):
    fields = ['attr1', 'attr2']
    # Test if unique set to true in global collection
    with assert_raises(IndexCreateError) as err:
        col.add_skiplist_index(fields=fields, unique=True, sparse=True, deduplicate=True)
    assert err.value.error_code == 400

    # Test if unique set to false in global collection
    result = col.add_skiplist_index(
        fields=fields,
        unique=False,
        sparse=True,
        deduplicate=True
    )
    expected_index = {
        'sparse': True,
        'type': 'skiplist',
        'fields': ['attr1', 'attr2'],
        'unique': False,
        'deduplicate': True,
        'new': True
    }
    for key, value in expected_index.items():
        assert result[key] == value

    result.pop('new', None)
    assert result in col.indexes()


def test_add_geo_index(col):
    # Test add geo index with one attribute
    result = col.add_geo_index(
        fields=['attr1'],
        ordered=False
    )

    expected_index = {
        'sparse': True,
        'type': 'geo',
        'fields': ['attr1'],
        'unique': False,
        'geo_json': False,
        'new': True,
    }
    for key, value in expected_index.items():
        assert result[key] == value

    result.pop('new', None)
    assert result in col.indexes()

    # Test add geo index with two attributes
    result = col.add_geo_index(
        fields=['attr1', 'attr2'],
        ordered=False,
    )
    expected_index = {
        'sparse': True,
        'type': 'geo',
        'fields': ['attr1', 'attr2'],
        'unique': False,
    }
    for key, value in expected_index.items():
        assert result[key] == value

    result.pop('new', None)
    assert result in col.indexes()

    # Test add geo index with more than two attributes (should fail)
    with assert_raises(IndexCreateError) as err:
        col.add_geo_index(fields=['attr1', 'attr2', 'attr3'])
    assert err.value.error_code == 10


def test_add_fulltext_index(col):
    # Test add fulltext index with one attributes
    result = col.add_fulltext_index(
        fields=['attr1'],
        min_length=10,
    )
    expected_index = {
        'sparse': True,
        'type': 'fulltext',
        'fields': ['attr1'],
        'min_length': 10,
        'unique': False,
    }
    for key, value in expected_index.items():
        assert result[key] == value

    result.pop('new', None)
    assert result in col.indexes()

    # Test add fulltext index with two attributes (should fail)
    with assert_raises(IndexCreateError) as err:
        col.add_fulltext_index(fields=['attr1', 'attr2'])
    assert err.value.error_code == 10


def test_add_persistent_index(col):
    fields = ['attr1', 'attr2']
    # Test if unique set to true in global collection
    with assert_raises(IndexCreateError) as err:
        col.add_persistent_index(fields=fields, unique=True, sparse=True)
    assert err.value.error_code == 400

    # Test add persistent index with two attributes
    result = col.add_persistent_index(
        fields=fields,
        unique=False,
        sparse=True,
    )
    expected_index = {
        'sparse': True,
        'type': 'persistent',
        'fields': ['attr1', 'attr2'],
        'unique': False,
    }
    for key, value in expected_index.items():
        assert result[key] == value

    result.pop('new', None)
    assert result in col.indexes()


def test_add_ttl_index(col):
    fields = ['attr1']
    # Test add_ttl_index
    result = col.add_ttl_index(fields=fields, expireAfter=1)
    # Test add ttl index result
    expected_index = {
        'sparse': True,
        'type': 'ttl',
        'fields': ['attr1'],
        'unique': False,
        'expireAfter': 1,
        'selectivity': 1,
    }

    for key, value in expected_index.items():
        assert result[key] == value

    result.pop('new', None)
    assert result in col.indexes()
    # Test add_ttl_index with 2 fields
    with assert_raises(IndexCreateError) as err:
        col.add_ttl_index(fields=['attr1', 'attr2'], expireAfter=10)
    assert err.value.error_code == 10


def test_delete_index(col, bad_col):
    old_indexes = set(extract('name', col.indexes()))
    col.add_hash_index(['attr3', 'attr4'], unique=False)
    col.add_skiplist_index(['attr3', 'attr4'], unique=False)
    col.add_fulltext_index(fields=['attr3'], min_length=10)

    indexes = col.indexes()
    new_indexes = set(extract('name', indexes))
    assert new_indexes.issuperset(old_indexes)
    indexes_to_delete = new_indexes - old_indexes
    for index_name in indexes_to_delete:
        assert col.delete_index(index_name) is True

    new_indexes = set(extract('name', col.indexes()))
    assert new_indexes == old_indexes

    # Test delete missing indexes
    for index_id in indexes_to_delete:
        assert col.delete_index(index_id, ignore_missing=True) is False
    for index_id in indexes_to_delete:
        with assert_raises(IndexDeleteError) as err:
            col.delete_index(index_id, ignore_missing=False)
        assert err.value.error_code == 1212
