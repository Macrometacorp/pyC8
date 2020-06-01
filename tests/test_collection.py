from __future__ import absolute_import, unicode_literals

from six import string_types

from c8.collection import StandardCollection
from c8.exceptions import (
    CollectionConfigureError,
    CollectionPropertiesError,
    CollectionTruncateError,
    CollectionCreateError,
    CollectionListError,
    CollectionDeleteError,
)
from tests.helpers import assert_raises, extract, generate_col_name


def test_collection_attributes(db, col, username):
    assert col.context in ['default', 'async', 'batch', 'transaction']
    assert col.username == username
    assert col.db_name == db.name
    assert col.name.startswith('test_collection')
    assert repr(col) == '<StandardCollection {}>'.format(col.name)


def test_collection_misc_methods(col, bad_col):
    # Test get properties
    properties = col.properties()
    assert properties['name'] == col.name
    assert properties['system'] is False

    # Test get properties with bad collection
    with assert_raises(CollectionPropertiesError) as err:
        bad_col.properties()
    assert err.value.error_code == 1228

    # Test configure properties
    prev_sync = properties['sync']
    properties = col.configure(
        sync=not prev_sync,
        journal_size=10000000
    )
    assert properties['name'] == col.name
    assert properties['system'] is False
    assert properties['sync'] is not prev_sync

    # Test configure properties with bad collection
    with assert_raises(CollectionConfigureError) as err:
        bad_col.configure(sync=True, journal_size=10000000)
    assert err.value.error_code == 1228

    # Test preconditions
    assert len(col) == 1

    # Test truncate collection
    assert col.truncate() is True
    assert len(col) == 0


def test_collection_management(db, bad_db):
    # Test create collection
    col_name = generate_col_name()
    assert db.has_collection(col_name) is False

    col = db.create_collection(
        name=col_name,
        sync=True,
        compact=False,
        journal_size=7774208,
        system=False,
        volatile=False,
        key_generator='autoincrement',
        user_keys=False,
        key_increment=9,
        key_offset=100,
        edge=True,
        shard_count=2,
        shard_fields=['test_attr'],
        index_bucket_count=10,
        replication_factor=1,
        shard_like='',
        sync_replication=False,
        enforce_replication_factor=False
    )
    assert db.has_collection(col_name) is True

    properties = col.properties()
    if col.context != 'transaction':
        assert 'id' in properties
    assert properties['name'] == col_name
    assert properties['sync'] is True
    assert properties['system'] is False
    assert properties['key_generator'] == 'autoincrement'
    assert properties['user_keys'] is False
    assert properties['key_increment'] == 9
    assert properties['key_offset'] == 100

    # Test create duplicate collection
    with assert_raises(CollectionCreateError) as err:
        db.create_collection(col_name)
    assert err.value.error_code == 1207

    # Test list collections
    assert all(
        entry['name'].startswith('test_collection')
        or entry['name'].startswith('_')
        for entry in db.collections()
    )

    # Test list collections with bad fabric
    with assert_raises(CollectionListError) as err:
        bad_db.collections()
    assert err.value.error_code == 1228

    # Test get collection object
    test_col = db.collection(col.name)
    assert isinstance(test_col, StandardCollection)
    assert test_col.name == col.name

    test_col = db[col.name]
    assert isinstance(test_col, StandardCollection)
    assert test_col.name == col.name

    # Test delete collection
    assert db.delete_collection(col_name, system=False) is True
    assert col_name not in extract('name', db.collections())

    # Test drop missing collection
    with assert_raises(CollectionDeleteError) as err:
        db.delete_collection(col_name)
    assert err.value.error_code == 1203
    assert db.delete_collection(col_name, ignore_missing=True) is False

