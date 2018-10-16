from __future__ import absolute_import, unicode_literals

from datetime import datetime

from six import string_types

from c8.exceptions import (
    DatabaseCreateError,
    DatabaseDeleteError,
    DatabaseListError,
    DatabasePropertiesError,
    ServerDetailsError,
    ServerEchoError,
    ServerVersionError,
    ServerEngineError
)
from tests.helpers import assert_raises, generate_db_name


def test_database_attributes(db, username):
    assert db.context in ['default', 'async', 'batch', 'transaction']
    assert db.username == username
    assert db.db_name == db.name
    assert db.name.startswith('test_database')
    assert repr(db) == '<StandardDatabase {}>'.format(db.name)


def test_database_misc_methods(db, bad_db):
    # Test get properties
    properties = db.properties()
    assert 'id' in properties
    assert 'path' in properties
    assert properties['name'] == db.name
    assert properties['system'] is False

    # Test get properties with bad database
    with assert_raises(DatabasePropertiesError) as err:
        bad_db.properties()
    assert err.value.error_code == 1228

    # Test get server version
    assert isinstance(db.version(), string_types)

    # Test get server version with bad database
    with assert_raises(ServerVersionError) as err:
        bad_db.version()
    assert err.value.error_code == 1228

    # Test get server details
    details = db.details()
    assert 'architecture' in details
    assert 'server-version' in details

    # Test get server details with bad database
    with assert_raises(ServerDetailsError) as err:
        bad_db.details()
    assert err.value.error_code == 1228

    # Test get storage engine
    engine = db.engine()
    assert engine['name'] in ['mmfiles', 'rocksdb']
    assert 'supports' in engine

    # Test get storage engine with bad database
    with assert_raises(ServerEngineError) as err:
        bad_db.engine()
    assert err.value.error_code == 1228


def test_database_management(db, sys_db, bad_db):
    # Test list databases
    result = sys_db.databases()
    assert '_system' in result

    # Test list databases with bad database
    with assert_raises(DatabaseListError):
        bad_db.databases()

    # Test create database
    db_name = generate_db_name()
    assert sys_db.has_database(db_name) is False
    assert sys_db.create_database(db_name) is True
    assert sys_db.has_database(db_name) is True

    # Test create duplicate database
    with assert_raises(DatabaseCreateError) as err:
        sys_db.create_database(db_name)
    assert err.value.error_code == 1207

    # Test create database without permissions
    with assert_raises(DatabaseCreateError) as err:
        db.create_database(db_name)
    assert err.value.error_code == 1230

    # Test delete database without permissions
    with assert_raises(DatabaseDeleteError) as err:
        db.delete_database(db_name)
    assert err.value.error_code == 1230

    # Test delete database
    assert sys_db.delete_database(db_name) is True
    assert db_name not in sys_db.databases()

    # Test delete missing database
    with assert_raises(DatabaseDeleteError) as err:
        sys_db.delete_database(db_name)
    assert err.value.error_code == 1228
    assert sys_db.delete_database(db_name, ignore_missing=True) is False
