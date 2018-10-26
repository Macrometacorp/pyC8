from __future__ import absolute_import, unicode_literals

from datetime import datetime

from six import string_types

from c8.exceptions import (
    FabricCreateError,
    FabricDeleteError,
    FabricListError,
    FabricPropertiesError,
    ServerDetailsError,
    ServerEchoError,
    ServerVersionError,
    ServerEngineError
)
from tests.helpers import assert_raises, generate_fabric_name


def test_fabric_attributes(fabric, username):
    assert fabric.context in ['default', 'async', 'batch', 'transaction']
    assert fabric.username == username
    assert fabric.fabric_name == fabric.name
    assert fabric.name.startswith('test_fabric')
    assert repr(fabric) == '<StandardFabric {}>'.format(fabric.name)


def test_fabric_misc_methods(fabric, bad_fabric):
    # Test get properties
    properties = fabric.properties()
    assert 'id' in properties
    assert 'path' in properties
    assert properties['name'] == fabric.name
    assert properties['system'] is False

    # Test get properties with bad fabric
    with assert_raises(FabricPropertiesError) as err:
        bad_fabric.properties()
    assert err.value.error_code == 1228

    # Test get server version
    assert isinstance(fabric.version(), string_types)

    # Test get server version with bad fabric
    with assert_raises(ServerVersionError) as err:
        bad_fabric.version()
    assert err.value.error_code == 1228

    # Test get server details
    details = fabric.details()
    assert 'architecture' in details
    assert 'server-version' in details

    # Test get server details with bad fabric
    with assert_raises(ServerDetailsError) as err:
        bad_fabric.details()
    assert err.value.error_code == 1228

    # Test get storage engine
    engine = fabric.engine()
    assert engine['name'] in ['mmfiles', 'rocksfabric']
    assert 'supports' in engine

    # Test get storage engine with bad fabric
    with assert_raises(ServerEngineError) as err:
        bad_fabric.engine()
    assert err.value.error_code == 1228


def test_fabric_management(fabric, sys_fabric, bad_fabric):
    # Test list fabrics
    result = sys_fabric.fabrics()
    assert '_system' in result

    # Test list fabrics with bad fabric
    with assert_raises(FabricListError):
        bad_fabric.fabrics()

    # Test create fabric
    fabric_name = generate_fabric_name()
    assert sys_fabric.has_fabric(fabric_name) is False
    assert sys_fabric.create_fabric(fabric_name) is True
    assert sys_fabric.has_fabric(fabric_name) is True

    # Test create duplicate fabric
    with assert_raises(FabricCreateError) as err:
        sys_fabric.create_fabric(fabric_name)
    assert err.value.error_code == 1207

    # Test create fabric without permissions
    with assert_raises(FabricCreateError) as err:
        fabric.create_fabric(fabric_name)
    assert err.value.error_code == 1230

    # Test delete fabric without permissions
    with assert_raises(FabricDeleteError) as err:
        fabric.delete_fabric(fabric_name)
    assert err.value.error_code == 1230

    # Test delete fabric
    assert sys_fabric.delete_fabric(fabric_name) is True
    assert fabric_name not in sys_fabric.fabrics()

    # Test delete missing fabric
    with assert_raises(FabricDeleteError) as err:
        sys_fabric.delete_fabric(fabric_name)
    assert err.value.error_code == 1228
    assert sys_fabric.delete_fabric(fabric_name, ignore_missing=True) is False
