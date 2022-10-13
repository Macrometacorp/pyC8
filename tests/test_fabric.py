from __future__ import absolute_import, unicode_literals
from c8.exceptions import (
    FabricCreateError,
    FabricDeleteError,
    FabricPropertiesError,
)
from tests.helpers import assert_raises, generate_fabric_name


def test_fabric_attributes(fabric, client):
    assert fabric.context in ['default', 'async', 'batch', 'transaction']
    assert fabric.tenant_name == client._tenant.name
    assert fabric.name.startswith('test_fabric')
    assert repr(fabric) == '<StandardFabric {}>'.format(fabric.name)


def test_fabric_misc_methods(fabric, client):
    # Test get properties
    properties = fabric.properties()
    assert 'associated_regions' in properties['options']
    assert 'clusters' in properties['options']
    assert properties['name'] == fabric.name
    assert properties['system'] is False
    # Test get properties with bad fabric
    with assert_raises(FabricPropertiesError) as err:
        client._tenant.useFabric(generate_fabric_name()).properties()
    assert err.value.error_code == 11


def test_fabric_management(fabric, client):
    # Test list fabrics
    sys_fabric = client._tenant.useFabric('_system')
    result = sys_fabric.fabrics()
    assert '_system' in result

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
