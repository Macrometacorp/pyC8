from __future__ import absolute_import, unicode_literals

from c8.exceptions import (
    FabricCreateError,
    FabricDeleteError,
    FabricGetMetadataError,
    FabricPropertiesError,
    FabricSetMetadataError,
    FabricUpdateMetadataError,
    GetDcDetailError,
)
from tests.helpers import assert_raises, extract, generate_fabric_name


def test_fabric_attributes(fabric, client):
    assert fabric.context in ["default", "async", "batch", "transaction"]
    assert fabric.tenant_name == client._tenant.name
    assert fabric.name.startswith("test_fabric")
    assert repr(fabric) == "<StandardFabric {}>".format(fabric.name)


def test_fabric_misc_methods(fabric, client):
    # Test get properties
    properties = fabric.properties()
    assert "associated_regions" in properties["options"]
    assert "clusters" in properties["options"]
    assert properties["name"] == fabric.name
    assert properties["system"] is False
    # Test get properties with bad fabric
    with assert_raises(FabricPropertiesError):
        client._tenant.useFabric(generate_fabric_name()).properties()

    # Test fabric details method
    details = fabric.fabrics_detail()
    assert "associated_regions" in details[0]["options"]
    assert "dcList" in details[0]["options"]
    assert "dynamo_local_tables" in details[0]["options"]
    assert "realTime" in details[0]["options"]
    assert "spotDc" in details[0]["options"]
    assert "status" in details[0]["options"]

    dc_list = fabric.dclist()
    assert len(dc_list) > 0
    local_dc = fabric.localdc()
    assert local_dc["_key"] in dc_list


def test_fabric_management(fabric, client):
    # Test list fabrics
    sys_fabric = client._tenant.useFabric("_system")
    result = sys_fabric.fabrics()
    assert "_system" in result

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


def test_metadata(client, tst_fabric_name):
    # Test get fabric metadata
    tst_fabric = client._tenant.useFabric(tst_fabric_name)
    resp = tst_fabric.get_fabric_metadata()
    assert resp["options"]["metadata"] is None

    # Test set fabric metadata
    assert tst_fabric.set_fabric_metadata({"foo": "bar"}) is True
    resp = tst_fabric.get_fabric_metadata()
    assert resp["options"]["metadata"] == {"foo": "bar"}

    # Test update fabric metadata
    assert tst_fabric.update_fabric_metadata({"foo": "baz"}) is True
    resp = tst_fabric.get_fabric_metadata()
    assert resp["options"]["metadata"] == {"foo": "baz"}


def test_datacenter(sys_fabric):
    # Test datacenter methods
    dclist = sys_fabric.dclist(detail=False)
    localdc = sys_fabric.localdc(detail=False)
    assert localdc in dclist
    resp = sys_fabric.get_dc_detail(localdc)
    assert resp["name"] == localdc
    assert resp["local"] is True
    assert localdc in extract("name", sys_fabric.dclist_all())

    # Test invalid dc name
    with assert_raises(GetDcDetailError) as err:
        sys_fabric.get_dc_detail("invalid")
    assert err.value.http_code == 404


def test_bad_fabric(client, bad_fabric_name):
    bad_fabric = client._tenant.useFabric(bad_fabric_name)
    fabric_name = generate_fabric_name()

    # Test create and delete functions on bad fabric
    with assert_raises(FabricCreateError) as err:
        bad_fabric.create_fabric(fabric_name)
    assert err.value.error_code == 1228

    with assert_raises(FabricDeleteError) as err:
        bad_fabric.delete_fabric(fabric_name)
    assert err.value.error_code == 1228

    # Test metadata functions on bad fabric
    with assert_raises(FabricGetMetadataError) as err:
        bad_fabric.get_fabric_metadata()
    assert err.value.error_code == 1228

    with assert_raises(FabricSetMetadataError) as err:
        bad_fabric.set_fabric_metadata({"foo": "bar"})
    assert err.value.error_code == 1228

    with assert_raises(FabricUpdateMetadataError) as err:
        bad_fabric.update_fabric_metadata({"foo": "baz"})
    assert err.value.error_code == 1228

    client._tenant.useFabric("_system")
