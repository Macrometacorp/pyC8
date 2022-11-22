# flake8: noqa
from __future__ import absolute_import, unicode_literals

import time

import pytest
import websocket

from c8.exceptions import (
    BillingAccessLevel,
    ClearBillingAccessLevel,
    ClearCollectionAccessLevel,
    ClearDataBaseAccessLevel,
    ClearStreamAccessLevel,
    CollectionAccessLevel,
    DataBaseError,
    GetAttributes,
    GetDataBaseAccessLevel,
    ListStreams,
    RemoveAllAttributes,
    RemoveAttribute,
    SetBillingAccessLevel,
    SetCollectionAccessLevel,
    SetDataBaseAccessLevel,
    SetStreamAccessLevel,
    StreamAccessLevel,
    UpdateAttributes,
)
from tests.helpers import assert_raises, extract

# def test_permission_management(client, sys_fabric):
#     # Create user
#     display_name = generate_username()
#     email = f"{display_name}@macrometa.com"
#     password = generate_string()
#     new_user = client.create_user(
#         email=email,
#         display_name=display_name,
#         password=password,
#         active=True,
#         extra={"foo": "bar"},
#     )
#
#     username = new_user["username"]
#     assert client.has_user(username)
#
#     sys_fabric = client._tenant.useFabric("_system")
#     tst_fabric_name = generate_fabric_name()
#     col_name_1 = generate_col_name()
#     col_name_2 = generate_col_name()
#     col_name_3 = generate_col_name()
#
#     sys_fabric.create_fabric(name=tst_fabric_name, users=["root"])
#
#     assert isinstance(client.get_permissions(username), dict)
#
#     # Test update permission (fabric level) to none and verify access
#     resp = client.set_database_access_level_user(username, tst_fabric_name, "none")
#     assert resp["error"] is False
#     assert client.get_database_access_level_user(username, tst_fabric_name) == "none"
#     user_1 = client.tenant(email, password)
#     assert resp["error"] is False
#     tst_fabric = user_1.useFabric(tst_fabric_name)
#     with assert_raises(CollectionCreateError) as err:
#         tst_fabric.create_collection(col_name_2)
#     assert err.value.http_code == 401
#     with assert_raises(CollectionListError) as err:
#         tst_fabric.collections()
#     assert err.value.http_code == 401
#
#     assert sys_fabric.create_collection(col_name_1) is not None
#     assert col_name_1 in extract("name", sys_fabric.collections())
#     resp = client.list_accessible_databases_user(username)
#     assert resp["_system"] == "ro"
#
#     # Test update permission (fabric level) to read only and verify access
#     resp = client.set_database_access_level_user(username, tst_fabric_name, "ro")
#     assert resp["error"] is False
#     assert client.get_database_access_level_user(username, tst_fabric_name) == "ro"
#     with assert_raises(CollectionCreateError) as err:
#         tst_fabric.create_collection(col_name_2)
#     assert err.value.http_code == 403
#     assert col_name_1 in extract("name", sys_fabric.collections())
#     assert col_name_2 not in extract("name", tst_fabric.collections())
#
#     # Test reset permission (fabric level) and verify access
#     assert client.remove_database_access_level_user(username, tst_fabric_name) is True
#     assert client.get_database_access_level_user(username, tst_fabric_name) == "none"
#     with assert_raises(CollectionCreateError) as err:
#         tst_fabric.create_collection(col_name_1)
#     assert err.value.http_code == 401
#     with assert_raises(CollectionListError) as err:
#         tst_fabric.collections()
#     assert err.value.http_code == 401
#
#     # Test update permission (fabric level) and verify access
#     client.set_database_access_level_user(username, tst_fabric_name, "rw")
#     assert client.get_database_access_level_user(username, tst_fabric_name) == "rw"
#     assert tst_fabric.create_collection(col_name_2) is not None
#     assert col_name_2 in extract("name", tst_fabric.collections())
#     assert tst_fabric.create_collection(col_name_3) is not None
#     assert col_name_3 in extract("name", tst_fabric.collections())
#
#     col_2 = tst_fabric.collection(col_name_2)
#     col_3 = tst_fabric.collection(col_name_3)
#
#     resp = client.list_accessible_collections_user(username, tst_fabric_name)
#     assert resp[col_name_2] == "rw"
#     assert resp[col_name_3] == "rw"
#
#     # Verify that user has read and write access to the collection
#     assert isinstance(col_2.insert({}), dict)
#     assert isinstance(col_3.insert({}), dict)
#
#     # Test update permission (collection level) to read only and verify access
#     client.set_collection_access_level_user(username, col_name_2, tst_fabric_name, "ro")
#     assert (
#         client.get_collection_access_level_user(username, col_name_2, tst_fabric_name)
#         == "ro"
#     )
#     assert isinstance(col_2.export(), list)
#     with assert_raises(DocumentInsertError) as err:
#         col_2.insert({})
#     assert err.value.http_code == 403
#     assert isinstance(col_3.insert({}), dict)
#
#     # Test update permission (collection level) to none and verify access
#     client.set_collection_access_level_user(
#         username, col_name_2, tst_fabric_name, "none"
#     )
#     assert (
#         client.get_collection_access_level_user(username, col_name_2, tst_fabric_name)
#         == "none"
#     )
#     with assert_raises(DocumentGetError) as err:
#         col_2.export()
#     assert err.value.http_code == 403
#     with assert_raises(DocumentInsertError) as err:
#         col_2.insert({})
#     assert err.value.http_code == 403
#     assert isinstance(col_3.export(), list)
#     assert isinstance(col_3.insert({}), dict)
#
#     # Test reset permission (collection level)
#     assert (
#         client.clear_collection_access_level_user(username, col_name_2, tst_fabric_name)
#         is True
#     )
#     assert (
#         client.get_collection_access_level_user(username, col_name_2, tst_fabric_name)
#         == "rw"
#     )
#     assert isinstance(col_2.insert({}), dict)
#     assert isinstance(col_3.insert({}), dict)
#
#     # Test permissions (stream level)
#     stream_name_1 = generate_stream_name()
#     stream_name_2 = generate_stream_name()
#     tst_fabric.create_stream(stream_name_1)
#     tst_fabric.create_stream(stream_name_2)
#
#     stream_1 = f"c8globals.{stream_name_1}"
#     stream_2 = f"c8globals.{stream_name_2}"
#
#     # Test update permission (stream level) to read only and read write and verify access
#     client.set_stream_access_level_user(username, stream_1, tst_fabric_name, "ro")
#     assert (
#         client.get_stream_access_level_user(username, stream_1, tst_fabric_name) == "ro"
#     )
#     client.set_stream_access_level_user(username, stream_2, tst_fabric_name, "rw")
#     assert (
#         client.get_stream_access_level_user(username, stream_2, tst_fabric_name) == "rw"
#     )
#     stream = tst_fabric.stream()
#     time.sleep(2)
#     with assert_raises(websocket._exceptions.WebSocketBadStatusException) as err:
#         stream.create_producer(stream_name_1)
#     assert "401 Unauthorized" in str(err.value)
#     stream.subscribe(stream_name_1)
#
#     stream.create_producer(stream_name_2)
#     stream.subscribe(stream_name_2)
#     resp = client.list_accessible_streams_user(username, tst_fabric_name)
#     assert resp[stream_1] == "ro"
#     assert resp[stream_2] == "rw"
#
#     # Test reset permission (stream level) and delete stream
#     assert (
#         client.clear_stream_access_level_user(username, stream_1, tst_fabric_name)
#         is True
#     )
#     assert (
#         client.get_stream_access_level_user(username, stream_1, tst_fabric_name) == "rw"
#     )
#     time.sleep(2)
#     stream.create_producer(stream_name_1)
#     stream.create_producer(stream_name_2)
#     assert tst_fabric.delete_stream(stream_1) is True
#     assert tst_fabric.delete_stream(stream_2) is True
#
#     sys_fabric = client._tenant.useFabric("_system")
#     sys_fabric.delete_fabric(tst_fabric_name)
#     sys_fabric.delete_collection(col_name_1)
#
#     # Basic tests for billing access level management
#     resp = client.set_billing_access_level_user(username, "rw")
#     assert resp["error"] is False
#     resp = client.get_billing_access_level_user(username)
#     assert resp["billing"] == "rw"
#     resp = client.clear_billing_access_level_user(username)
#     assert resp["error"] is False
#     resp = client.get_billing_access_level_user(username)
#     assert resp["billing"] == "none"
#     assert client.delete_user(username) is True


@pytest.mark.vcr
def test_attributes_user(client):
    # Create user
    display_name = "test_user_permission_1"
    email = f"{display_name}@macrometa.com"
    password = "Sdk@123456##"
    new_user = client.create_user(
        email=email,
        display_name=display_name,
        password=password,
        active=True,
        extra={"foo": "bar"},
    )

    username = new_user["username"]
    assert client.has_user(username)

    # Tests for user attributes management
    resp = client.update_attributes_user(
        username, '{"foo": "bar", "boo": "baz", "soo": "kon"}'
    )
    assert resp["error"] is False
    resp = client.get_attributes_user(username)
    assert resp["result"] == {"foo": "bar", "boo": "baz", "soo": "kon"}

    resp = client.update_attributes_user(username, '{"foo": "baz", "boo": "bar"}')
    assert resp["error"] is False
    resp = client.get_attributes_user(username)
    assert resp["result"] == {"foo": "baz", "boo": "bar", "soo": "kon"}

    resp = client.remove_attribute_user(username, "soo")
    assert resp["error"] is False
    resp = client.get_attributes_user(username)
    assert resp["result"] == {"foo": "baz", "boo": "bar"}

    resp = client.remove_all_attributes_user(username)
    assert resp["error"] is False
    resp = client.get_attributes_user(username)
    assert resp["result"] == {}

    assert client.delete_user(username) is True


@pytest.mark.vcr
def test_permission_exceptions(client, sys_fabric):
    # Create user
    username = "test_user_permission_2"
    display_name = "test_user_permission_3"
    email = f"{display_name}@macrometa.com"
    password = "Sdk@123456##"
    new_user = client.create_user(
        email=email,
        display_name=display_name,
        password=password,
        active=True,
        extra={"foo": "bar"},
    )

    username_2 = new_user["username"]
    assert client.has_user(username_2)
    user = client.tenant(email, password)

    col_name_1 = "test_collection_permission_1"
    assert sys_fabric.create_collection(col_name_1) is not None

    # Test that missing users should not be able to access permissions (fabric level)
    with assert_raises(DataBaseError) as err:
        client.list_accessible_databases_user(username)
    assert err.value.http_code == 404

    with assert_raises(GetDataBaseAccessLevel) as err:
        client.get_database_access_level_user(username, sys_fabric.name)
    assert err.value.http_code == 404

    with assert_raises(ClearDataBaseAccessLevel) as err:
        client.remove_database_access_level_user(username, sys_fabric.name)
    assert err.value.http_code == 404

    with assert_raises(SetDataBaseAccessLevel) as err:
        client.set_database_access_level_user(username, sys_fabric.name, "rw")
    assert err.value.http_code == 404

    # Test that missing users should not be able to access permissions (collection level)
    with assert_raises(CollectionAccessLevel) as err:
        client.list_accessible_collections_user(username)
    assert err.value.http_code == 404

    with assert_raises(CollectionAccessLevel) as err:
        client.get_collection_access_level_user(username, col_name_1)
    assert err.value.http_code == 404

    with assert_raises(SetCollectionAccessLevel) as err:
        client.set_collection_access_level_user(
            username, col_name_1, sys_fabric.name, "rw"
        )
    assert err.value.http_code == 404

    with assert_raises(ClearCollectionAccessLevel) as err:
        client.clear_collection_access_level_user(username, col_name_1)
    assert err.value.http_code == 404

    # Test that missing users should not be able to access permissions (stream level)
    with assert_raises(ListStreams) as err:
        client.list_accessible_streams_user(username)
    assert err.value.http_code == 404

    stream_name_1 = "test_stream_permission_1"
    sys_fabric.create_stream(stream_name_1)

    stream_1 = f"c8globals.{stream_name_1}"

    with assert_raises(StreamAccessLevel) as err:
        client.get_stream_access_level_user(username, stream_1)
    assert err.value.http_code == 404

    with assert_raises(SetStreamAccessLevel) as err:
        client.set_stream_access_level_user(username, stream_1, sys_fabric.name, "rw")
    assert err.value.http_code == 404

    with assert_raises(ClearStreamAccessLevel) as err:
        client.clear_stream_access_level_user(username, stream_1)
    assert err.value.http_code == 404

    # Test that missing users should not be able to access permissions (billing level)
    with assert_raises(BillingAccessLevel) as err:
        client.get_billing_access_level_user(username)
    assert err.value.http_code == 404

    with assert_raises(SetBillingAccessLevel) as err:
        client.set_billing_access_level_user(username, "rw")
    assert err.value.http_code == 404

    with assert_raises(ClearBillingAccessLevel) as err:
        client.clear_billing_access_level_user(username)
    assert err.value.http_code == 404

    # Test that only root users can access attributes
    with assert_raises(GetAttributes) as err:
        user.get_attributes_user(username)
    assert err.value.http_code == 403

    with assert_raises(UpdateAttributes) as err:
        user.update_attributes_user(username, '{"foo": "bar"}')
    assert err.value.http_code == 403

    with assert_raises(RemoveAllAttributes) as err:
        user.remove_all_attributes_user(username)
    assert err.value.http_code == 403

    with assert_raises(RemoveAttribute) as err:
        user.remove_attribute_user(username, "foo")
    assert err.value.http_code == 403

    time.sleep(0.5)
    assert sys_fabric.delete_stream(stream_1) is True
    assert sys_fabric.delete_collection(col_name_1) is True
