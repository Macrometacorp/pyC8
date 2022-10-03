from __future__ import absolute_import, unicode_literals

from six import string_types

from c8.exceptions import (
    FabricPropertiesError,
    UserCreateError,
    UserDeleteError,
    UserGetError,
    UserUpdateError,
    C8AuthenticationError
)
from tests.helpers import (
    assert_raises,
    extract,
    generate_username,
    generate_string,
)


def test_user_management(client):
    # Test create user
    display_name = generate_username()
    email = f"{display_name}@macrometa.com"
    password = generate_string()
    new_user = client.create_user(
        email = email,
        display_name=display_name,
        password=password,
        active=True,
        extra={'foo': 'bar'},
    )
    assert new_user['display_name'] == display_name
    assert new_user['email'] == email
    assert new_user['active'] is True
    assert new_user['extra'] == {'foo': 'bar'}

    # Test has user
    username = new_user['username']
    assert client.has_user(username)

    # Test create user using duplicate email
    with assert_raises(UserCreateError) as err:
        client.create_user(
            email = email,
            password=password
        )
    assert err.value.error_code == 100012

    # Test list users
    for user in client.get_users():
        assert isinstance(user['username'], string_types)
        assert isinstance(user['active'], bool)
        assert isinstance(user['extra'], dict)
    assert client.get_user(username) == new_user

    # Test get user
    users = client.get_users()
    for user in users:
        assert 'active' in user
        assert 'extra' in user
        assert 'username' in user
    assert username in extract('username', client.get_users())

    # Test get missing user
    with assert_raises(UserGetError) as err:
        client.get_user(generate_username())
    assert err.value.error_code == 1703

    # Update existing user
    new_user = client.update_user(
        username=username,
        password=password,
        active=False,
        extra={'bar': 'baz'},
    )
    assert new_user['username'] == username
    assert new_user['active'] is False
    assert new_user['extra'] == {'bar': 'baz'}
    assert client.get_user(username) == new_user

    # Update missing user
    with assert_raises(UserUpdateError) as err:
        client.update_user(
            username=generate_username(),
            password=generate_string()
        )
    assert err.value.error_code == 1703

    # Delete an existing user
    assert client.delete_user(username) is True

    # Delete a missing user
    with assert_raises(UserDeleteError) as err:
        client.delete_user(username, ignore_missing=False)
    assert err.value.error_code == 1703
    assert client.delete_user(username, ignore_missing=True) is False


def test_user_change_password(client, sys_fabric):
    display_name = generate_username()
    email = f"{display_name}@macrometa.com"
    password1 = generate_string()
    password2 = generate_string()

    new_user = client.create_user(
        email = email,
        display_name=display_name,
        password=password1,
        active=True,
        extra={'foo': 'bar'},
    )

    username = new_user['username']
    client.set_database_access_level_user(username, sys_fabric.name, 'rw')

    user1 = client.tenant(email, password1)
    db1 = user1.useFabric(sys_fabric.name)

    # Check authentication
    assert isinstance(db1.properties(), dict)
    with assert_raises(C8AuthenticationError) as err:
        client.tenant(email, password2)
    assert "\"errorNum\":401" in str(err.value)

    # Update the user password and check again
    client.update_user(username, password2)
    user2 = client.tenant(email, password2)
    db2 = user2.useFabric(sys_fabric.name)
    assert isinstance(db2.properties(), dict)
    with assert_raises(FabricPropertiesError) as err:
        db1.properties()
    assert err.value.http_code == 401

    # Replace the user password back and check again
    client.update_user(username, password1)
    user1 = client.tenant(email, password1)
    db1 = user1.useFabric(sys_fabric.name)
    assert isinstance(db1.properties(), dict)
    with assert_raises(FabricPropertiesError) as err:
        db2.properties()
    assert err.value.http_code == 401
