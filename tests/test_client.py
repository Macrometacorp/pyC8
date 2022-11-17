from __future__ import absolute_import, unicode_literals

import os

import pytest
import requests
from dotenv import load_dotenv

from c8.client import C8Client
from c8.exceptions import C8AuthenticationError, StreamListError
from c8.http import DefaultHTTPClient
from c8.version import __version__
from tests.helpers import assert_raises, generate_string, generate_username

load_dotenv()


@pytest.mark.vcr
def test_client_attributes():
    session = DefaultHTTPClient()
    client = C8Client(
        protocol="https",
        host=os.environ.get("FEDERATION_URL"),
        port=443,
        apikey=os.environ.get("API_KEY"),
        geofabric=os.environ.get("FABRIC"),
        http_client=session,
    )
    assert client.version == __version__
    assert client.protocol == "https"
    assert client.host == os.environ.get("FEDERATION_URL")
    assert client.port == 443
    assert client.base_url == "https://api-{}:443".format(
        os.environ.get("FEDERATION_URL")
    )
    assert client._apikey == os.environ.get("API_KEY")
    assert client._fabric_name == os.environ.get("FABRIC")
    assert repr(client) == "<C8Client https://api-{}:443>".format(
        os.environ.get("FEDERATION_URL")
    )


@pytest.mark.vcr
def test_client_bad_connection(username, password):
    client = C8Client(
        protocol="https",
        host=os.environ.get("FEDERATION_URL"),
        port=443,
        apikey=os.environ.get("API_KEY"),
        geofabric=os.environ.get("FABRIC"),
    )

    bad_username = "test_user_bad_1"
    bad_password = "Sdk@1234!"

    # Test connection with bad username password
    with assert_raises(C8AuthenticationError) as err:
        client.tenant(bad_username, bad_password)
    assert "Failed to Authenticate the C8DB user" in str(err.value)

    # Test connection with invalid host URL
    client._url = "http://127.0.0.1:8500"
    with assert_raises(requests.exceptions.ConnectionError) as err:
        client.tenant(username, password)
    assert "Failed to establish a new connection" in str(err.value)


def test_client_custom_http_client():
    # Define custom HTTP client which increments the counter on any API call.
    class MyHTTPClient(DefaultHTTPClient):
        def __init__(self):
            super(MyHTTPClient, self).__init__()
            self.counter = 0

        def send_request(
            self, method, url, headers=None, params=None, data=None, auth=None
        ):
            self.counter += 1
            return super(MyHTTPClient, self).send_request(
                method, url, headers, params, data, auth
            )

    http_client = MyHTTPClient()
    client = C8Client(
        protocol="https",
        host=os.environ.get("FEDERATION_URL"),
        port=443,
        apikey=os.environ.get("API_KEY"),
        geofabric=os.environ.get("FABRIC"),
        http_client=http_client,
    )

    # Test calling any api even on bad response the counter should increment
    with assert_raises(StreamListError) as err:
        client.get_streams()
    assert err.value.http_code == 401
    assert http_client.counter == 1


@pytest.mark.vcr
def test_get_jwt(client):
    client._tenant.useFabric("_system")
    user = client.create_user(
        email="{}@macrometa.io".format("test_user_client_1"),
        password="121@Macrometa",
    )

    resp = client.get_jwt(
        email=user["email"],
        password="121@Macrometa",
        tenant=client._tenant.name,
        username=user["username"],
    )

    assert "jwt" in resp
    assert resp["username"] == user["username"]
    assert resp["tenant"] == user["tenant"]

    resp = client.get_jwt(email=user["email"], password="121@Macrometa")

    assert "jwt" in resp
    assert resp["username"] == user["username"]
    assert resp["tenant"] == user["tenant"]

    resp = client.get_jwt(
        tenant=user["tenant"], password="121@Macrometa", username=user["username"]
    )

    assert "jwt" in resp
    assert resp["username"] == user["username"]
    assert resp["tenant"] == user["tenant"]

    with assert_raises(C8AuthenticationError):
        client.get_jwt(password="121@Macrometa")

    with assert_raises(C8AuthenticationError):
        client.get_jwt(tenant=user["tenant"], password="121@Macrometa")

    with assert_raises(C8AuthenticationError):
        client.get_jwt(username=user["username"], password="121@Macrometa")

    with assert_raises(C8AuthenticationError):
        client.get_jwt(
            email="{}@macrometa.io".format(generate_username()),
            password=generate_string(),
        )

    client.delete_user(username=user["username"])
