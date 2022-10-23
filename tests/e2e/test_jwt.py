import pytest
from c8.JWT.core import JWTServerError
from tests.e2e.conftest import get_client_instance
from tests.helpers import assert_raises
from dotenv import load_dotenv
import os


@pytest.fixture()
def jwt_client_setup():
    client = get_client_instance()
    return client


def test_obtain_jwt(jwt_client_setup):
    load_dotenv()
    resp = jwt_client_setup.jwt.get_jwt_for_user(email=os.environ.get('TENANT_EMAIL'),
                                                 password=os.environ.get('TENANT_PASSWORD'))
    assert 'jwt' in resp

    resp = jwt_client_setup.jwt.get_jwt_for_user(tenant=os.environ.get('TENANT_EMAIL').replace('@', '_'),
                                                 password=os.environ.get('TENANT_PASSWORD'),
                                                 username=os.environ.get('TENANT_USERNAME'))
    assert 'jwt' in resp

    resp = jwt_client_setup.jwt.get_jwt_for_user(email=os.environ.get('TENANT_EMAIL'),
                                                 tenant=os.environ.get('TENANT_EMAIL').replace('@', '_'),
                                                 password=os.environ.get('TENANT_PASSWORD'),
                                                 username=os.environ.get('TENANT_USERNAME'))
    assert 'jwt' in resp

    with assert_raises(JWTServerError) as err:
        jwt_client_setup.jwt.get_jwt_for_user(tenant=os.environ.get('TENANT_EMAIL').replace('@', '_'),
                                              password=os.environ.get('TENANT_PASSWORD'))
    assert err.value.error_code == 400
