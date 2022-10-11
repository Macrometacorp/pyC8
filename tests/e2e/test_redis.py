from conftest import get_client_instance

"""
To run end to end test .env file in /e2e is needed.
File needs to contain:
FEDERATION_URL
TENANT_EMAIL
API_KEY
FABRIC

Make sure that redis collection exists on fabric with REDIS_COLLECTION string variable.

Tests need to be run in sequence since we first create data than query for that same
data.
"""

REDIS_COLLECTION = "testRedis"


def test_redis_set():

    client = get_client_instance()

    response = client.redis_set("test", "1", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_get():
    client = get_client_instance()

    response = client.redis_get("test", REDIS_COLLECTION)
    print(response)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_zadd():
    client = get_client_instance()

    response = client.redis_zadd("testZadd", 2, "test", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "1"} == response


def test_redis_zrange():
    client = get_client_instance()

    response = client.redis_zrange("testZadd", 0, 1, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": ["test"]} == response


def test_redis_lpush():
    client = get_client_instance()

    list_data = ["iron", "gold", "copper"]
    response = client.redis_lpush("list", list_data, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": ["3"]} == response


def test_redis_lrange():
    client = get_client_instance()

    response = client.redis_lrange("list", 0, 1, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": ["copper", "gold"]} == response


def test_redis_hset():
    client = get_client_instance()

    response = client.redis_hset("games", "action", "elden", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hget():
    client = get_client_instance()

    response = client.redis_hget("games", "action", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "elden"} == response


def test_redis_sadd():
    client = get_client_instance()

    response = client.redis_sadd("animals", "dog", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_spop():
    client = get_client_instance()

    response = client.redis_spop("animals", 1, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": ['dog']} == response

