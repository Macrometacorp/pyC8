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

REDIS_COLLECTION = "dinoRedisTest"


def test_redis_set():
    client = get_client_instance()

    response = client.redis_set("test", "1", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_append():
    client = get_client_instance()

    response = client.redis_append("test", "2", REDIS_COLLECTION)
    print(response)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": "2"} == response


def test_redis_dec():
    client = get_client_instance()

    response = client.redis_decr("test", REDIS_COLLECTION)
    print(response)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": "11"} == response


def test_redis_decby():
    client = get_client_instance()

    response = client.redis_decrby("test", 10, REDIS_COLLECTION)
    print(response)
    # Response from platform -> Returned value is int
    assert {"code": 200, "result": 1} == response


def test_redis_get():
    client = get_client_instance()

    response = client.redis_get("test", REDIS_COLLECTION)
    print(response)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_getdel():
    client = get_client_instance()

    response = client.redis_getdel("test", REDIS_COLLECTION)
    print(response)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_getex():
    client = get_client_instance()

    response = client.redis_getex("test", REDIS_COLLECTION, "EX", "200")
    print(response)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_getrange():
    client = get_client_instance()

    response = client.redis_getrange("test", 0, 0, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "1"} == response


def test_redis_getset():
    client = get_client_instance()

    response = client.redis_getset("test", "test_value", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "1"} == response


def test_redis_incr():
    client = get_client_instance()

    response = client.redis_incr("test", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "2"} == response


def test_redis_incrby():
    client = get_client_instance()

    response = client.redis_incrby("test", 10, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "12"} == response


def test_redis_incrbyfloat():
    client = get_client_instance()

    response = client.redis_incrbyfloat("test", 0.5, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "12.5"} == response


def test_redis_set_2():
    client = get_client_instance()

    response = client.redis_set("test2", "22", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_mget():
    client = get_client_instance()

    response = client.redis_mget(["test", "test2"], REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": ["12.5", "22"]} == response


def test_redis_mset():
    client = get_client_instance()

    response = client.redis_mset(
        {"test3": "value3", "test4": "value4"},
        REDIS_COLLECTION
    )
    print(response)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_msetnx():
    client = get_client_instance()

    response = client.redis_msetnx(
        {"test5": "value5", "test6": "value6"},
        REDIS_COLLECTION
    )
    print(response)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setex():
    client = get_client_instance()

    response = client.redis_setex("ttlKeySec", 30, "value", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_psetex():
    client = get_client_instance()

    response = client.redis_psetex("ttlKeyMs", 30000, "value", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_setbit():
    client = get_client_instance()

    response = client.redis_setbit("bitKey", 7, 0, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_setnx():
    client = get_client_instance()

    response = client.redis_setnx("testSetNx", "1", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setrange():
    client = get_client_instance()

    response = client.redis_setrange("testSetNx", 0, "2", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setnx_2():
    client = get_client_instance()

    response = client.redis_setnx("testString", "string", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_strlen():
    client = get_client_instance()

    response = client.redis_strlen("testString", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_setnx_3():
    client = get_client_instance()

    response = client.redis_setnx("myKeyString", "foobar", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_bitcount():
    client = get_client_instance()

    response = client.redis_bitcount("myKeyString", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 26} == response


def test_redis_bitcount_2():
    client = get_client_instance()

    response = client.redis_bitcount("myKeyString", REDIS_COLLECTION, 0, 0)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 4} == response

# This test fails
# We support Redis commands till 6.2, bit/byte operation was added in 7.0
# def test_redis_bitcount_3():
#     client = get_client_instance()
#
#     response = client.redis_bitcount("myKeyString", REDIS_COLLECTION, 1, 1, "BYTE")
#     print(response)
#     # Response from platform
#     assert {"code": 200, "result": 6} == response


def test_redis_set_3():
    client = get_client_instance()

    response = client.redis_set("key1", "foobar", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_set_4():
    client = get_client_instance()

    response = client.redis_set("key2", "abcdef", REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_bitop():
    client = get_client_instance()

    response = client.redis_bitop("AND", "dest", ["key1", "key2"], REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_setbit_2():
    client = get_client_instance()

    response = client.redis_setbit("mykey", 7, 1, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_getbit():
    client = get_client_instance()

    response = client.redis_getbit("mykey", 7, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_set_5():
    client = get_client_instance()

    response = client.redis_set("mykey2", '\x00\x00\x00', REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_bitpos():
    client = get_client_instance()

    response = client.redis_bitpos("mykey2", 0, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_bitpos_2():
    client = get_client_instance()

    response = client.redis_bitpos("mykey2", 1, REDIS_COLLECTION)
    print(response)
    # Response from platform
    assert {"code": 200, "result": -1} == response


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
    assert {"code": 200, "result": 3} == response


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

