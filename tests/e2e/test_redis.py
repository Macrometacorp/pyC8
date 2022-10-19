from conftest import get_client_instance

"""
Tests need to be run in sequence since we first create collection, after that we fill 
collection with test data data, run tests and check for the results.
"""

REDIS_COLLECTION = "testRedisCollection"


def test_create_redis_collection():
    client = get_client_instance()
    response = client.create_collection(
        REDIS_COLLECTION,
        stream=True,
    )
    # Response from platform
    assert REDIS_COLLECTION == response.name


def test_redis_set():
    client = get_client_instance()
    response = client.redis_set("test", "1", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_append():
    client = get_client_instance()

    response = client.redis_append("test", "2", REDIS_COLLECTION)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": 2} == response


def test_redis_dec():
    client = get_client_instance()
    response = client.redis_decr("test", REDIS_COLLECTION)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": 11} == response


def test_redis_decby():
    client = get_client_instance()
    response = client.redis_decrby("test", 10, REDIS_COLLECTION)
    # Response from platform -> Returned value is int
    assert {"code": 200, "result": 1} == response


def test_redis_get():
    client = get_client_instance()
    response = client.redis_get("test", REDIS_COLLECTION)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_getdel():
    client = get_client_instance()
    response = client.redis_getdel("test", REDIS_COLLECTION)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_getex():
    client = get_client_instance()
    client.redis_set("test", "EX", REDIS_COLLECTION)
    response = client.redis_getex("test", REDIS_COLLECTION, "EX", "200")
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "EX"} == response


def test_redis_getrange():
    client = get_client_instance()
    response = client.redis_getrange("test", 0, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "E"} == response


def test_redis_getset():
    client = get_client_instance()
    response = client.redis_getset("test", "test_value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "EX"} == response


def test_redis_incr():
    client = get_client_instance()
    response = client.redis_incr("test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_incrby():
    client = get_client_instance()
    response = client.redis_incrby("test", 10, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


def test_redis_incrbyfloat():
    client = get_client_instance()
    response = client.redis_incrbyfloat("test", 0.5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": '11.5'} == response


def test_redis_set_2():
    client = get_client_instance()
    response = client.redis_set("test2", "22", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_mget():
    client = get_client_instance()
    response = client.redis_mget(["test", "test2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["11.5", "22"]} == response


def test_redis_mset():
    client = get_client_instance()
    response = client.redis_mset(
        {"test3": "value3", "test4": "value4"},
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_msetnx():
    client = get_client_instance()
    response = client.redis_msetnx(
        {"test5": "value5", "test6": "value6"},
        REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setex():
    client = get_client_instance()
    response = client.redis_setex("ttlKeySec", 30, "value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_psetex():
    client = get_client_instance()
    response = client.redis_psetex("ttlKeyMs", 30000, "value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_setbit():
    client = get_client_instance()
    response = client.redis_setbit("bitKey", 7, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_setnx():
    client = get_client_instance()
    response = client.redis_setnx("testSetNx", "1", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setrange():
    client = get_client_instance()
    response = client.redis_setrange("testSetNx", 0, "2", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setnx_2():
    client = get_client_instance()
    response = client.redis_setnx("testString", "string", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_strlen():
    client = get_client_instance()
    response = client.redis_strlen("testString", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_setnx_3():
    client = get_client_instance()
    response = client.redis_setnx("myKeyString", "foobar", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_bitcount():
    client = get_client_instance()
    response = client.redis_bitcount("myKeyString", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 26} == response


def test_redis_bitcount_2():
    client = get_client_instance()
    response = client.redis_bitcount("myKeyString", REDIS_COLLECTION, 0, 0)
    # Response from platform
    assert {"code": 200, "result": 4} == response


# This test fails
# We support Redis commands till 6.2, bit/byte operation was added in 7.0
# def test_redis_bitcount_3():
#     client = get_client_instance()
#
#     response = client.redis_bitcount("myKeyString", REDIS_COLLECTION, 1, 1, "BYTE")
#     
#     # Response from platform
#     assert {"code": 200, "result": 6} == response


def test_redis_set_3():
    client = get_client_instance()
    response = client.redis_set("key1", "foobar", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_set_4():
    client = get_client_instance()
    response = client.redis_set("key2", "abcdef", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_bitop():
    client = get_client_instance()
    response = client.redis_bitop("AND", "dest", ["key1", "key2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_setbit_2():
    client = get_client_instance()
    response = client.redis_setbit("mykey", 7, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_getbit():
    client = get_client_instance()
    response = client.redis_getbit("mykey", 7, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_set_5():
    client = get_client_instance()
    response = client.redis_set("mykey2", '\x00\x00\x00', REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_bitpos():
    client = get_client_instance()
    response = client.redis_bitpos("mykey2", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_bitpos_2():
    client = get_client_instance()
    response = client.redis_bitpos("mykey2", 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


def test_redis_lpush():
    client = get_client_instance()
    list_data = ["iron", "gold", "copper"]
    response = client.redis_lpush("list", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_lindex():
    client = get_client_instance()
    response = client.redis_lindex("list", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "copper"} == response


def test_redis_linsert():
    client = get_client_instance()
    response = client.redis_linsert(
        "list",
        "AFTER",
        "copper",
        "silver",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 4} == response


def test_redis_llen():
    client = get_client_instance()
    response = client.redis_llen("list", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 4} == response


def test_redis_lrange():
    client = get_client_instance()
    response = client.redis_lrange("list", 0, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["copper", "silver"]} == response


def test_redis_lpush_1():
    client = get_client_instance()
    list_data = ["a", "b", "c"]
    response = client.redis_lpush("testList1", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_lpush_2():
    client = get_client_instance()
    list_data = ["x", "y", "z"]
    response = client.redis_lpush("testList2", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_lmove():
    client = get_client_instance()
    response = client.redis_lmove(
        "testList1",
        "testList2",
        "RIGHT",
        "LEFT",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "a"} == response


def test_redis_rpush():
    client = get_client_instance()
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = client.redis_rpush("testListPos", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


def test_redis_lpos():
    client = get_client_instance()
    response = client.redis_lpos("testListPos", 3, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_lpos_2():
    client = get_client_instance()
    response = client.redis_lpos("testListPos", 3, REDIS_COLLECTION, 0, 3)
    # Response from platform
    assert {"code": 200, "result": [6, 8, 9]} == response


def test_redis_lpop():
    client = get_client_instance()
    response = client.redis_lpop("testList2", REDIS_COLLECTION, 1)
    # Response from platform
    assert {"code": 200, "result": ["a"]} == response


def test_redis_lpushx():
    client = get_client_instance()
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = client.redis_lpushx("testListPos", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 22} == response


def test_redis_rpush_2():
    client = get_client_instance()
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = client.redis_rpush("testListRpushx", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


def test_redis_rpushx():
    client = get_client_instance()
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = client.redis_rpushx("testListRpushx", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 22} == response


def test_redis_rpush_3():
    client = get_client_instance()
    list_data = ["hello", "hello", "foo", "hello"]
    response = client.redis_rpush("testListLrem", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 4} == response


def test_redis_lrem():
    client = get_client_instance()
    response = client.redis_lrem("testListLrem", -2, "hello", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_lset():
    client = get_client_instance()
    response = client.redis_lset("testListLrem", 0, "test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_trim():
    client = get_client_instance()
    response = client.redis_ltrim("testListLrem", 0, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_rpop():
    client = get_client_instance()
    response = client.redis_rpop("testListLrem", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "test"} == response


def test_redis_rpush_4():
    client = get_client_instance()
    list_data = ["one", "two", "three"]
    response = client.redis_rpush("myPushList", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_rpoplpush():
    client = get_client_instance()
    response = client.redis_rpoplpush("myPushList", "myOtherPushList", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "three"} == response


def test_redis_hset():
    client = get_client_instance()
    response = client.redis_hset(
        "games",
        {"action": "elden", "driving": "GT7"},
        REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_hget():
    client = get_client_instance()
    response = client.redis_hget("games", "action", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "elden"} == response


def test_redis_hdel():
    client = get_client_instance()
    response = client.redis_hdel("games", ["action"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hexists():
    client = get_client_instance()
    response = client.redis_hexists("games", "driving", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hgetall():
    client = get_client_instance()
    response = client.redis_hgetall("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["driving", "GT7"]} == response


def test_redis_hincrby():
    client = get_client_instance()
    response = client.redis_hincrby("myhash", "field", 5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": '5'} == response


def test_redis_hincrbyfloat():
    client = get_client_instance()
    response = client.redis_hincrbyfloat("myhashfloat", "field", 10.5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": '10.500000'} == response


def test_redis_hkeys():
    client = get_client_instance()
    response = client.redis_hkeys("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["driving"]} == response


def test_redis_hlen():
    client = get_client_instance()
    response = client.redis_hlen("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hset_2():
    client = get_client_instance()
    response = client.redis_hset(
        "newgames",
        {"action": "elden", "driving": "GT7"},
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_hmget():
    client = get_client_instance()
    response = client.redis_hmget("newgames", ["action", "driving"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["elden", "GT7"]} == response


def test_redis_hmset():
    client = get_client_instance()
    response = client.redis_hmset(
        "world",
        {"land": "dog", "sea": "octopus"},
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_hmscan_1():
    client = get_client_instance()
    response = client.redis_hscan("games", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ['cursor:driving', ['driving', 'GT7']]} == response


def test_redis_hmscan_2():
    client = get_client_instance()
    response = client.redis_hscan("games", 0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert {"code": 200, "result": ["cursor:driving", ["driving", "GT7"]]} == response


def test_redis_hstrlen():
    client = get_client_instance()
    response = client.redis_hstrlen("games", "driving", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_hmset_2():
    client = get_client_instance()
    response = client.redis_hmset(
        "coin",
        {"heads": "obverse", "tails": "reverse", "edge": "null"},
        REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_hrandfield_1():
    client = get_client_instance()
    response = client.redis_hrandfield("coin", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_hrandfield_2():
    client = get_client_instance()
    response = client.redis_hrandfield("coin", REDIS_COLLECTION, -5, "WITHVALUES")
    # Response from platform
    assert 200 == response.get("code")


def test_redis_hvals():
    client = get_client_instance()
    response = client.redis_hvals("coin", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["obverse", "reverse", "null"]} == response


def test_redis_sadd():
    client = get_client_instance()
    response = client.redis_sadd("animals", ["dog"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_scard():
    client = get_client_instance()
    response = client.redis_scard("animals", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_sdiff():
    client = get_client_instance()
    # Test Setup according to Redis docs
    client.redis_sadd("key1sdiff", ["a", "b", "c"], REDIS_COLLECTION)
    client.redis_sadd("key2sdiff", ["c"], REDIS_COLLECTION)
    client.redis_sadd("key3sdiff", ["d", "e"], REDIS_COLLECTION)
    response = client.redis_sdiff(
        ["key1sdiff", "key2sdiff", "key3sdiff"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["b", "a"]} == response


def test_redis_sdiffstore():
    client = get_client_instance()
    response = client.redis_sdiffstore(
        "destinationKeysdiffstore",
        ["key1sdiff", "key2sdiff", "key3sdiff"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_sinter():
    client = get_client_instance()
    # Test Setup according to Redis docs
    client.redis_sadd("key11", ["a", "b", "c"], REDIS_COLLECTION)
    client.redis_sadd("key22", ["c", "d", "e"], REDIS_COLLECTION)
    response = client.redis_sinter(["key11", "key22"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["c"]} == response


def test_redis_sinterstore():
    client = get_client_instance()
    response = client.redis_sinterstore(
        "destinationInter",
        ["key11", "key22"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_sismember():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis_sismember("key11", "a", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_smembers():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis_smembers("key11", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c"]} == response


def test_redis_smismember():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis_smismember("key11", ["a", "b", "z"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": [1, 1, 0]} == response


def test_redis_smove():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis_smove("key11", "key22", "b", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_spop():
    client = get_client_instance()
    response = client.redis_spop("animals", 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ['dog']} == response


def test_redis_srandmember_1():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis_srandmember("key22", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_srandmember_2():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis_srandmember("key22", REDIS_COLLECTION, -5)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_srem():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis_srem("key22", ["e", "b"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_sscan_1():
    client = get_client_instance()
    client.redis_sadd("keyScan", ["a", "b", "c"], REDIS_COLLECTION)
    response = client.redis_sscan("keyScan", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["cursor:c", ["a", "b", "c"]]} == response


def test_redis_sscan_2():
    client = get_client_instance()
    response = client.redis_sscan("keyScan", 0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert {"code": 200, "result": ["cursor:c", ["a", "b", "c"]]} == response


def test_redis_sunion():
    client = get_client_instance()
    # Test Setup according to Redis docs
    client.redis_sadd("key111", ["a", "b", "c"], REDIS_COLLECTION)
    client.redis_sadd("key222", ["c", "d", "e"], REDIS_COLLECTION)
    response = client.redis_sunion(["key111", "key222"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c", "d", "e"]} == response


def test_redis_sunionstore():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis_sunionstore(
        "destinationUnionStore",
        ["key111", "key222"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 5} == response


def test_redis_zadd():
    client = get_client_instance()
    response = client.redis_zadd("testZadd", [1, "test"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zadd_2():
    client = get_client_instance()
    response = client.redis_zadd(
        "testZadd2",
        [1, "test2"],
        REDIS_COLLECTION,
        options=["NX", "INCR"]
    )
    # Response from platform
    assert {"code": 200, "result": "1"} == response


def test_redis_zrange():
    client = get_client_instance()
    response = client.redis_zrange("testZadd", 0, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["test"]} == response


def test_redis_zrange_2():
    client = get_client_instance()
    response = client.redis_zrange("testZadd", 0, 1, REDIS_COLLECTION, ["WITHSCORES"])
    # Response from platform
    assert {"code": 200, "result": ["test", 1]} == response


def test_redis_zcard():
    client = get_client_instance()
    response = client.redis_zcard("testZadd", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zcount():
    client = get_client_instance()
    response = client.redis_zcount("testZadd", 0, 2, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zdiff():
    client = get_client_instance()
    client.redis_zadd("testDiff1", [1, "one"], REDIS_COLLECTION)
    client.redis_zadd("testDiff1", [2, "two"], REDIS_COLLECTION)
    client.redis_zadd("testDiff1", [3, "three"], REDIS_COLLECTION)
    client.redis_zadd("testDiff2", [1, "one"], REDIS_COLLECTION)
    client.redis_zadd("testDiff2", [2, "two"], REDIS_COLLECTION)
    response = client.redis_zdiff(
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three"]} == response


def test_redis_zdiff_2():
    client = get_client_instance()
    response = client.redis_zdiff(
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION,
        with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["three", 3]} == response


def test_redis_zdiffstore():
    client = get_client_instance()
    response = client.redis_zdiffstore(
        "destinationZdiff",
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION,
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zincrby():
    client = get_client_instance()
    response = client.redis_zincrby("testZadd", 1.5, "test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2.5} == response


def test_redis_zinter():
    client = get_client_instance()
    client.redis_zadd("zset1", [1, "one", 2, "two"], REDIS_COLLECTION)
    client.redis_zadd("zset2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = client.redis_zinter(2, ["zset1", "zset2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "two"]} == response


def test_redis_zinter_2():
    client = get_client_instance()
    response = client.redis_zinter(
        2,
        ["zset1", "zset2"],
        REDIS_COLLECTION,
        with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["one", 2, "two", 4]} == response


def test_redis_zinterstore():
    client = get_client_instance()
    response = client.redis_zinterstore(
        "zinterStore",
        2,
        ["zset1", "zset2"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zlexcount():
    client = get_client_instance()
    client.redis_zadd(
        "zlexSet1", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e"],
        REDIS_COLLECTION
    )
    client.redis_zadd("zlexSet1", [0, "f", 0, "g"], REDIS_COLLECTION)
    response = client.redis_zlexcount("zlexSet1", "-", "+", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 7} == response


def test_redis_zmscore():
    client = get_client_instance()
    response = client.redis_zmscore("zlexSet1", ["a", "b"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": [0, 0]} == response


def test_redis_zpopmax():
    client = get_client_instance()
    client.redis_zadd("zpop", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = client.redis_zpopmax("zpop", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three", "3"]} == response


def test_redis_zpopmin():
    client = get_client_instance()
    client.redis_zadd("zpop", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = client.redis_zpopmin("zpop", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "1"]} == response


def test_redis_zrandmember():
    client = get_client_instance()
    response = client.redis_zrandmember("zpop", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_zrangebylex():
    client = get_client_instance()
    client.redis_zadd(
        "zrangeByLexSet1", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"],
        REDIS_COLLECTION
    )
    response = client.redis_zrangebylex("zrangeByLexSet1", "-", "[c", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c"]} == response


def test_redis_zrangebyscore():
    client = get_client_instance()
    client.redis_zadd(
        "zrangeByScoreSet1",
        [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis_zrangebyscore(
        "zrangeByScoreSet1",
        "-inf",
        "+inf",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["one", "two", "three"]} == response


def test_redis_zrank():
    client = get_client_instance()
    response = client.redis_zrank("zrangeByScoreSet1", "three", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zrem():
    client = get_client_instance()
    response = client.redis_zrem(
        "zrangeByScoreSet1",
        ["two", "three"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zremrangebylex():
    client = get_client_instance()
    client.redis_zadd(
        "zremrangebylex", [0, "aaaaa", 0, "b", 0, "c", 0, "d", 0, "e"],
        REDIS_COLLECTION
    )
    client.redis_zadd(
        "zremrangebylex", [0, "foo", 0, "zap", 0, "zip", 0, "ALPHA", 0, "alpha"],
        REDIS_COLLECTION
    )
    response = client.redis_zremrangebylex(
        "zremrangebylex",
        "[alpha",
        "[omega",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_zremrangebyrank():
    client = get_client_instance()
    client.redis_zadd(
        "zremrangebyrank", [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis_zremrangebyrank(
        "zremrangebyrank",
        0,
        1,
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zremrangebyscore():
    client = get_client_instance()
    client.redis_zadd(
        "zremrangebyscore2", [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis_zremrangebyscore(
        "zremrangebyscore2",
        "-inf",
        "(2",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zrevrange():
    client = get_client_instance()
    client.redis_zadd(
        "zrevrange", [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis_zrevrange("zrevrange", 0, -1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three", "two", "one"]} == response


def test_redis_zrevrangebylex():
    client = get_client_instance()
    client.redis_zadd(
        "zrevrangebylex", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"],
        REDIS_COLLECTION
    )
    response = client.redis_zrevrangebylex("zrevrangebylex", "[c", "-",
                                           REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["c", "b", "a"]} == response


def test_redis_zrevrangebyscore():
    client = get_client_instance()
    client.redis_zadd(
        "zrevrangebyscore",
        [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis_zrevrangebyscore(
        "zrevrangebyscore",
        "+inf",
        "-inf",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["three", "two", "one"]} == response


def test_redis_zrevrank():
    client = get_client_instance()
    response = client.redis_zrevrank("zrevrangebyscore", "one", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zscan():
    client = get_client_instance()
    response = client.redis_zscan("zrevrangebyscore", 0, REDIS_COLLECTION)
    # Response from platform
    assert {
               "code": 200,
               "result": [
                   "cursor:3-three",
                   [1, "one", 2, "two", 3, "three"]
               ]
           } == response


def test_redis_zscore():
    client = get_client_instance()
    response = client.redis_zscore("zrevrangebyscore", "one", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zunion():
    client = get_client_instance()
    client.redis_zadd("zunionSet1", [1, "one", 2, "two"], REDIS_COLLECTION)
    client.redis_zadd("zunionSet2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = client.redis_zunion(2, ["zunionSet1", "zunionSet2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "three", "two"]} == response


def test_redis_zunion_2():
    client = get_client_instance()
    response = client.redis_zunion(
        2,
        ["zunionSet1", "zunionSet2"],
        REDIS_COLLECTION,
        with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["one", 2, "three", 3, "two", 4]} == response


def test_redis_zunionstore():
    client = get_client_instance()
    client.redis_zadd("zunionStoreSet1", [1, "one", 2, "two"], REDIS_COLLECTION)
    client.redis_zadd(
        "zunionStoreSet2",
        [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis_zunionstore(
        "zunionDestination",
        2,
        ["zunionStoreSet1", "zunionStoreSet2"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_copy():
    client = get_client_instance()
    client.redis_set("dolly", "sheep", REDIS_COLLECTION)
    response = client.redis_copy("dolly", "clone", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_exists():
    client = get_client_instance()
    response = client.redis_exists(["dolly", "clone"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_del():
    client = get_client_instance()
    response = client.redis_del(["dolly", "clone"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_expire():
    client = get_client_instance()
    client.redis_set("expire", "test", REDIS_COLLECTION)
    response = client.redis_expire("expire", 30, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_expire_2():
    client = get_client_instance()
    client.redis_set("expire2", "test", REDIS_COLLECTION)
    response = client.redis_expire("expire2", 30, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_expireat():
    client = get_client_instance()
    client.redis_set("expireat", "test", REDIS_COLLECTION)
    response = client.redis_expireat("expireat", 30, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_expireat_2():
    client = get_client_instance()
    client.redis_set("expireat2", "test", REDIS_COLLECTION)
    response = client.redis_expireat("expireat2", 30, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_persist():
    client = get_client_instance()
    response = client.redis_persist("expireat2", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpire():
    client = get_client_instance()
    client.redis_set("pexpire", "test", REDIS_COLLECTION)
    response = client.redis_pexpire("pexpire", 8000, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpire_2():
    client = get_client_instance()
    client.redis_set("pexpire2", "test", REDIS_COLLECTION)
    response = client.redis_pexpire("pexpire2", 8000, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpireat():
    client = get_client_instance()
    client.redis_set("pexpireat", "test", REDIS_COLLECTION)
    response = client.redis_pexpire("pexpireat", 8000, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpireat_2():
    client = get_client_instance()
    client.redis_set("pexpireat2", "test", REDIS_COLLECTION)
    response = client.redis_pexpire("pexpireat2", 8000, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pttl():
    client = get_client_instance()
    client.redis_set("pttl", "test", REDIS_COLLECTION)
    response = client.redis_pttl("pttl", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


def test_redis_randomkey():
    client = get_client_instance()
    response = client.redis_randomkey(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_rename():
    client = get_client_instance()
    client.redis_set("rename", "test", REDIS_COLLECTION)
    response = client.redis_rename("rename", "newName", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_scan():
    client = get_client_instance()
    response = client.redis_scan(0, REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_scan_2():
    client = get_client_instance()
    response = client.redis_scan(0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_ttl():
    client = get_client_instance()
    client.redis_set("ttl", "test", REDIS_COLLECTION)
    response = client.redis_ttl("ttl", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


def test_redis_type():
    client = get_client_instance()
    client.redis_set("type", "test", REDIS_COLLECTION)
    response = client.redis_type("type", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "string"} == response


def test_redis_unlink():
    client = get_client_instance()
    client.redis_set("unlink1", "test", REDIS_COLLECTION)
    client.redis_set("unlink2", "test", REDIS_COLLECTION)
    response = client.redis_unlink(["unlink1", "unlink2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_echo():
    client = get_client_instance()
    response = client.redis_echo("Hello World!", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "Hello World!"} == response


def test_redis_ping():
    client = get_client_instance()
    response = client.redis_ping(REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "PONG"} == response


def test_redis_ping_2():
    client = get_client_instance()
    response = client.redis_ping(REDIS_COLLECTION, "Hello World!")
    # Response from platform
    assert {"code": 200, "result": "Hello World!"} == response


def test_redis_dbsize():
    client = get_client_instance()
    response = client.redis_dbsize(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_flushdb():
    client = get_client_instance()
    response = client.redis_flushdb(REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_flushdb_2():
    client = get_client_instance()
    response = client.redis_flushdb(REDIS_COLLECTION, async_flush=True)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_time():
    client = get_client_instance()
    response = client.redis_time(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_delete_redis_collection():
    client = get_client_instance()
    response = client.delete_collection(REDIS_COLLECTION)
    # Response from platform
    assert True == response