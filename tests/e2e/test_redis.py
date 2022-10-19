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
    response = client.redis.set("test", "1", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_append():
    client = get_client_instance()
    response = client.redis.append("test", "2", REDIS_COLLECTION)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": 2} == response


def test_redis_dec():
    client = get_client_instance()
    response = client.redis.decr("test", REDIS_COLLECTION)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": 11} == response


def test_redis_decby():
    client = get_client_instance()
    response = client.redis.decrby("test", 10, REDIS_COLLECTION)
    # Response from platform -> Returned value is int
    assert {"code": 200, "result": 1} == response


def test_redis_get():
    client = get_client_instance()
    response = client.redis.get("test", REDIS_COLLECTION)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_getdel():
    client = get_client_instance()
    response = client.redis.getdel("test", REDIS_COLLECTION)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_getex():
    client = get_client_instance()
    client.redis.set("test", "EX", REDIS_COLLECTION)
    response = client.redis.getex("test", REDIS_COLLECTION, "EX", "200")
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "EX"} == response


def test_redis_getrange():
    client = get_client_instance()
    response = client.redis.getrange("test", 0, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "E"} == response


def test_redis_getset():
    client = get_client_instance()
    response = client.redis.getset("test", "test_value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "EX"} == response


def test_redis_incr():
    client = get_client_instance()
    response = client.redis.incr("test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_incrby():
    client = get_client_instance()
    response = client.redis.incrby("test", 10, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


def test_redis_incrbyfloat():
    client = get_client_instance()
    response = client.redis.incrbyfloat("test", 0.5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": '11.5'} == response


def test_redis_set_2():
    client = get_client_instance()
    response = client.redis.set("test2", "22", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_mget():
    client = get_client_instance()
    response = client.redis.mget(["test", "test2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["11.5", "22"]} == response


def test_redis_mset():
    client = get_client_instance()
    response = client.redis.mset(
        {"test3": "value3", "test4": "value4"},
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_msetnx():
    client = get_client_instance()
    response = client.redis.msetnx(
        {"test5": "value5", "test6": "value6"},
        REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setex():
    client = get_client_instance()
    response = client.redis.setex("ttlKeySec", 30, "value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_psetex():
    client = get_client_instance()
    response = client.redis.psetex("ttlKeyMs", 30000, "value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_setbit():
    client = get_client_instance()
    response = client.redis.setbit("bitKey", 7, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_setnx():
    client = get_client_instance()
    response = client.redis.setnx("testSetNx", "1", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setrange():
    client = get_client_instance()
    response = client.redis.setrange("testSetNx", 0, "2", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setnx_2():
    client = get_client_instance()
    response = client.redis.setnx("testString", "string", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_strlen():
    client = get_client_instance()
    response = client.redis.strlen("testString", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_setnx_3():
    client = get_client_instance()
    response = client.redis.setnx("myKeyString", "foobar", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_bitcount():
    client = get_client_instance()
    response = client.redis.bitcount("myKeyString", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 26} == response


def test_redis_bitcount_2():
    client = get_client_instance()
    response = client.redis.bitcount("myKeyString", REDIS_COLLECTION, 0, 0)
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
    response = client.redis.set("key1", "foobar", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_set_4():
    client = get_client_instance()
    response = client.redis.set("key2", "abcdef", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_bitop():
    client = get_client_instance()
    response = client.redis.bitop("AND", "dest", ["key1", "key2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_setbit_2():
    client = get_client_instance()
    response = client.redis.setbit("mykey", 7, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_getbit():
    client = get_client_instance()
    response = client.redis.getbit("mykey", 7, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_set_5():
    client = get_client_instance()
    response = client.redis.set("mykey2", '\x00\x00\x00', REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_bitpos():
    client = get_client_instance()
    response = client.redis.bitpos("mykey2", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_bitpos_2():
    client = get_client_instance()
    response = client.redis.bitpos("mykey2", 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


def test_redis_lpush():
    client = get_client_instance()
    list_data = ["iron", "gold", "copper"]
    response = client.redis.lpush("list", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_lindex():
    client = get_client_instance()
    response = client.redis.lindex("list", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "copper"} == response


def test_redis_linsert():
    client = get_client_instance()
    response = client.redis.linsert(
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
    response = client.redis.llen("list", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 4} == response


def test_redis_lrange():
    client = get_client_instance()
    response = client.redis.lrange("list", 0, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["copper", "silver"]} == response


def test_redis_lpush_1():
    client = get_client_instance()
    list_data = ["a", "b", "c"]
    response = client.redis.lpush("testList1", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_lpush_2():
    client = get_client_instance()
    list_data = ["x", "y", "z"]
    response = client.redis.lpush("testList2", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_lmove():
    client = get_client_instance()
    response = client.redis.lmove(
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
    response = client.redis.rpush("testListPos", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


def test_redis_lpos():
    client = get_client_instance()
    response = client.redis.lpos("testListPos", 3, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_lpos_2():
    client = get_client_instance()
    response = client.redis.lpos("testListPos", 3, REDIS_COLLECTION, 0, 3)
    # Response from platform
    assert {"code": 200, "result": [6, 8, 9]} == response


def test_redis_lpop():
    client = get_client_instance()
    response = client.redis.lpop("testList2", REDIS_COLLECTION, 1)
    # Response from platform
    assert {"code": 200, "result": ["a"]} == response


def test_redis_lpushx():
    client = get_client_instance()
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = client.redis.lpushx("testListPos", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 22} == response


def test_redis_rpush_2():
    client = get_client_instance()
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = client.redis.rpush("testListRpushx", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


def test_redis_rpushx():
    client = get_client_instance()
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = client.redis.rpushx("testListRpushx", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 22} == response


def test_redis_rpush_3():
    client = get_client_instance()
    list_data = ["hello", "hello", "foo", "hello"]
    response = client.redis.rpush("testListLrem", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 4} == response


def test_redis_lrem():
    client = get_client_instance()
    response = client.redis.lrem("testListLrem", -2, "hello", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_lset():
    client = get_client_instance()
    response = client.redis.lset("testListLrem", 0, "test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_trim():
    client = get_client_instance()
    response = client.redis.ltrim("testListLrem", 0, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_rpop():
    client = get_client_instance()
    response = client.redis.rpop("testListLrem", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "test"} == response


def test_redis_rpush_4():
    client = get_client_instance()
    list_data = ["one", "two", "three"]
    response = client.redis.rpush("myPushList", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_rpoplpush():
    client = get_client_instance()
    response = client.redis.rpoplpush("myPushList", "myOtherPushList", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "three"} == response


def test_redis_hset():
    client = get_client_instance()
    response = client.redis.hset(
        "games",
        {"action": "elden", "driving": "GT7"},
        REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_hget():
    client = get_client_instance()
    response = client.redis.hget("games", "action", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "elden"} == response


def test_redis_hdel():
    client = get_client_instance()
    response = client.redis.hdel("games", ["action"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hexists():
    client = get_client_instance()
    response = client.redis.hexists("games", "driving", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hgetall():
    client = get_client_instance()
    response = client.redis.hgetall("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["driving", "GT7"]} == response


def test_redis_hincrby():
    client = get_client_instance()
    response = client.redis.hincrby("myhash", "field", 5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": '5'} == response


def test_redis_hincrbyfloat():
    client = get_client_instance()
    response = client.redis.hincrbyfloat("myhashfloat", "field", 10.5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": '10.500000'} == response


def test_redis_hkeys():
    client = get_client_instance()
    response = client.redis.hkeys("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["driving"]} == response


def test_redis_hlen():
    client = get_client_instance()
    response = client.redis.hlen("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hset_2():
    client = get_client_instance()
    response = client.redis.hset(
        "newgames",
        {"action": "elden", "driving": "GT7"},
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_hmget():
    client = get_client_instance()
    response = client.redis.hmget("newgames", ["action", "driving"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["elden", "GT7"]} == response


def test_redis_hmset():
    client = get_client_instance()
    response = client.redis.hmset(
        "world",
        {"land": "dog", "sea": "octopus"},
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_hmscan_1():
    client = get_client_instance()
    response = client.redis.hscan("games", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ['cursor:driving', ['driving', 'GT7']]} == response


def test_redis_hmscan_2():
    client = get_client_instance()
    response = client.redis.hscan("games", 0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert {"code": 200, "result": ["cursor:driving", ["driving", "GT7"]]} == response


def test_redis_hstrlen():
    client = get_client_instance()
    response = client.redis.hstrlen("games", "driving", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_hmset_2():
    client = get_client_instance()
    response = client.redis.hmset(
        "coin",
        {"heads": "obverse", "tails": "reverse", "edge": "null"},
        REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_hrandfield_1():
    client = get_client_instance()
    response = client.redis.hrandfield("coin", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_hrandfield_2():
    client = get_client_instance()
    response = client.redis.hrandfield("coin", REDIS_COLLECTION, -5, "WITHVALUES")
    # Response from platform
    assert 200 == response.get("code")


def test_redis_hvals():
    client = get_client_instance()
    response = client.redis.hvals("coin", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["obverse", "reverse", "null"]} == response


def test_redis_sadd():
    client = get_client_instance()
    response = client.redis.sadd("animals", ["dog"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_scard():
    client = get_client_instance()
    response = client.redis.scard("animals", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_sdiff():
    client = get_client_instance()
    # Test Setup according to Redis docs
    client.redis.sadd("key1sdiff", ["a", "b", "c"], REDIS_COLLECTION)
    client.redis.sadd("key2sdiff", ["c"], REDIS_COLLECTION)
    client.redis.sadd("key3sdiff", ["d", "e"], REDIS_COLLECTION)
    response = client.redis.sdiff(
        ["key1sdiff", "key2sdiff", "key3sdiff"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["b", "a"]} == response


def test_redis_sdiffstore():
    client = get_client_instance()
    response = client.redis.sdiffstore(
        "destinationKeysdiffstore",
        ["key1sdiff", "key2sdiff", "key3sdiff"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_sinter():
    client = get_client_instance()
    # Test Setup according to Redis docs
    client.redis.sadd("key11", ["a", "b", "c"], REDIS_COLLECTION)
    client.redis.sadd("key22", ["c", "d", "e"], REDIS_COLLECTION)
    response = client.redis.sinter(["key11", "key22"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["c"]} == response


def test_redis_sinterstore():
    client = get_client_instance()
    response = client.redis.sinterstore(
        "destinationInter",
        ["key11", "key22"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_sismember():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis.sismember("key11", "a", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_smembers():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis.smembers("key11", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c"]} == response


def test_redis_smismember():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis.smismember("key11", ["a", "b", "z"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": [1, 1, 0]} == response


def test_redis_smove():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis.smove("key11", "key22", "b", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_spop():
    client = get_client_instance()
    response = client.redis.spop("animals", 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ['dog']} == response


def test_redis_srandmember_1():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis.srandmember("key22", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_srandmember_2():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis.srandmember("key22", REDIS_COLLECTION, -5)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_srem():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis.srem("key22", ["e", "b"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_sscan_1():
    client = get_client_instance()
    client.redis.sadd("keyScan", ["a", "b", "c"], REDIS_COLLECTION)
    response = client.redis.sscan("keyScan", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["cursor:c", ["a", "b", "c"]]} == response


def test_redis_sscan_2():
    client = get_client_instance()
    response = client.redis.sscan("keyScan", 0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert {"code": 200, "result": ["cursor:c", ["a", "b", "c"]]} == response


def test_redis_sunion():
    client = get_client_instance()
    # Test Setup according to Redis docs
    client.redis.sadd("key111", ["a", "b", "c"], REDIS_COLLECTION)
    client.redis.sadd("key222", ["c", "d", "e"], REDIS_COLLECTION)
    response = client.redis.sunion(["key111", "key222"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c", "d", "e"]} == response


def test_redis_sunionstore():
    client = get_client_instance()
    # Test Setup according to Redis docs
    response = client.redis.sunionstore(
        "destinationUnionStore",
        ["key111", "key222"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 5} == response


def test_redis_zadd():
    client = get_client_instance()
    response = client.redis.zadd("testZadd", [1, "test"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zadd_2():
    client = get_client_instance()
    response = client.redis.zadd(
        "testZadd2",
        [1, "test2"],
        REDIS_COLLECTION,
        options=["NX", "INCR"]
    )
    # Response from platform
    assert {"code": 200, "result": "1"} == response


def test_redis_zrange():
    client = get_client_instance()
    response = client.redis.zrange("testZadd", 0, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["test"]} == response


def test_redis_zrange_2():
    client = get_client_instance()
    response = client.redis.zrange("testZadd", 0, 1, REDIS_COLLECTION, ["WITHSCORES"])
    # Response from platform
    assert {"code": 200, "result": ["test", 1]} == response


def test_redis_zcard():
    client = get_client_instance()
    response = client.redis.zcard("testZadd", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zcount():
    client = get_client_instance()
    response = client.redis.zcount("testZadd", 0, 2, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zdiff():
    client = get_client_instance()
    client.redis.zadd("testDiff1", [1, "one"], REDIS_COLLECTION)
    client.redis.zadd("testDiff1", [2, "two"], REDIS_COLLECTION)
    client.redis.zadd("testDiff1", [3, "three"], REDIS_COLLECTION)
    client.redis.zadd("testDiff2", [1, "one"], REDIS_COLLECTION)
    client.redis.zadd("testDiff2", [2, "two"], REDIS_COLLECTION)
    response = client.redis.zdiff(
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three"]} == response


def test_redis_zdiff_2():
    client = get_client_instance()
    response = client.redis.zdiff(
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION,
        with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["three", 3]} == response


def test_redis_zdiffstore():
    client = get_client_instance()
    response = client.redis.zdiffstore(
        "destinationZdiff",
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION,
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zincrby():
    client = get_client_instance()
    response = client.redis.zincrby("testZadd", 1.5, "test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2.5} == response


def test_redis_zinter():
    client = get_client_instance()
    client.redis.zadd("zset1", [1, "one", 2, "two"], REDIS_COLLECTION)
    client.redis.zadd("zset2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = client.redis.zinter(2, ["zset1", "zset2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "two"]} == response


def test_redis_zinter_2():
    client = get_client_instance()
    response = client.redis.zinter(
        2,
        ["zset1", "zset2"],
        REDIS_COLLECTION,
        with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["one", 2, "two", 4]} == response


def test_redis_zinterstore():
    client = get_client_instance()
    response = client.redis.zinterstore(
        "zinterStore",
        2,
        ["zset1", "zset2"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zlexcount():
    client = get_client_instance()
    client.redis.zadd(
        "zlexSet1", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e"],
        REDIS_COLLECTION
    )
    client.redis.zadd("zlexSet1", [0, "f", 0, "g"], REDIS_COLLECTION)
    response = client.redis.zlexcount("zlexSet1", "-", "+", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 7} == response


def test_redis_zmscore():
    client = get_client_instance()
    response = client.redis.zmscore("zlexSet1", ["a", "b"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": [0, 0]} == response


def test_redis_zpopmax():
    client = get_client_instance()
    client.redis.zadd("zpop", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = client.redis.zpopmax("zpop", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three", "3"]} == response


def test_redis_zpopmin():
    client = get_client_instance()
    client.redis.zadd("zpop", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = client.redis.zpopmin("zpop", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "1"]} == response


def test_redis_zrandmember():
    client = get_client_instance()
    response = client.redis.zrandmember("zpop", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_zrangebylex():
    client = get_client_instance()
    client.redis.zadd(
        "zrangeByLexSet1", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"],
        REDIS_COLLECTION
    )
    response = client.redis.zrangebylex("zrangeByLexSet1", "-", "[c", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c"]} == response


def test_redis_zrangebyscore():
    client = get_client_instance()
    client.redis.zadd(
        "zrangeByScoreSet1",
        [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis.zrangebyscore(
        "zrangeByScoreSet1",
        "-inf",
        "+inf",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["one", "two", "three"]} == response


def test_redis_zrank():
    client = get_client_instance()
    response = client.redis.zrank("zrangeByScoreSet1", "three", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zrem():
    client = get_client_instance()
    response = client.redis.zrem(
        "zrangeByScoreSet1",
        ["two", "three"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zremrangebylex():
    client = get_client_instance()
    client.redis.zadd(
        "zremrangebylex", [0, "aaaaa", 0, "b", 0, "c", 0, "d", 0, "e"],
        REDIS_COLLECTION
    )
    client.redis.zadd(
        "zremrangebylex", [0, "foo", 0, "zap", 0, "zip", 0, "ALPHA", 0, "alpha"],
        REDIS_COLLECTION
    )
    response = client.redis.zremrangebylex(
        "zremrangebylex",
        "[alpha",
        "[omega",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_zremrangebyrank():
    client = get_client_instance()
    client.redis.zadd(
        "zremrangebyrank", [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis.zremrangebyrank(
        "zremrangebyrank",
        0,
        1,
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zremrangebyscore():
    client = get_client_instance()
    client.redis.zadd(
        "zremrangebyscore2", [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis.zremrangebyscore(
        "zremrangebyscore2",
        "-inf",
        "(2",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zrevrange():
    client = get_client_instance()
    client.redis.zadd(
        "zrevrange", [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis.zrevrange("zrevrange", 0, -1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three", "two", "one"]} == response


def test_redis_zrevrangebylex():
    client = get_client_instance()
    client.redis.zadd(
        "zrevrangebylex", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"],
        REDIS_COLLECTION
    )
    response = client.redis.zrevrangebylex("zrevrangebylex", "[c", "-",
                                           REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["c", "b", "a"]} == response


def test_redis_zrevrangebyscore():
    client = get_client_instance()
    client.redis.zadd(
        "zrevrangebyscore",
        [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis.zrevrangebyscore(
        "zrevrangebyscore",
        "+inf",
        "-inf",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["three", "two", "one"]} == response


def test_redis_zrevrank():
    client = get_client_instance()
    response = client.redis.zrevrank("zrevrangebyscore", "one", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zscan():
    client = get_client_instance()
    response = client.redis.zscan("zrevrangebyscore", 0, REDIS_COLLECTION)
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
    response = client.redis.zscore("zrevrangebyscore", "one", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zunion():
    client = get_client_instance()
    client.redis.zadd("zunionSet1", [1, "one", 2, "two"], REDIS_COLLECTION)
    client.redis.zadd("zunionSet2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = client.redis.zunion(2, ["zunionSet1", "zunionSet2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "three", "two"]} == response


def test_redis_zunion_2():
    client = get_client_instance()
    response = client.redis.zunion(
        2,
        ["zunionSet1", "zunionSet2"],
        REDIS_COLLECTION,
        with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["one", 2, "three", 3, "two", 4]} == response


def test_redis_zunionstore():
    client = get_client_instance()
    client.redis.zadd("zunionStoreSet1", [1, "one", 2, "two"], REDIS_COLLECTION)
    client.redis.zadd(
        "zunionStoreSet2",
        [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = client.redis.zunionstore(
        "zunionDestination",
        2,
        ["zunionStoreSet1", "zunionStoreSet2"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_copy():
    client = get_client_instance()
    client.redis.set("dolly", "sheep", REDIS_COLLECTION)
    response = client.redis.copy("dolly", "clone", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_exists():
    client = get_client_instance()
    response = client.redis.exists(["dolly", "clone"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_delete():
    client = get_client_instance()
    response = client.redis.delete(["dolly", "clone"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_expire():
    client = get_client_instance()
    client.redis.set("expire", "test", REDIS_COLLECTION)
    response = client.redis.expire("expire", 30, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_expire_2():
    client = get_client_instance()
    client.redis.set("expire2", "test", REDIS_COLLECTION)
    response = client.redis.expire("expire2", 30, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_expireat():
    client = get_client_instance()
    client.redis.set("expireat", "test", REDIS_COLLECTION)
    response = client.redis.expireat("expireat", 30, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_expireat_2():
    client = get_client_instance()
    client.redis.set("expireat2", "test", REDIS_COLLECTION)
    response = client.redis.expireat("expireat2", 30, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_persist():
    client = get_client_instance()
    response = client.redis.persist("expireat2", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpire():
    client = get_client_instance()
    client.redis.set("pexpire", "test", REDIS_COLLECTION)
    response = client.redis.pexpire("pexpire", 8000, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpire_2():
    client = get_client_instance()
    client.redis.set("pexpire2", "test", REDIS_COLLECTION)
    response = client.redis.pexpire("pexpire2", 8000, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpireat():
    client = get_client_instance()
    client.redis.set("pexpireat", "test", REDIS_COLLECTION)
    response = client.redis.pexpire("pexpireat", 8000, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpireat_2():
    client = get_client_instance()
    client.redis.set("pexpireat2", "test", REDIS_COLLECTION)
    response = client.redis.pexpire("pexpireat2", 8000, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pttl():
    client = get_client_instance()
    client.redis.set("pttl", "test", REDIS_COLLECTION)
    response = client.redis.pttl("pttl", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


def test_redis_randomkey():
    client = get_client_instance()
    response = client.redis.randomkey(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_rename():
    client = get_client_instance()
    client.redis.set("rename", "test", REDIS_COLLECTION)
    response = client.redis.rename("rename", "newName", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_scan():
    client = get_client_instance()
    response = client.redis.scan(0, REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_scan_2():
    client = get_client_instance()
    response = client.redis.scan(0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_ttl():
    client = get_client_instance()
    client.redis.set("ttl", "test", REDIS_COLLECTION)
    response = client.redis.ttl("ttl", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


def test_redis_type():
    client = get_client_instance()
    client.redis.set("type", "test", REDIS_COLLECTION)
    response = client.redis.type("type", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "string"} == response


def test_redis_unlink():
    client = get_client_instance()
    client.redis.set("unlink1", "test", REDIS_COLLECTION)
    client.redis.set("unlink2", "test", REDIS_COLLECTION)
    response = client.redis.unlink(["unlink1", "unlink2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_echo():
    client = get_client_instance()
    response = client.redis.echo("Hello World!", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "Hello World!"} == response


def test_redis_ping():
    client = get_client_instance()
    response = client.redis.ping(REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "PONG"} == response


def test_redis_ping_2():
    client = get_client_instance()
    response = client.redis.ping(REDIS_COLLECTION, "Hello World!")
    # Response from platform
    assert {"code": 200, "result": "Hello World!"} == response


def test_redis_dbsize():
    client = get_client_instance()
    response = client.redis.dbsize(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_flushdb():
    client = get_client_instance()
    response = client.redis.flushdb(REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_flushdb_2():
    client = get_client_instance()
    response = client.redis.flushdb(REDIS_COLLECTION, async_flush=True)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_time():
    client = get_client_instance()
    response = client.redis.time(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_delete_redis_collection():
    client = get_client_instance()
    response = client.delete_collection(REDIS_COLLECTION)
    # Response from platform
    assert True == response