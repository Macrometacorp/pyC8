import pytest

"""
Tests need to be run in sequence since we first create collection, after that we fill
collection with test data data, run tests and check for the results.
"""

REDIS_COLLECTION = "testRedisCollection"


@pytest.mark.vcr
def test_create_redis_collection(get_client_instance):
    response = get_client_instance.create_collection(
        REDIS_COLLECTION,
        stream=True,
    )
    # Response from platform
    assert REDIS_COLLECTION == response.name


@pytest.mark.vcr
def test_redis_set(get_client_instance):
    response = get_client_instance.redis.set("test", "1", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_append(get_client_instance):
    response = get_client_instance.redis.append("test", "2", REDIS_COLLECTION)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_dec(get_client_instance):
    response = get_client_instance.redis.decr("test", REDIS_COLLECTION)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": 11} == response


@pytest.mark.vcr
def test_redis_decby(get_client_instance):
    response = get_client_instance.redis.decrby("test", 10, REDIS_COLLECTION)
    # Response from platform -> Returned value is int
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_get(get_client_instance):
    response = get_client_instance.redis.get("test", REDIS_COLLECTION)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


@pytest.mark.vcr
def test_redis_getdel(get_client_instance):
    response = get_client_instance.redis.getdel("test", REDIS_COLLECTION)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


@pytest.mark.vcr
def test_redis_getex(get_client_instance):
    get_client_instance.redis.set("test", "EX", REDIS_COLLECTION)
    response = get_client_instance.redis.getex("test", REDIS_COLLECTION, "EX", "200")
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "EX"} == response


@pytest.mark.vcr
def test_redis_getrange(get_client_instance):
    response = get_client_instance.redis.getrange("test", 0, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "E"} == response


@pytest.mark.vcr
def test_redis_getset(get_client_instance):
    response = get_client_instance.redis.getset("test", "test_value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "EX"} == response


@pytest.mark.vcr
def test_redis_incr(get_client_instance):
    response = get_client_instance.redis.incr("test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_incrby(get_client_instance):
    response = get_client_instance.redis.incrby("test", 10, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


@pytest.mark.vcr
def test_redis_incrbyfloat(get_client_instance):
    response = get_client_instance.redis.incrbyfloat("test", 0.5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "11.5"} == response


@pytest.mark.vcr
def test_redis_set_2(get_client_instance):
    response = get_client_instance.redis.set("test2", "22", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_mget(get_client_instance):
    response = get_client_instance.redis.mget(["test", "test2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["11.5", "22"]} == response


@pytest.mark.vcr
def test_redis_mset(get_client_instance):
    response = get_client_instance.redis.mset(
        {"test3": "value3", "test4": "value4"}, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_msetnx(get_client_instance):
    response = get_client_instance.redis.msetnx(
        {"test5": "value5", "test6": "value6"}, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_setex(get_client_instance):
    response = get_client_instance.redis.setex(
        "ttlKeySec", 30, "value", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_psetex(get_client_instance):
    response = get_client_instance.redis.psetex(
        "ttlKeyMs", 30000, "value", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_setbit(get_client_instance):
    response = get_client_instance.redis.setbit("bitKey", 7, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


@pytest.mark.vcr
def test_redis_setnx(get_client_instance):
    response = get_client_instance.redis.setnx("testSetNx", "1", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_setrange(get_client_instance):
    response = get_client_instance.redis.setrange("testSetNx", 0, "2", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_setnx_2(get_client_instance):
    response = get_client_instance.redis.setnx("testString", "string", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_strlen(get_client_instance):
    response = get_client_instance.redis.strlen("testString", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


@pytest.mark.vcr
def test_redis_setnx_3(get_client_instance):
    response = get_client_instance.redis.setnx(
        "myKeyString", "foobar", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_bitcount(get_client_instance):
    response = get_client_instance.redis.bitcount("myKeyString", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 26} == response


@pytest.mark.vcr
def test_redis_bitcount_2(get_client_instance):
    response = get_client_instance.redis.bitcount("myKeyString", REDIS_COLLECTION, 0, 0)
    # Response from platform
    assert {"code": 200, "result": 4} == response


# This test fails
# We support Redis commands till 6.2, bit/byte operation was added in 7.0
# def test_redis_bitcount_3(get_client_instance):
#
#     response = get_client_instance.redis_bitcount("myKeyString", REDIS_COLLECTION, 1, 1, "BYTE")
#
#     # Response from platform
#     assert {"code": 200, "result": 6} == response


@pytest.mark.vcr
def test_redis_set_3(get_client_instance):
    response = get_client_instance.redis.set("key1", "foobar", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_set_4(get_client_instance):
    response = get_client_instance.redis.set("key2", "abcdef", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_bitop(get_client_instance):
    response = get_client_instance.redis.bitop(
        "AND", "dest", ["key1", "key2"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 6} == response


@pytest.mark.vcr
def test_redis_setbit_2(get_client_instance):
    response = get_client_instance.redis.setbit("mykey", 7, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


@pytest.mark.vcr
def test_redis_getbit(get_client_instance):
    response = get_client_instance.redis.getbit("mykey", 7, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_set_5(get_client_instance):
    response = get_client_instance.redis.set("mykey2", "\x00\x00\x00", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_bitpos(get_client_instance):
    response = get_client_instance.redis.bitpos("mykey2", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


@pytest.mark.vcr
def test_redis_bitpos_2(get_client_instance):
    response = get_client_instance.redis.bitpos("mykey2", 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


@pytest.mark.vcr
def test_redis_lpush(get_client_instance):
    list_data = ["iron", "gold", "copper"]
    response = get_client_instance.redis.lpush("list", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


@pytest.mark.vcr
def test_redis_lindex(get_client_instance):
    response = get_client_instance.redis.lindex("list", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "copper"} == response


@pytest.mark.vcr
def test_redis_linsert(get_client_instance):
    response = get_client_instance.redis.linsert(
        "list", "AFTER", "copper", "silver", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 4} == response


@pytest.mark.vcr
def test_redis_llen(get_client_instance):
    response = get_client_instance.redis.llen("list", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 4} == response


@pytest.mark.vcr
def test_redis_lrange(get_client_instance):
    response = get_client_instance.redis.lrange("list", 0, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["copper", "silver"]} == response


@pytest.mark.vcr
def test_redis_lpush_1(get_client_instance):
    list_data = ["a", "b", "c"]
    response = get_client_instance.redis.lpush("testList1", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


@pytest.mark.vcr
def test_redis_lpush_2(get_client_instance):
    list_data = ["x", "y", "z"]
    response = get_client_instance.redis.lpush("testList2", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


@pytest.mark.vcr
def test_redis_lmove(get_client_instance):
    response = get_client_instance.redis.lmove(
        "testList1", "testList2", "RIGHT", "LEFT", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "a"} == response


@pytest.mark.vcr
def test_redis_rpush(get_client_instance):
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = get_client_instance.redis.rpush(
        "testListPos", list_data, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 11} == response


@pytest.mark.vcr
def test_redis_lpos(get_client_instance):
    response = get_client_instance.redis.lpos("testListPos", 3, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


@pytest.mark.vcr
def test_redis_lpos_2(get_client_instance):
    response = get_client_instance.redis.lpos("testListPos", 3, REDIS_COLLECTION, 0, 3)
    # Response from platform
    assert {"code": 200, "result": [6, 8, 9]} == response


@pytest.mark.vcr
def test_redis_lpop(get_client_instance):
    response = get_client_instance.redis.lpop("testList2", REDIS_COLLECTION, 1)
    # Response from platform
    assert {"code": 200, "result": ["a"]} == response


@pytest.mark.vcr
def test_redis_lpushx(get_client_instance):
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = get_client_instance.redis.lpushx(
        "testListPos", list_data, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 22} == response


@pytest.mark.vcr
def test_redis_rpush_2(get_client_instance):
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = get_client_instance.redis.rpush(
        "testListRpushx", list_data, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 11} == response


@pytest.mark.vcr
def test_redis_rpushx(get_client_instance):
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = get_client_instance.redis.rpushx(
        "testListRpushx", list_data, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 22} == response


@pytest.mark.vcr
def test_redis_rpush_3(get_client_instance):
    list_data = ["hello", "hello", "foo", "hello"]
    response = get_client_instance.redis.rpush(
        "testListLrem", list_data, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 4} == response


@pytest.mark.vcr
def test_redis_lrem(get_client_instance):
    response = get_client_instance.redis.lrem(
        "testListLrem", -2, "hello", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_lset(get_client_instance):
    response = get_client_instance.redis.lset(
        "testListLrem", 0, "test", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_trim(get_client_instance):
    response = get_client_instance.redis.ltrim("testListLrem", 0, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_rpop(get_client_instance):
    response = get_client_instance.redis.rpop("testListLrem", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "test"} == response


@pytest.mark.vcr
def test_redis_rpush_4(get_client_instance):
    list_data = ["one", "two", "three"]
    response = get_client_instance.redis.rpush(
        "myPushList", list_data, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 3} == response


@pytest.mark.vcr
def test_redis_rpoplpush(get_client_instance):
    response = get_client_instance.redis.rpoplpush(
        "myPushList", "myOtherPushList", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "three"} == response


@pytest.mark.vcr
def test_redis_hset(get_client_instance):
    response = get_client_instance.redis.hset(
        "games", {"action": "elden", "driving": "GT7"}, REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_hget(get_client_instance):
    response = get_client_instance.redis.hget("games", "action", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "elden"} == response


@pytest.mark.vcr
def test_redis_hdel(get_client_instance):
    response = get_client_instance.redis.hdel("games", ["action"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_hexists(get_client_instance):
    response = get_client_instance.redis.hexists("games", "driving", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_hgetall(get_client_instance):
    response = get_client_instance.redis.hgetall("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["driving", "GT7"]} == response


@pytest.mark.vcr
def test_redis_hincrby(get_client_instance):
    response = get_client_instance.redis.hincrby("myhash", "field", 5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "5"} == response


@pytest.mark.vcr
def test_redis_hincrbyfloat(get_client_instance):
    response = get_client_instance.redis.hincrbyfloat(
        "myhashfloat", "field", 10.5, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "10.500000"} == response


@pytest.mark.vcr
def test_redis_hkeys(get_client_instance):
    response = get_client_instance.redis.hkeys("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["driving"]} == response


@pytest.mark.vcr
def test_redis_hlen(get_client_instance):
    response = get_client_instance.redis.hlen("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_hset_2(get_client_instance):
    response = get_client_instance.redis.hset(
        "newgames", {"action": "elden", "driving": "GT7"}, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_hmget(get_client_instance):
    response = get_client_instance.redis.hmget(
        "newgames", ["action", "driving"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["elden", "GT7"]} == response


@pytest.mark.vcr
def test_redis_hmset(get_client_instance):
    response = get_client_instance.redis.hmset(
        "world", {"land": "dog", "sea": "octopus"}, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_hmscan_1(get_client_instance):
    response = get_client_instance.redis.hscan("games", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["cursor:driving", ["driving", "GT7"]]} == response


@pytest.mark.vcr
def test_redis_hmscan_2(get_client_instance):
    response = get_client_instance.redis.hscan("games", 0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert {"code": 200, "result": ["cursor:driving", ["driving", "GT7"]]} == response


@pytest.mark.vcr
def test_redis_hstrlen(get_client_instance):
    response = get_client_instance.redis.hstrlen("games", "driving", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


@pytest.mark.vcr
def test_redis_hmset_2(get_client_instance):
    response = get_client_instance.redis.hmset(
        "coin",
        {"heads": "obverse", "tails": "reverse", "edge": "null"},
        REDIS_COLLECTION,
    )

    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_hrandfield_1(get_client_instance):
    response = get_client_instance.redis.hrandfield("coin", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_redis_hrandfield_2(get_client_instance):
    response = get_client_instance.redis.hrandfield(
        "coin", REDIS_COLLECTION, -5, "WITHVALUES"
    )
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_redis_hvals(get_client_instance):
    response = get_client_instance.redis.hvals("coin", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["obverse", "reverse", "null"]} == response


@pytest.mark.vcr
def test_redis_sadd(get_client_instance):
    response = get_client_instance.redis.sadd("animals", ["dog"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_scard(get_client_instance):
    response = get_client_instance.redis.scard("animals", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_sdiff(get_client_instance):
    # Test Setup according to Redis docs
    get_client_instance.redis.sadd("key1sdiff", ["a", "b", "c"], REDIS_COLLECTION)
    get_client_instance.redis.sadd("key2sdiff", ["c"], REDIS_COLLECTION)
    get_client_instance.redis.sadd("key3sdiff", ["d", "e"], REDIS_COLLECTION)
    response = get_client_instance.redis.sdiff(
        ["key1sdiff", "key2sdiff", "key3sdiff"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["b", "a"]} == response


@pytest.mark.vcr
def test_redis_sdiffstore(get_client_instance):
    response = get_client_instance.redis.sdiffstore(
        "destinationKeysdiffstore",
        ["key1sdiff", "key2sdiff", "key3sdiff"],
        REDIS_COLLECTION,
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_sinter(get_client_instance):
    # Test Setup according to Redis docs
    get_client_instance.redis.sadd("key11", ["a", "b", "c"], REDIS_COLLECTION)
    get_client_instance.redis.sadd("key22", ["c", "d", "e"], REDIS_COLLECTION)
    response = get_client_instance.redis.sinter(["key11", "key22"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["c"]} == response


@pytest.mark.vcr
def test_redis_sinterstore(get_client_instance):
    response = get_client_instance.redis.sinterstore(
        "destinationInter", ["key11", "key22"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_sismember(get_client_instance):
    # Test Setup according to Redis docs
    response = get_client_instance.redis.sismember("key11", "a", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_smembers(get_client_instance):
    # Test Setup according to Redis docs
    response = get_client_instance.redis.smembers("key11", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c"]} == response


@pytest.mark.vcr
def test_redis_smismember(get_client_instance):
    # Test Setup according to Redis docs
    response = get_client_instance.redis.smismember(
        "key11", ["a", "b", "z"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": [1, 1, 0]} == response


@pytest.mark.vcr
def test_redis_smove(get_client_instance):
    # Test Setup according to Redis docs
    response = get_client_instance.redis.smove("key11", "key22", "b", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_spop(get_client_instance):
    response = get_client_instance.redis.spop("animals", 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["dog"]} == response


@pytest.mark.vcr
def test_redis_srandmember_1(get_client_instance):
    # Test Setup according to Redis docs
    response = get_client_instance.redis.srandmember("key22", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_redis_srandmember_2(get_client_instance):
    # Test Setup according to Redis docs
    response = get_client_instance.redis.srandmember("key22", REDIS_COLLECTION, -5)
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_redis_srem(get_client_instance):
    # Test Setup according to Redis docs
    response = get_client_instance.redis.srem("key22", ["e", "b"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_sscan_1(get_client_instance):
    get_client_instance.redis.sadd("keyScan", ["a", "b", "c"], REDIS_COLLECTION)
    response = get_client_instance.redis.sscan("keyScan", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["cursor:c", ["a", "b", "c"]]} == response


@pytest.mark.vcr
def test_redis_sscan_2(get_client_instance):
    response = get_client_instance.redis.sscan("keyScan", 0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert {"code": 200, "result": ["cursor:c", ["a", "b", "c"]]} == response


@pytest.mark.vcr
def test_redis_sunion(get_client_instance):
    # Test Setup according to Redis docs
    get_client_instance.redis.sadd("key111", ["a", "b", "c"], REDIS_COLLECTION)
    get_client_instance.redis.sadd("key222", ["c", "d", "e"], REDIS_COLLECTION)
    response = get_client_instance.redis.sunion(["key111", "key222"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c", "d", "e"]} == response


@pytest.mark.vcr
def test_redis_sunionstore(get_client_instance):
    # Test Setup according to Redis docs
    response = get_client_instance.redis.sunionstore(
        "destinationUnionStore", ["key111", "key222"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 5} == response


@pytest.mark.vcr
def test_redis_zadd(get_client_instance):
    response = get_client_instance.redis.zadd("testZadd", [1, "test"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_zadd_2(get_client_instance):
    response = get_client_instance.redis.zadd(
        "testZadd2", [1, "test2"], REDIS_COLLECTION, options=["NX", "INCR"]
    )
    # Response from platform
    assert {"code": 200, "result": "1"} == response


@pytest.mark.vcr
def test_redis_zrange(get_client_instance):
    response = get_client_instance.redis.zrange("testZadd", 0, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["test"]} == response


@pytest.mark.vcr
def test_redis_zrange_2(get_client_instance):
    response = get_client_instance.redis.zrange(
        "testZadd", 0, 1, REDIS_COLLECTION, ["WITHSCORES"]
    )
    # Response from platform
    assert {"code": 200, "result": ["test", 1]} == response


@pytest.mark.vcr
def test_redis_zcard(get_client_instance):
    response = get_client_instance.redis.zcard("testZadd", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_zcount(get_client_instance):
    response = get_client_instance.redis.zcount("testZadd", 0, 2, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_zdiff(get_client_instance):
    get_client_instance.redis.zadd("testDiff1", [1, "one"], REDIS_COLLECTION)
    get_client_instance.redis.zadd("testDiff1", [2, "two"], REDIS_COLLECTION)
    get_client_instance.redis.zadd("testDiff1", [3, "three"], REDIS_COLLECTION)
    get_client_instance.redis.zadd("testDiff2", [1, "one"], REDIS_COLLECTION)
    get_client_instance.redis.zadd("testDiff2", [2, "two"], REDIS_COLLECTION)
    response = get_client_instance.redis.zdiff(
        2, ["testDiff1", "testDiff2"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["three"]} == response


@pytest.mark.vcr
def test_redis_zdiff_2(get_client_instance):
    response = get_client_instance.redis.zdiff(
        2, ["testDiff1", "testDiff2"], REDIS_COLLECTION, with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["three", 3]} == response


@pytest.mark.vcr
def test_redis_zdiffstore(get_client_instance):
    response = get_client_instance.redis.zdiffstore(
        "destinationZdiff",
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION,
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_zincrby(get_client_instance):
    response = get_client_instance.redis.zincrby(
        "testZadd", 1.5, "test", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2.5} == response


@pytest.mark.vcr
def test_redis_zinter(get_client_instance):
    get_client_instance.redis.zadd("zset1", [1, "one", 2, "two"], REDIS_COLLECTION)
    get_client_instance.redis.zadd(
        "zset2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zinter(2, ["zset1", "zset2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "two"]} == response


@pytest.mark.vcr
def test_redis_zinter_2(get_client_instance):
    response = get_client_instance.redis.zinter(
        2, ["zset1", "zset2"], REDIS_COLLECTION, with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["one", 2, "two", 4]} == response


@pytest.mark.vcr
def test_redis_zinterstore(get_client_instance):
    response = get_client_instance.redis.zinterstore(
        "zinterStore", 2, ["zset1", "zset2"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_zlexcount(get_client_instance):
    get_client_instance.redis.zadd(
        "zlexSet1", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e"], REDIS_COLLECTION
    )
    get_client_instance.redis.zadd("zlexSet1", [0, "f", 0, "g"], REDIS_COLLECTION)
    response = get_client_instance.redis.zlexcount(
        "zlexSet1", "-", "+", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 7} == response


@pytest.mark.vcr
def test_redis_zmscore(get_client_instance):
    response = get_client_instance.redis.zmscore(
        "zlexSet1", ["a", "b"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": [0, 0]} == response


@pytest.mark.vcr
def test_redis_zpopmax(get_client_instance):
    get_client_instance.redis.zadd(
        "zpop", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zpopmax("zpop", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three", "3"]} == response


@pytest.mark.vcr
def test_redis_zpopmin(get_client_instance):
    get_client_instance.redis.zadd(
        "zpop", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zpopmin("zpop", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "1"]} == response


@pytest.mark.vcr
def test_redis_zrandmember(get_client_instance):
    response = get_client_instance.redis.zrandmember("zpop", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_redis_zrangebylex(get_client_instance):
    get_client_instance.redis.zadd(
        "zrangeByLexSet1",
        [0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"],
        REDIS_COLLECTION,
    )
    response = get_client_instance.redis.zrangebylex(
        "zrangeByLexSet1", "-", "[c", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c"]} == response


@pytest.mark.vcr
def test_redis_zrangebyscore(get_client_instance):
    get_client_instance.redis.zadd(
        "zrangeByScoreSet1", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zrangebyscore(
        "zrangeByScoreSet1", "-inf", "+inf", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["one", "two", "three"]} == response


@pytest.mark.vcr
def test_redis_zrank(get_client_instance):
    response = get_client_instance.redis.zrank(
        "zrangeByScoreSet1", "three", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_zrem(get_client_instance):
    response = get_client_instance.redis.zrem(
        "zrangeByScoreSet1", ["two", "three"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_zremrangebylex(get_client_instance):
    get_client_instance.redis.zadd(
        "zremrangebylex", [0, "aaaaa", 0, "b", 0, "c", 0, "d", 0, "e"], REDIS_COLLECTION
    )
    get_client_instance.redis.zadd(
        "zremrangebylex",
        [0, "foo", 0, "zap", 0, "zip", 0, "ALPHA", 0, "alpha"],
        REDIS_COLLECTION,
    )
    response = get_client_instance.redis.zremrangebylex(
        "zremrangebylex", "[alpha", "[omega", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 6} == response


@pytest.mark.vcr
def test_redis_zremrangebyrank(get_client_instance):
    get_client_instance.redis.zadd(
        "zremrangebyrank", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zremrangebyrank(
        "zremrangebyrank", 0, 1, REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_zremrangebyscore(get_client_instance):
    get_client_instance.redis.zadd(
        "zremrangebyscore2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zremrangebyscore(
        "zremrangebyscore2", "-inf", "(2", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_zrevrange(get_client_instance):
    get_client_instance.redis.zadd(
        "zrevrange", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zrevrange("zrevrange", 0, -1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three", "two", "one"]} == response


@pytest.mark.vcr
def test_redis_zrevrangebylex(get_client_instance):
    get_client_instance.redis.zadd(
        "zrevrangebylex",
        [0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"],
        REDIS_COLLECTION,
    )
    response = get_client_instance.redis.zrevrangebylex(
        "zrevrangebylex", "[c", "-", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["c", "b", "a"]} == response


@pytest.mark.vcr
def test_redis_zrevrangebyscore(get_client_instance):
    get_client_instance.redis.zadd(
        "zrevrangebyscore", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zrevrangebyscore(
        "zrevrangebyscore", "+inf", "-inf", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["three", "two", "one"]} == response


@pytest.mark.vcr
def test_redis_zrevrank(get_client_instance):
    response = get_client_instance.redis.zrevrank(
        "zrevrangebyscore", "one", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_zscan(get_client_instance):
    response = get_client_instance.redis.zscan("zrevrangebyscore", 0, REDIS_COLLECTION)
    # Response from platform
    assert {
        "code": 200,
        "result": ["cursor:3-three", [1, "one", 2, "two", 3, "three"]],
    } == response


@pytest.mark.vcr
def test_redis_zscore(get_client_instance):
    response = get_client_instance.redis.zscore(
        "zrevrangebyscore", "one", REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_zunion(get_client_instance):
    get_client_instance.redis.zadd("zunionSet1", [1, "one", 2, "two"], REDIS_COLLECTION)
    get_client_instance.redis.zadd(
        "zunionSet2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zunion(
        2, ["zunionSet1", "zunionSet2"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["one", "three", "two"]} == response


@pytest.mark.vcr
def test_redis_zunion_2(get_client_instance):
    response = get_client_instance.redis.zunion(
        2, ["zunionSet1", "zunionSet2"], REDIS_COLLECTION, with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["one", 2, "three", 3, "two", 4]} == response


@pytest.mark.vcr
def test_redis_zunionstore(get_client_instance):
    get_client_instance.redis.zadd(
        "zunionStoreSet1", [1, "one", 2, "two"], REDIS_COLLECTION
    )
    get_client_instance.redis.zadd(
        "zunionStoreSet2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION
    )
    response = get_client_instance.redis.zunionstore(
        "zunionDestination", 2, ["zunionStoreSet1", "zunionStoreSet2"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 3} == response


@pytest.mark.vcr
def test_redis_copy(get_client_instance):
    get_client_instance.redis.set("dolly", "sheep", REDIS_COLLECTION)
    response = get_client_instance.redis.copy("dolly", "clone", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_exists(get_client_instance):
    response = get_client_instance.redis.exists(["dolly", "clone"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_delete(get_client_instance):
    response = get_client_instance.redis.delete(["dolly", "clone"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_expire(get_client_instance):
    get_client_instance.redis.set("expire", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.expire("expire", 30, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_expire_2(get_client_instance):
    get_client_instance.redis.set("expire2", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.expire("expire2", 30, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_expireat(get_client_instance):
    get_client_instance.redis.set("expireat", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.expireat("expireat", 30, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_expireat_2(get_client_instance):
    get_client_instance.redis.set("expireat2", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.expireat(
        "expireat2", 30, REDIS_COLLECTION, "NX"
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_persist(get_client_instance):
    response = get_client_instance.redis.persist("expireat2", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_pexpire(get_client_instance):
    get_client_instance.redis.set("pexpire", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.pexpire("pexpire", 8000, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_pexpire_2(get_client_instance):
    get_client_instance.redis.set("pexpire2", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.pexpire(
        "pexpire2", 8000, REDIS_COLLECTION, "NX"
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_pexpireat(get_client_instance):
    get_client_instance.redis.set("pexpireat", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.pexpire("pexpireat", 8000, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_pexpireat_2(get_client_instance):
    get_client_instance.redis.set("pexpireat2", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.pexpire(
        "pexpireat2", 8000, REDIS_COLLECTION, "NX"
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


@pytest.mark.vcr
def test_redis_pttl(get_client_instance):
    get_client_instance.redis.set("pttl", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.pttl("pttl", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


@pytest.mark.vcr
def test_redis_randomkey(get_client_instance):
    response = get_client_instance.redis.randomkey(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_redis_rename(get_client_instance):
    get_client_instance.redis.set("rename", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.rename("rename", "newName", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_scan(get_client_instance):
    response = get_client_instance.redis.scan(0, REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_redis_scan_2(get_client_instance):
    response = get_client_instance.redis.scan(0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_redis_ttl(get_client_instance):
    get_client_instance.redis.set("ttl", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.ttl("ttl", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


@pytest.mark.vcr
def test_redis_type(get_client_instance):
    get_client_instance.redis.set("type", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.type("type", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "string"} == response


@pytest.mark.vcr
def test_redis_unlink(get_client_instance):
    get_client_instance.redis.set("unlink1", "test", REDIS_COLLECTION)
    get_client_instance.redis.set("unlink2", "test", REDIS_COLLECTION)
    response = get_client_instance.redis.unlink(
        ["unlink1", "unlink2"], REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


@pytest.mark.vcr
def test_redis_echo(get_client_instance):
    response = get_client_instance.redis.echo("Hello World!", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "Hello World!"} == response


@pytest.mark.vcr
def test_redis_ping(get_client_instance):
    response = get_client_instance.redis.ping(REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "PONG"} == response


@pytest.mark.vcr
def test_redis_ping_2(get_client_instance):
    response = get_client_instance.redis.ping(REDIS_COLLECTION, "Hello World!")
    # Response from platform
    assert {"code": 200, "result": "Hello World!"} == response


@pytest.mark.vcr
def test_redis_dbsize(get_client_instance):
    response = get_client_instance.redis.dbsize(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_redis_flushdb(get_client_instance):
    response = get_client_instance.redis.flushdb(REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_flushdb_2(get_client_instance):
    response = get_client_instance.redis.flushdb(REDIS_COLLECTION, async_flush=True)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


@pytest.mark.vcr
def test_redis_time(get_client_instance):
    response = get_client_instance.redis.time(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


@pytest.mark.vcr
def test_delete_redis_collection(get_client_instance):
    response = get_client_instance.delete_collection(REDIS_COLLECTION)
    # Response from platform
    assert response is True
