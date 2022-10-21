import pytest
from conftest import get_client_instance

"""
Tests need to be run in sequence since we first create collection, after that we fill 
collection with test data data, run tests and check for the results.
"""

REDIS_COLLECTION = "testRedisCollection"


@pytest.fixture
def setup_client():
    client = get_client_instance()
    return client


def test_create_redis_collection(setup_client):
    response = setup_client.create_collection(
        REDIS_COLLECTION,
        stream=True,
    )
    # Response from platform
    assert REDIS_COLLECTION == response.name


def test_redis_set(setup_client):
    response = setup_client.redis.set("test", "1", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_append(setup_client):
    response = setup_client.redis.append("test", "2", REDIS_COLLECTION)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": 2} == response


def test_redis_dec(setup_client):
    response = setup_client.redis.decr("test", REDIS_COLLECTION)
    # Response from platform -> Number of strings in values
    assert {"code": 200, "result": 11} == response


def test_redis_decby(setup_client):
    response = setup_client.redis.decrby("test", 10, REDIS_COLLECTION)
    # Response from platform -> Returned value is int
    assert {"code": 200, "result": 1} == response


def test_redis_get(setup_client):
    response = setup_client.redis.get("test", REDIS_COLLECTION)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_getdel(setup_client):
    response = setup_client.redis.getdel("test", REDIS_COLLECTION)
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "1"} == response


def test_redis_getex(setup_client):
    setup_client.redis.set("test", "EX", REDIS_COLLECTION)
    response = setup_client.redis.getex("test", REDIS_COLLECTION, "EX", "200")
    # Response from platform -> make sure that we have data on platform "result": "2"
    assert {"code": 200, "result": "EX"} == response


def test_redis_getrange(setup_client):
    response = setup_client.redis.getrange("test", 0, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "E"} == response


def test_redis_getset(setup_client):
    response = setup_client.redis.getset("test", "test_value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "EX"} == response


def test_redis_incr(setup_client):
    response = setup_client.redis.incr("test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_incrby(setup_client):
    response = setup_client.redis.incrby("test", 10, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


def test_redis_incrbyfloat(setup_client):
    response = setup_client.redis.incrbyfloat("test", 0.5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": '11.5'} == response


def test_redis_set_2(setup_client):
    response = setup_client.redis.set("test2", "22", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_mget(setup_client):
    response = setup_client.redis.mget(["test", "test2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["11.5", "22"]} == response


def test_redis_mset(setup_client):
    response = setup_client.redis.mset(
        {"test3": "value3", "test4": "value4"},
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_msetnx(setup_client):
    response = setup_client.redis.msetnx(
        {"test5": "value5", "test6": "value6"},
        REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setex(setup_client):
    response = setup_client.redis.setex("ttlKeySec", 30, "value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_psetex(setup_client):
    response = setup_client.redis.psetex("ttlKeyMs", 30000, "value", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_setbit(setup_client):
    response = setup_client.redis.setbit("bitKey", 7, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_setnx(setup_client):
    response = setup_client.redis.setnx("testSetNx", "1", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setrange(setup_client):
    response = setup_client.redis.setrange("testSetNx", 0, "2", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_setnx_2(setup_client):
    response = setup_client.redis.setnx("testString", "string", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_strlen(setup_client):
    response = setup_client.redis.strlen("testString", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_setnx_3(setup_client):
    response = setup_client.redis.setnx("myKeyString", "foobar", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_bitcount(setup_client):
    response = setup_client.redis.bitcount("myKeyString", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 26} == response


def test_redis_bitcount_2(setup_client):
    response = setup_client.redis.bitcount("myKeyString", REDIS_COLLECTION, 0, 0)
    # Response from platform
    assert {"code": 200, "result": 4} == response


# This test fails
# We support Redis commands till 6.2, bit/byte operation was added in 7.0
# def test_redis_bitcount_3():
#     setup_client = get_client_instance()
#
#     response = setup_client.redis_bitcount("myKeyString", REDIS_COLLECTION, 1, 1, "BYTE")
#     
#     # Response from platform
#     assert {"code": 200, "result": 6} == response


def test_redis_set_3(setup_client):
    response = setup_client.redis.set("key1", "foobar", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_set_4(setup_client):
    response = setup_client.redis.set("key2", "abcdef", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_bitop(setup_client):
    response = setup_client.redis.bitop("AND", "dest", ["key1", "key2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_setbit_2(setup_client):
    response = setup_client.redis.setbit("mykey", 7, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_getbit(setup_client):
    response = setup_client.redis.getbit("mykey", 7, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_set_5(setup_client):
    response = setup_client.redis.set("mykey2", '\x00\x00\x00', REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_bitpos(setup_client):
    response = setup_client.redis.bitpos("mykey2", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 0} == response


def test_redis_bitpos_2(setup_client):
    response = setup_client.redis.bitpos("mykey2", 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


def test_redis_lpush(setup_client):
    list_data = ["iron", "gold", "copper"]
    response = setup_client.redis.lpush("list", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_lindex(setup_client):
    response = setup_client.redis.lindex("list", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "copper"} == response


def test_redis_linsert(setup_client):
    response = setup_client.redis.linsert(
        "list",
        "AFTER",
        "copper",
        "silver",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 4} == response


def test_redis_llen(setup_client):
    response = setup_client.redis.llen("list", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 4} == response


def test_redis_lrange(setup_client):
    response = setup_client.redis.lrange("list", 0, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["copper", "silver"]} == response


def test_redis_lpush_1(setup_client):
    list_data = ["a", "b", "c"]
    response = setup_client.redis.lpush("testList1", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_lpush_2(setup_client):
    list_data = ["x", "y", "z"]
    response = setup_client.redis.lpush("testList2", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_lmove(setup_client):
    response = setup_client.redis.lmove(
        "testList1",
        "testList2",
        "RIGHT",
        "LEFT",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "a"} == response


def test_redis_rpush(setup_client):
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = setup_client.redis.rpush("testListPos", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


def test_redis_lpos(setup_client):
    response = setup_client.redis.lpos("testListPos", 3, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_lpos_2(setup_client):
    response = setup_client.redis.lpos("testListPos", 3, REDIS_COLLECTION, 0, 3)
    # Response from platform
    assert {"code": 200, "result": [6, 8, 9]} == response


def test_redis_lpop(setup_client):
    response = setup_client.redis.lpop("testList2", REDIS_COLLECTION, 1)
    # Response from platform
    assert {"code": 200, "result": ["a"]} == response


def test_redis_lpushx(setup_client):
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = setup_client.redis.lpushx("testListPos", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 22} == response


def test_redis_rpush_2(setup_client):
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = setup_client.redis.rpush("testListRpushx", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 11} == response


def test_redis_rpushx(setup_client):
    list_data = ["a", "b", "c", "d", 1, 2, 3, 4, 3, 3, 3]
    response = setup_client.redis.rpushx("testListRpushx", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 22} == response


def test_redis_rpush_3(setup_client):
    list_data = ["hello", "hello", "foo", "hello"]
    response = setup_client.redis.rpush("testListLrem", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 4} == response


def test_redis_lrem(setup_client):
    response = setup_client.redis.lrem("testListLrem", -2, "hello", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_lset(setup_client):
    response = setup_client.redis.lset("testListLrem", 0, "test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_trim(setup_client):
    response = setup_client.redis.ltrim("testListLrem", 0, 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_rpop(setup_client):
    response = setup_client.redis.rpop("testListLrem", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "test"} == response


def test_redis_rpush_4(setup_client):
    list_data = ["one", "two", "three"]
    response = setup_client.redis.rpush("myPushList", list_data, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_rpoplpush(setup_client):
    response = setup_client.redis.rpoplpush("myPushList", "myOtherPushList", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "three"} == response


def test_redis_hset(setup_client):
    response = setup_client.redis.hset(
        "games",
        {"action": "elden", "driving": "GT7"},
        REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_hget(setup_client):
    response = setup_client.redis.hget("games", "action", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "elden"} == response


def test_redis_hdel(setup_client):
    response = setup_client.redis.hdel("games", ["action"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hexists(setup_client):
    response = setup_client.redis.hexists("games", "driving", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hgetall(setup_client):
    response = setup_client.redis.hgetall("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["driving", "GT7"]} == response


def test_redis_hincrby(setup_client):
    response = setup_client.redis.hincrby("myhash", "field", 5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": '5'} == response


def test_redis_hincrbyfloat(setup_client):
    response = setup_client.redis.hincrbyfloat("myhashfloat", "field", 10.5, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": '10.500000'} == response


def test_redis_hkeys(setup_client):
    response = setup_client.redis.hkeys("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["driving"]} == response


def test_redis_hlen(setup_client):
    response = setup_client.redis.hlen("games", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_hset_2(setup_client):
    response = setup_client.redis.hset(
        "newgames",
        {"action": "elden", "driving": "GT7"},
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_hmget(setup_client):
    response = setup_client.redis.hmget("newgames", ["action", "driving"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["elden", "GT7"]} == response


def test_redis_hmset(setup_client):
    response = setup_client.redis.hmset(
        "world",
        {"land": "dog", "sea": "octopus"},
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_hmscan_1(setup_client):
    response = setup_client.redis.hscan("games", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ['cursor:driving', ['driving', 'GT7']]} == response


def test_redis_hmscan_2(setup_client):
    response = setup_client.redis.hscan("games", 0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert {"code": 200, "result": ["cursor:driving", ["driving", "GT7"]]} == response


def test_redis_hstrlen(setup_client):
    response = setup_client.redis.hstrlen("games", "driving", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_hmset_2(setup_client):
    response = setup_client.redis.hmset(
        "coin",
        {"heads": "obverse", "tails": "reverse", "edge": "null"},
        REDIS_COLLECTION
    )

    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_hrandfield_1(setup_client):
    response = setup_client.redis.hrandfield("coin", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_hrandfield_2(setup_client):
    response = setup_client.redis.hrandfield("coin", REDIS_COLLECTION, -5, "WITHVALUES")
    # Response from platform
    assert 200 == response.get("code")


def test_redis_hvals(setup_client):
    response = setup_client.redis.hvals("coin", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["obverse", "reverse", "null"]} == response


def test_redis_sadd(setup_client):
    response = setup_client.redis.sadd("animals", ["dog"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_scard(setup_client):
    response = setup_client.redis.scard("animals", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_sdiff(setup_client):
    # Test Setup according to Redis docs
    setup_client.redis.sadd("key1sdiff", ["a", "b", "c"], REDIS_COLLECTION)
    setup_client.redis.sadd("key2sdiff", ["c"], REDIS_COLLECTION)
    setup_client.redis.sadd("key3sdiff", ["d", "e"], REDIS_COLLECTION)
    response = setup_client.redis.sdiff(
        ["key1sdiff", "key2sdiff", "key3sdiff"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["b", "a"]} == response


def test_redis_sdiffstore(setup_client):
    response = setup_client.redis.sdiffstore(
        "destinationKeysdiffstore",
        ["key1sdiff", "key2sdiff", "key3sdiff"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_sinter(setup_client):
    # Test Setup according to Redis docs
    setup_client.redis.sadd("key11", ["a", "b", "c"], REDIS_COLLECTION)
    setup_client.redis.sadd("key22", ["c", "d", "e"], REDIS_COLLECTION)
    response = setup_client.redis.sinter(["key11", "key22"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["c"]} == response


def test_redis_sinterstore(setup_client):
    response = setup_client.redis.sinterstore(
        "destinationInter",
        ["key11", "key22"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_sismember(setup_client):
    # Test Setup according to Redis docs
    response = setup_client.redis.sismember("key11", "a", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_smembers(setup_client):
    # Test Setup according to Redis docs
    response = setup_client.redis.smembers("key11", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c"]} == response


def test_redis_smismember(setup_client):
    # Test Setup according to Redis docs
    response = setup_client.redis.smismember("key11", ["a", "b", "z"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": [1, 1, 0]} == response


def test_redis_smove(setup_client):
    # Test Setup according to Redis docs
    response = setup_client.redis.smove("key11", "key22", "b", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_spop(setup_client):
    response = setup_client.redis.spop("animals", 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ['dog']} == response


def test_redis_srandmember_1(setup_client):
    # Test Setup according to Redis docs
    response = setup_client.redis.srandmember("key22", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_srandmember_2(setup_client):
    # Test Setup according to Redis docs
    response = setup_client.redis.srandmember("key22", REDIS_COLLECTION, -5)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_srem(setup_client):
    # Test Setup according to Redis docs
    response = setup_client.redis.srem("key22", ["e", "b"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_sscan_1(setup_client):
    setup_client.redis.sadd("keyScan", ["a", "b", "c"], REDIS_COLLECTION)
    response = setup_client.redis.sscan("keyScan", 0, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["cursor:c", ["a", "b", "c"]]} == response


def test_redis_sscan_2(setup_client):
    response = setup_client.redis.sscan("keyScan", 0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert {"code": 200, "result": ["cursor:c", ["a", "b", "c"]]} == response


def test_redis_sunion(setup_client):
    # Test Setup according to Redis docs
    setup_client.redis.sadd("key111", ["a", "b", "c"], REDIS_COLLECTION)
    setup_client.redis.sadd("key222", ["c", "d", "e"], REDIS_COLLECTION)
    response = setup_client.redis.sunion(["key111", "key222"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c", "d", "e"]} == response


def test_redis_sunionstore(setup_client):
    # Test Setup according to Redis docs
    response = setup_client.redis.sunionstore(
        "destinationUnionStore",
        ["key111", "key222"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 5} == response


def test_redis_zadd(setup_client):
    response = setup_client.redis.zadd("testZadd", [1, "test"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zadd_2(setup_client):
    response = setup_client.redis.zadd(
        "testZadd2",
        [1, "test2"],
        REDIS_COLLECTION,
        options=["NX", "INCR"]
    )
    # Response from platform
    assert {"code": 200, "result": "1"} == response


def test_redis_zrange(setup_client):
    response = setup_client.redis.zrange("testZadd", 0, 1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["test"]} == response


def test_redis_zrange_2(setup_client):
    response = setup_client.redis.zrange("testZadd", 0, 1, REDIS_COLLECTION, ["WITHSCORES"])
    # Response from platform
    assert {"code": 200, "result": ["test", 1]} == response


def test_redis_zcard(setup_client):
    response = setup_client.redis.zcard("testZadd", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zcount(setup_client):
    response = setup_client.redis.zcount("testZadd", 0, 2, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zdiff(setup_client):
    setup_client.redis.zadd("testDiff1", [1, "one"], REDIS_COLLECTION)
    setup_client.redis.zadd("testDiff1", [2, "two"], REDIS_COLLECTION)
    setup_client.redis.zadd("testDiff1", [3, "three"], REDIS_COLLECTION)
    setup_client.redis.zadd("testDiff2", [1, "one"], REDIS_COLLECTION)
    setup_client.redis.zadd("testDiff2", [2, "two"], REDIS_COLLECTION)
    response = setup_client.redis.zdiff(
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three"]} == response


def test_redis_zdiff_2(setup_client):
    response = setup_client.redis.zdiff(
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION,
        with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["three", 3]} == response


def test_redis_zdiffstore(setup_client):
    response = setup_client.redis.zdiffstore(
        "destinationZdiff",
        2,
        ["testDiff1", "testDiff2"],
        REDIS_COLLECTION,
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zincrby(setup_client):
    response = setup_client.redis.zincrby("testZadd", 1.5, "test", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2.5} == response


def test_redis_zinter(setup_client):
    setup_client.redis.zadd("zset1", [1, "one", 2, "two"], REDIS_COLLECTION)
    setup_client.redis.zadd("zset2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = setup_client.redis.zinter(2, ["zset1", "zset2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "two"]} == response


def test_redis_zinter_2(setup_client):
    response = setup_client.redis.zinter(
        2,
        ["zset1", "zset2"],
        REDIS_COLLECTION,
        with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["one", 2, "two", 4]} == response


def test_redis_zinterstore(setup_client):
    response = setup_client.redis.zinterstore(
        "zinterStore",
        2,
        ["zset1", "zset2"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zlexcount(setup_client):
    setup_client.redis.zadd(
        "zlexSet1", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e"],
        REDIS_COLLECTION
    )
    setup_client.redis.zadd("zlexSet1", [0, "f", 0, "g"], REDIS_COLLECTION)
    response = setup_client.redis.zlexcount("zlexSet1", "-", "+", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 7} == response


def test_redis_zmscore(setup_client):
    response = setup_client.redis.zmscore("zlexSet1", ["a", "b"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": [0, 0]} == response


def test_redis_zpopmax(setup_client):
    setup_client.redis.zadd("zpop", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = setup_client.redis.zpopmax("zpop", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three", "3"]} == response


def test_redis_zpopmin(setup_client):
    setup_client.redis.zadd("zpop", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = setup_client.redis.zpopmin("zpop", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "1"]} == response


def test_redis_zrandmember(setup_client):
    response = setup_client.redis.zrandmember("zpop", REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_zrangebylex(setup_client):
    setup_client.redis.zadd(
        "zrangeByLexSet1", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"],
        REDIS_COLLECTION
    )
    response = setup_client.redis.zrangebylex("zrangeByLexSet1", "-", "[c", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["a", "b", "c"]} == response


def test_redis_zrangebyscore(setup_client):
    setup_client.redis.zadd(
        "zrangeByScoreSet1",
        [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = setup_client.redis.zrangebyscore(
        "zrangeByScoreSet1",
        "-inf",
        "+inf",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["one", "two", "three"]} == response


def test_redis_zrank(setup_client):
    response = setup_client.redis.zrank("zrangeByScoreSet1", "three", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zrem(setup_client):
    response = setup_client.redis.zrem(
        "zrangeByScoreSet1",
        ["two", "three"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zremrangebylex(setup_client):
    setup_client.redis.zadd(
        "zremrangebylex", [0, "aaaaa", 0, "b", 0, "c", 0, "d", 0, "e"],
        REDIS_COLLECTION
    )
    setup_client.redis.zadd(
        "zremrangebylex", [0, "foo", 0, "zap", 0, "zip", 0, "ALPHA", 0, "alpha"],
        REDIS_COLLECTION
    )
    response = setup_client.redis.zremrangebylex(
        "zremrangebylex",
        "[alpha",
        "[omega",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 6} == response


def test_redis_zremrangebyrank(setup_client):
    setup_client.redis.zadd(
        "zremrangebyrank", [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = setup_client.redis.zremrangebyrank(
        "zremrangebyrank",
        0,
        1,
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zremrangebyscore(setup_client):
    setup_client.redis.zadd(
        "zremrangebyscore2", [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = setup_client.redis.zremrangebyscore(
        "zremrangebyscore2",
        "-inf",
        "(2",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zrevrange(setup_client):
    setup_client.redis.zadd(
        "zrevrange", [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = setup_client.redis.zrevrange("zrevrange", 0, -1, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["three", "two", "one"]} == response


def test_redis_zrevrangebylex(setup_client):
    setup_client.redis.zadd(
        "zrevrangebylex", [0, "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g"],
        REDIS_COLLECTION
    )
    response = setup_client.redis.zrevrangebylex("zrevrangebylex", "[c", "-",
                                           REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["c", "b", "a"]} == response


def test_redis_zrevrangebyscore(setup_client):
    setup_client.redis.zadd(
        "zrevrangebyscore",
        [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = setup_client.redis.zrevrangebyscore(
        "zrevrangebyscore",
        "+inf",
        "-inf",
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": ["three", "two", "one"]} == response


def test_redis_zrevrank(setup_client):
    response = setup_client.redis.zrevrank("zrevrangebyscore", "one", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_zscan(setup_client):
    response = setup_client.redis.zscan("zrevrangebyscore", 0, REDIS_COLLECTION)
    # Response from platform
    assert {
               "code": 200,
               "result": [
                   "cursor:3-three",
                   [1, "one", 2, "two", 3, "three"]
               ]
           } == response


def test_redis_zscore(setup_client):
    response = setup_client.redis.zscore("zrevrangebyscore", "one", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_zunion(setup_client):
    setup_client.redis.zadd("zunionSet1", [1, "one", 2, "two"], REDIS_COLLECTION)
    setup_client.redis.zadd("zunionSet2", [1, "one", 2, "two", 3, "three"], REDIS_COLLECTION)
    response = setup_client.redis.zunion(2, ["zunionSet1", "zunionSet2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": ["one", "three", "two"]} == response


def test_redis_zunion_2(setup_client):
    response = setup_client.redis.zunion(
        2,
        ["zunionSet1", "zunionSet2"],
        REDIS_COLLECTION,
        with_scores=True
    )
    # Response from platform
    assert {"code": 200, "result": ["one", 2, "three", 3, "two", 4]} == response


def test_redis_zunionstore(setup_client):
    setup_client.redis.zadd("zunionStoreSet1", [1, "one", 2, "two"], REDIS_COLLECTION)
    setup_client.redis.zadd(
        "zunionStoreSet2",
        [1, "one", 2, "two", 3, "three"],
        REDIS_COLLECTION
    )
    response = setup_client.redis.zunionstore(
        "zunionDestination",
        2,
        ["zunionStoreSet1", "zunionStoreSet2"],
        REDIS_COLLECTION
    )
    # Response from platform
    assert {"code": 200, "result": 3} == response


def test_redis_copy(setup_client):
    setup_client.redis.set("dolly", "sheep", REDIS_COLLECTION)
    response = setup_client.redis.copy("dolly", "clone", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_exists(setup_client):
    response = setup_client.redis.exists(["dolly", "clone"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_delete(setup_client):
    response = setup_client.redis.delete(["dolly", "clone"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_expire(setup_client):
    setup_client.redis.set("expire", "test", REDIS_COLLECTION)
    response = setup_client.redis.expire("expire", 30, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_expire_2(setup_client):
    setup_client.redis.set("expire2", "test", REDIS_COLLECTION)
    response = setup_client.redis.expire("expire2", 30, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_expireat(setup_client):
    setup_client.redis.set("expireat", "test", REDIS_COLLECTION)
    response = setup_client.redis.expireat("expireat", 30, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_expireat_2(setup_client):
    setup_client.redis.set("expireat2", "test", REDIS_COLLECTION)
    response = setup_client.redis.expireat("expireat2", 30, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_persist(setup_client):
    response = setup_client.redis.persist("expireat2", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpire():
    setup_client = get_client_instance()
    setup_client.redis.set("pexpire", "test", REDIS_COLLECTION)
    response = setup_client.redis.pexpire("pexpire", 8000, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpire_2(setup_client):
    setup_client.redis.set("pexpire2", "test", REDIS_COLLECTION)
    response = setup_client.redis.pexpire("pexpire2", 8000, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpireat(setup_client):
    setup_client.redis.set("pexpireat", "test", REDIS_COLLECTION)
    response = setup_client.redis.pexpire("pexpireat", 8000, REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pexpireat_2(setup_client):
    setup_client.redis.set("pexpireat2", "test", REDIS_COLLECTION)
    response = setup_client.redis.pexpire("pexpireat2", 8000, REDIS_COLLECTION, "NX")
    # Response from platform
    assert {"code": 200, "result": 1} == response


def test_redis_pttl(setup_client):
    setup_client.redis.set("pttl", "test", REDIS_COLLECTION)
    response = setup_client.redis.pttl("pttl", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


def test_redis_randomkey(setup_client):
    response = setup_client.redis.randomkey(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_rename(setup_client):
    setup_client.redis.set("rename", "test", REDIS_COLLECTION)
    response = setup_client.redis.rename("rename", "newName", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_scan(setup_client):
    response = setup_client.redis.scan(0, REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_scan_2(setup_client):
    response = setup_client.redis.scan(0, REDIS_COLLECTION, "*", 100)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_ttl(setup_client):
    setup_client.redis.set("ttl", "test", REDIS_COLLECTION)
    response = setup_client.redis.ttl("ttl", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": -1} == response


def test_redis_type(setup_client):
    setup_client.redis.set("type", "test", REDIS_COLLECTION)
    response = setup_client.redis.type("type", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "string"} == response


def test_redis_unlink(setup_client):
    setup_client.redis.set("unlink1", "test", REDIS_COLLECTION)
    setup_client.redis.set("unlink2", "test", REDIS_COLLECTION)
    response = setup_client.redis.unlink(["unlink1", "unlink2"], REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": 2} == response


def test_redis_echo(setup_client):
    response = setup_client.redis.echo("Hello World!", REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "Hello World!"} == response


def test_redis_ping(setup_client):
    response = setup_client.redis.ping(REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "PONG"} == response


def test_redis_ping_2(setup_client):
    response = setup_client.redis.ping(REDIS_COLLECTION, "Hello World!")
    # Response from platform
    assert {"code": 200, "result": "Hello World!"} == response


def test_redis_dbsize(setup_client):
    response = setup_client.redis.dbsize(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_redis_flushdb(setup_client):
    response = setup_client.redis.flushdb(REDIS_COLLECTION)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_flushdb_2(setup_client):
    response = setup_client.redis.flushdb(REDIS_COLLECTION, async_flush=True)
    # Response from platform
    assert {"code": 200, "result": "OK"} == response


def test_redis_time(setup_client):
    response = setup_client.redis.time(REDIS_COLLECTION)
    # Response from platform
    assert 200 == response.get("code")


def test_delete_redis_collection(setup_client):
    response = setup_client.delete_collection(REDIS_COLLECTION)
    # Response from platform
    assert True == response