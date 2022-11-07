from c8 import C8Client

# Creating client instance with API KEY
client = C8Client(
    protocol="https", host="gdn.paas.macrometa.io", port=443, apikey="<your API key>"
)

collection_name = "test_redis_collection_4"
client.create_collection(collection_name, stream=True)

# redis string type example
client.redis.set("test_key", "test_value", collection_name)
get_string = client.redis.get("test_key", collection_name)

# redis sorted set type example
client.redis.zadd("test_zadd", [1, "test"], collection_name)
get_sorted_set = client.redis.zrange("test_zadd", 0, 1, collection_name)

# redis list type example
list_data = ["iron", "gold", "copper"]
client.redis.lpush("list", list_data, collection_name)
get_list = client.redis.lrange("list", 0, 1, collection_name)

# redis hash type example
client.redis.hset("games", {"action": "elden", "driving": "GT7"}, collection_name)
get_hash = client.redis.hget("games", "action", collection_name)

# redis sets type example
client.redis.sadd("animals", ["dog"], collection_name)
get_set = client.redis.spop("animals", 1, collection_name)
