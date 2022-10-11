from c8.redis.core import build_request, response_handler


class HashCommands:

    @staticmethod
    def hset_command(key, field, value, collection):
        data = ["HSET", key, field, value]
        request = build_request(collection, data)

        return [request, response_handler]

    @staticmethod
    def hget_command(key, field, collection):
        data = ["H", key, field]
        request = build_request(collection, data)

        return [request, response_handler]
