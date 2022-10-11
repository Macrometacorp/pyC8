from c8.redis.core import build_request, response_handler


class SortedSetCommands:

    @staticmethod
    def zadd_command(key, score, member, collection):
        data = ["ZADD", key, score, member]
        request = build_request(collection, data)

        return [request, response_handler]

    @staticmethod
    def zrange_command(key, start, stop, collection):
        data = ["ZRANGE", key, start, stop]
        request = build_request(collection, data)

        return [request, response_handler]
