from c8.redis.core import build_request, RedisServerError


class SortedSetCommands:

    @staticmethod
    def zadd_command(key, score, member, collection):
        data = ["ZADD", key, score, member]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]

    @staticmethod
    def zrange_command(key, start, stop, collection):
        data = ["ZRANGE", key, start, stop]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]
