from c8.redis.core import build_request, RedisServerError


class HashCommands:

    @staticmethod
    def hset_command(key, field, value, collection):
        data = ["HSET", key, field, value]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]

    @staticmethod
    def hget_command(key, field, collection):
        data = ["H", key, field]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]
