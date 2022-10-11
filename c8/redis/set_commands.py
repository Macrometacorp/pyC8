from c8.redis.core import build_request, RedisServerError


class SetCommands:

    @staticmethod
    def sadd_command(key, member, collection):
        data = ["SADD", key, member]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]

    @staticmethod
    def spop_command(key, count, collection):
        data = ["SPOP", key, count]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]
