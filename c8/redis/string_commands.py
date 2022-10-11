from c8.redis.core import build_request, RedisServerError


class StringCommands:

    @staticmethod
    def append_command(key, value, collection):
        data = ["APPEND", key, value]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]

    @staticmethod
    def decr_command(key, collection):
        data = ["DECR", key]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]

    @staticmethod
    def decrby_command(key, decrement, collection):
        data = ["DECRBY", key, decrement]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]

    @staticmethod
    def set_command(key, value, collection):
        data = ["SET", key, value]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]

    @staticmethod
    def get_command(key, collection):
        data = ["GET", key]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]
