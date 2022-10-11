from c8.redis.core import build_request, RedisServerError


class ListCommands:

    @staticmethod
    def lpush_command(key, elements, collection):
        data = ["LPUSH", key]
        data.extend(elements)
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]

    @staticmethod
    def lrange_command(key, start, stop, collection):
        data = ["LRANGE", key, start, stop]
        request = build_request(collection, data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return [request, response_handler]
