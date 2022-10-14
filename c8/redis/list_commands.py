from c8.redis.core import build_request, response_handler


class ListCommands:

    @staticmethod
    def lpush_command(key, elements, collection):
        data = ["LPUSH", key]
        data.extend(elements)
        request = build_request(collection, data)

        return [request, response_handler]

    @staticmethod
    def lrange_command(key, start, stop, collection):
        data = ["LRANGE", key, start, stop]
        request = build_request(collection, data)

        return [request, response_handler]
