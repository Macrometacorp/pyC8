from c8.redis.core import build_request, response_handler


class StringCommands:

    @staticmethod
    def set_command(key, value, collection):
        data = ["SET", key, value]
        request = build_request(collection, data)

        return [request, response_handler]

    @staticmethod
    def get_command(key, collection):
        data = ["GET", key]
        request = build_request(collection, data)

        return [request, response_handler]
