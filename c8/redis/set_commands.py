from c8.redis.core import build_request, response_handler


class SetCommands:

    @staticmethod
    def sadd_command(key, member, collection):
        data = ["SADD", key, member]
        request = build_request(collection, data)

        return [request, response_handler]

    @staticmethod
    def spop_command(key, count, collection):
        data = ["SPOP", key, count]
        request = build_request(collection, data)

        return [request, response_handler]
