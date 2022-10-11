from c8.api import APIWrapper

from c8.redis.core import build_request, RedisServerError
from c8.redis.commands import Commands


class RedisInterface(APIWrapper, Commands):
    """Redis API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection, executor):
        super(RedisInterface, self).__init__(connection, executor)

    def __repr__(self):
        return '<RedisInterface in {}>'.format(self._conn.fabric_name)

    def command_parser(self, command, collection, *args):
        data = [command, *args]
        filtered_data = [i for i in data if i is not None]

        request = build_request(collection, filtered_data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return self._execute(request, response_handler)

    def append(self, key, value, collection):
        request_response_handler = self.append_command(key, value, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def decr(self, key, collection):
        request_response_handler = self.decr_command(key, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def decrby(self, key, decrement, collection):
        request_response_handler = self.decrby_command(key, decrement, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def get(self, key, collection):
        request_response_handler = self.get_command(key, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def zadd(self, key, score, member, collection):
        request_response_handler = self.zadd_command(key, score, member, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def zrange(self, key, start, stop, collection):
        request_response_handler = self.zrange_command(key, start, stop, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def lpush(self, key, elements, collection):
        request_response_handler = self.lpush_command(key, elements, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def lrange(self, key, start, stop, collection):
        request_response_handler = self.lrange_command(key, start, stop, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def hset(self, key, field, value, collection):
        request_response_handler = self.hset_command(key, field, value, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def hget(self, key, field, collection):
        request_response_handler = self.hget_command(key, field, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def sadd(self, key, member, collection):
        request_response_handler = self.sadd_command(key, member, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

    def spop(self, key, count, collection):
        request_response_handler = self.spop_command(key, count, collection)
        return self._execute(request_response_handler[0], request_response_handler[1])

