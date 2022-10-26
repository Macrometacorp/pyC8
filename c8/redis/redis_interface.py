from c8.api import APIWrapper
from c8.redis.core import RedisServerError, build_request


class RedisInterface(APIWrapper):

    """Redis API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection, executor):
        super(RedisInterface, self).__init__(connection, executor)

    def __repr__(self):
        return "<RedisInterface in {}>".format(self._conn.fabric_name)

    def command_parser(self, command, collection, *args):
        data = [command, *args]
        filtered_data = [i for i in data if i is not None]

        request = build_request(collection, filtered_data)

        def response_handler(response):
            if not response.is_success and request is not None:
                raise RedisServerError(response, request)
            return response.body

        return self._execute(request, response_handler)
