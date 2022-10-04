from json import dumps

from c8.api import APIWrapper
from c8.request import Request


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
        return '<RedisInterface in {}>'.format(self._conn.fabric_name)

    # String operation
    def set(self, key, value, collection):
        """

        """
        data = {"SET", key, value}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)

    def get(self, key, collection):
        """

        """
        data = {"GET", key}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)

    def zadd(self, key, score, member, collection):
        """

        """
        data = {"ZADD", key, score, member}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)

    def zrange(self, key, start, stop, collection):
        """

        """
        data = {"ZRANGE", key, start, stop}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)

    def lpush(self, key, elements, collection):
        """

        """
        data = {"LPUSH", key, elements}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)

    def lrange(self, key, start, stop, collection):
        """

        """
        data = {"LRANGE", key, start, stop}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)

    def hset(self, key, field, value, collection):
        """

        """
        data = {"HSET", key, field, value}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)

    def hget(self, key, field, collection):
        """

        """
        data = {"HGET", key, field}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)

    def sadd(self, key, members, collection):
        """

        """
        data = {"SADD", key, members}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)

    def spop(self, key, count, collection):
        """

        """
        data = {"SPOP", key, count}
        test = dumps(data)
        request = Request(
            method='post',
            endpoint='/redis/' + collection,
            data=dumps(data)
        )

        # Parse response method that is sent to execute
        def response_handler(resp):
            if not resp.is_success:
                print("Error")
            return resp.body

        return self._execute(request, response_handler)