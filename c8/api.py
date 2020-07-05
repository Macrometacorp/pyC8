from __future__ import absolute_import, unicode_literals

__all__ = ['APIWrapper']


class APIWrapper(object):
    """Base class for API wrappers.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection, executor):
        self._conn = connection
        self._executor = executor
        self._is_transaction = self.context == 'transaction'

    @property
    def tenant_name(self):
        """Return the name of the current tenant.

        :return: tenant name.
        :rtype: str | unicode
        """
        return self._conn.tenant_name

    @property
    def fabric_name(self):
        """Return the name of the current fabric.

        :return: Fabric name.
        :rtype: str | unicode
        """
        return self._conn.fabric_name

    @property
    def stream_name(self):
        """Return the name of the current stream.

        :return: Stream name.
        :rtype: str | unicode
        """
        return self._conn.stream_name

    @property
    def topic_name(self):
        """Return the name of the current topic.

        :return: topic name.
        :rtype: str | unicode
        """
        return self._conn.topic_name

    @property
    def topic_persistence(self):
        """Return true if it is a persistent topic.

        :return: topic persistence.
        :rtype: bool
        """
        return self._conn.topic_persistence


    @property
    def username(self):
        """Return the username.

        :returns: Username.
        :rtype: str | unicode
        """
        return self._conn.username

    @property
    def context(self):
        """Return the API execution context.

        :return: API execution context. Possible values are "default", "async",
            "batch" and "transaction".
        :rtype: str | unicode
        """
        return self._executor.context

    def _execute(self, request, response_handler):
        """Execute an API per execution context.

        :param request: HTTP request.
        :type request: c8.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: API execution result.
        :rtype: str | unicode | bool | int | list | dict
        """
        return self._executor.execute(request, response_handler)
