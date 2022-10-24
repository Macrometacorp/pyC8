import json

from c8.api import APIWrapper
from c8.exceptions import StreamAppChangeActiveStateError
from c8.request import Request


class StreamApps(APIWrapper):
    """Base class for collection API wrappers.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    :param name: Collection name.
    :type
    """

    def __init__(self, connection, executor, name):
        super(StreamApps, self).__init__(connection, executor)
        self._name = name
        self._id_prefix = name + "/"

    @property
    def name(self):
        """Return stream app name.

        :return: stream app name.
        :rtype: str | unicode
        """
        return self._name

    def update(self, data, dclist=[]):
        """updates the stream app
        @data: stream app definition
        @dclist: regions where stream app has to be deployed

        """
        # create request body
        req_body = {"definition": data, "regions": dclist}
        req = Request(
            method="put",
            endpoint="/streamapps/{}".format(self.name),
            data=json.dumps(req_body),
        )

        def response_handler(resp):
            if resp.is_success is True:
                return resp.body["streamApps"]
            return resp.body

        return self._execute(req, response_handler)

    def change_state(self, active):
        """enable or disable stream app
        @active: state of the stream app to be updated
        """
        if active is True:
            status = "true"
        elif active is False:
            status = "false"

        req = Request(
            method="patch",
            endpoint="/streamapps/{}/active?active={}".format(self.name, status),
        )

        def response_handler(resp):
            if resp.is_success is not True:
                raise StreamAppChangeActiveStateError(resp, req)
            return resp.body["streamApps"]

        return self._execute(req, response_handler)

    def get(self):
        """gets the stream app by name"""
        req = Request(
            method="get",
            endpoint="/streamapps/{}".format(self.name),
        )

        def response_handler(resp):
            if resp.is_success is True:
                return resp.body["streamApps"]
            return False

        return self._execute(req, response_handler)

    def delete(self):
        """deletes the stream app by name"""
        req = Request(
            method="delete",
            endpoint="/streamapps/{}".format(self.name),
        )

        def response_handler(resp):
            if resp.is_success is True:
                return True
            return False

        return self._execute(req, response_handler)

    def query(self, query):
        """query the stream app by name
        @query: query to be executed against the stream app
        """
        # create request body
        req_body = {"query": query}
        # create request
        req = Request(
            method="post",
            endpoint="/streamapps/query/{}".format(self.name),
            data=json.dumps(req_body),
        )
        # create response handler

        def response_handler(resp):
            if resp.is_success is True:
                return resp.body
            return False

        # call the api
        return self._execute(req, response_handler)

    def publish_message_http_source(self, stream, message):
        """publish messages via HTTP source streams
        @stream: name of the http source stream
        @message: message to be published
        """

        # create request
        req = Request(
            method="post",
            endpoint="/streamapps/http/{}/{}".format(self.name, stream),
            data=message,
        )
        # create response handler

        def response_handler(resp):
            if resp.is_success is True:
                return resp.body
            return False

        return self._execute(req, response_handler)
