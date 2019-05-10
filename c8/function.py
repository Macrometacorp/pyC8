from __future__ import absolute_import, unicode_literals

from c8.api import APIWrapper
from c8.exceptions import FunctionCreateError
from c8.exceptions import FunctionDeleteError
from c8.exceptions import FunctionExecuteError
from c8.exceptions import FunctionGetError
from c8.exceptions import FunctionListError
from c8.exceptions import FunctionUpdateError
from c8.executor import DefaultExecutor
from c8.request import Request

__all__ = ['Function']


class Function(APIWrapper):
    """Base class for Function API wrappers.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection):
        super(Function, self).__init__(connection,
                                       executor=DefaultExecutor(connection))

    def list_functions(self):
        """Return the details of all functions.

        :return: Function details.
        :rtype: list
        :raise c8.exceptions.FunctionListError: If retrieval fails.
        """
        request = Request(method='get', endpoint='/_fn')

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionListError(resp, request)
            return resp.body['result']

        return self._execute(request, response_handler)

    def create_function(self, data):
        """Create a new function.

        :param data: data to create function.
        :type data: dict
        :return: True if function is created successfully.
        :rtype: bool
        :raise c8.exceptions.FunctionCreateError: If create fails.
        """
        request = Request(method='post', endpoint='/_fn', data=data)

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionCreateError(resp, request)
            return True

        return self._execute(request, response_handler)

    def update_funtion(self, name, is_local, size="", image=""):
        """Update function size and image for a existing function.
        :param name: function name.
        :type name: str | unicode
        :param is_local: True if function is local else false
        :tye is_local: bool
        :param size: size of function
        :type triggers: str | unicode
        :param image: image name
        :type image: str | unicode
        :return: True if function was updated successfully.
        :rtype: bool
        :raise c8.exceptions.FunctionUpdateError: If update fails.
        """
        data = {"service": name, "local": is_local}
        if size:
            data["fnSize"] = size
        if image:
            data["image"] = image
        request = Request(method='put', data=data, endpoint='/_fn')

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionUpdateError(resp, request)
            return True

        return self._execute(request, response_handler)

    def get_health(self):
        """
        :return: True if function health is good.
        :rtype: bool
        :raise c8.exceptions.FunctionGetError: If getting health status failed.
        """
        request = Request(method='get', endpoint='/_fn/health')

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionGetError(resp, request)
            return True

        return self._execute(request, response_handler)

    def get_function_summary(self, name):
        """Get summary of function.

        :param name: function name.
        :type name: str | unicode
        :return: summary of functions.
        :rtype: dict
        :raise c8.exceptions.FunctionGetError: If getting summary fails.
        """
        request = Request(method='get', endpoint='/_fn/summary/%s' % name)

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionGetError(resp, request)
            return resp.body['result']

        return self._execute(request, response_handler)

    def update_input_triggers(self, name, is_local, triggers=[],
                              trigger_type=""):
        """Update input triggers for a existing function.
        :param name: function name.
        :type name: str | unicode
        :param is_local: True if function is local else false
        :tye is_local: bool
        :param triggers: trigger names
        :type triggers: list
        :param trigger_type: type of trigger (stream/collection)
        :type trigger_type: str
        :return: True if function was updated successfully.
        :rtype: bool
        :raise c8.exceptions.FunctionUpdateError: If update fails.
        """
        data = {"local": is_local}
        if triggers:
            data["inputTrigger"] = triggers
        if trigger_type:
            data["inputTriggerType"] = trigger_type
        request = Request(method='put', data=data,
                          endpoint='/_fn/%s/inputTriggers' % name)

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionUpdateError(resp, request)
            return True

        return self._execute(request, response_handler)

    def delete_function(self, name):
        """Delete a existing function.

        :param name: function name.
        :type name: str | unicode
        :return: True if function was deleted successfully.
        :rtype: bool
        :raise c8.exceptions.FunctionDeleteError: If delete fails.
        """
        request = Request(method='delete', endpoint='/_fn/{}'.format(name))

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionDeleteError(resp, request)
            return True

        return self._execute(request, response_handler)

    def execute_function(self, name, data):
        """Execute function.

        :param name: function name.
        :type name: str | unicode
        :param data: data to be executed for function
        :type data: dict
        :return: True if function was deleted successfully.
        :rtype: bool
        :raise c8.exceptions.FunctionDeleteError: If delete fails.
        """
        request = Request(method='post', endpoint='/_fn/{}'.format(name),
                          data=data)

        def response_handler(resp):
            print("resp: %s" % resp.body)
            if not resp.is_success:
                raise FunctionExecuteError(resp, request)
            return True

        return self._execute(request, response_handler)
