from __future__ import absolute_import, unicode_literals

from c8.api import APIWrapper
from c8.exceptions import FunctionCreateError
from c8.exceptions import FunctionDeleteError
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
        :rtype: dict
        :raise c8.exceptions.FunctionListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_fn'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionListError(resp, request)
            return resp.body['result']

        return self._execute(request, response_handler)

    def create_function(self, name, image, triggers, trigger_type, is_local):
        """Create a new function.
        :param name: function name.
        :type name: str | unicode
        :param image: image name from docker hub
        :type image: str
        :param triggers: trigger names
        :type triggers: list
        :param trigger_type: type of trigger (stream/collection)
        :type trigger_type: str
        :param is_local: true if trigger is local else false
        :type is_local: bool
        :return: True if function is created successfully.
        :rtype: bool
        :raise c8.exceptions.FunctionCreateError: If create fails.
        """
        data = {
            "fnSize": "xlarge",
            "image": image,
            "inputTrigger": triggers,
            "inputTriggerType": trigger_type,
            "local": is_local,
            "service": name
        }
        request = Request(method='post', endpoint='/_fn', data=data)

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionCreateError(resp, request)
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

    def delete_function(self, name):
        """Update a existing function.

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

    def update_input_triggers_function(self, name, triggers, trigger_type):
        """Update input triggers for a existing function.
        :param name: function name.
        :type name: str | unicode
        :param triggers: trigger names
        :type triggers: list
        :param trigger_type: type of trigger (stream/collection)
        :type trigger_type: str
        :return: True if function was updated successfully.
        :rtype: bool
        :raise c8.exceptions.FunctionUpdateError: If update fails.
        """
        data = {"inputTrigger": triggers, "inputTriggerType": trigger_type}
        request = Request(method='put', data=data,
                          endpoint='/_fn/%s/inputTriggers' % name)

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionUpdateError(resp, request)
            return True

        return self._execute(request, response_handler)

    def update_funtion(self, name, function_size, image):
        """Update function size and image for a existing function.
        :param name: function name.
        :type name: str | unicode
        :param function_size: size of function
        :type triggers: str | unicode
        :param image: image name
        :type image: str | unicode
        :return: True if function was updated successfully.
        :rtype: bool
        :raise c8.exceptions.FunctionUpdateError: If update fails.
        """
        data = {"fnSize": function_size, "image": image, "service": name}
        request = Request(method='put', data=data, endpoint='/_fn')

        def response_handler(resp):
            if not resp.is_success:
                raise FunctionUpdateError(resp, request)
            return True

        return self._execute(request, response_handler)
