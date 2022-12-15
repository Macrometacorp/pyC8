from json import dumps

from c8.api import APIWrapper
from c8.executor import DefaultExecutor
from c8.function.core import FunctionServerError
from c8.request import Request


class FunctionInterface(APIWrapper):
    """Redis API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    """

    def __init__(self, connection):
        super(FunctionInterface, self).__init__(
            connection, executor=DefaultExecutor(connection)
        )

    def __repr__(self):
        return "<FunctionInterface in {}>".format(self._conn.fabric_name)

    def execute(self, request, exception_type):
        def response_handler(response):
            if not response.is_success and request is not None:
                raise exception_type(response, request)
            return response.body

        return self._execute(request, response_handler)

    def list_function_workers(self, worker_type="all"):
        """
        List multiple function workers.

        :param worker_type: Worker type (ex. Akamai)
        :type worker_type: str

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        request = Request(
            method="get", endpoint="/function", params={"type": worker_type}
        )

        return self.execute(request, FunctionServerError)

    def deploy_query_worker_to_edge_worker(
        self, name, query_worker_name, worker_type="akamai", environment="PRODUCTION"
    ):
        """
        Deploy the query worker to the edge worker environment. Note: The edge worker
        activation is delayed while the environment is set up. Use the get function
        worker information API to get the activation status of the edge worker.

        :param name: Name of the edge worker
        :type name: str
        :param query_worker_name: Name of the Macrometa query worker
        :type query_worker_name: str
        :param worker_type: Key of the data
        :type worker_type: str
        :param environment: The environment in which the edge worker is activated.
        STAGING | PRODUCTION
        :type environment: str

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """

        query_parameters = {
            "type": worker_type,
            "name": name,
            "queryWorkerName": query_worker_name,
            "environment": environment,
        }
        if worker_type != "akamai":
            query_parameters["type"] = worker_type
        if environment != "production":
            query_parameters["environment"] = environment

        request = Request(
            method="post",
            endpoint="/function/generate",
            params=query_parameters,
            data={},
        )

        return self.execute(request, FunctionServerError)

    def deploy_stream_publisher_to_edge_worker(
        self,
        name,
        stream_worker_name,
        stream_name,
        worker_type="akamai",
        environment="PRODUCTION",
    ):
        """
        Deploy the stream publisher to the edge worker environment.
        Note: The edge worker activation is delayed while the environment is set up.
        Use the get edge function information API to get the activation status of
        the edge worker.

        :param name: Name of the edge worker
        :type name: str
        :param stream_worker_name: Name of the Macrometa stream worker
        :type stream_worker_name: str
        :param stream_name: Name of the Macrometa stream
        :type stream_name: str
        :param worker_type: Key of the data
        :type worker_type: str
        :param environment: The environment in which the edge worker is activated.
        STAGING | PRODUCTION
        :type environment: str

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        query_parameters = {
            "type": worker_type,
            "name": name,
            "streamWorkerName": stream_worker_name,
            "streamName": stream_name,
            "environment": environment,
        }

        if worker_type != "akamai":
            query_parameters["type"] = worker_type
        if environment != "production":
            query_parameters["environment"] = environment

        request = Request(
            method="post",
            endpoint="/function/generate/publisher",
            params=query_parameters,
            data={},
        )

        return self.execute(request, FunctionServerError)

    def deploy_stream_adhoc_query_to_edge_worker(
        self, name, stream_worker_name, worker_type="akamai", environment="PRODUCTION"
    ):
        """
        Deploy the stream adhoc query to the edge worker environment. Note: The edge worker
        activation is delayed while the environment is set up. Use the get function
        worker information API to get the activation status of the edge worker.

        :param name: Name of the edge worker
        :type name: str
        :param stream_worker_name: Name of the Macrometa stream worker
        :type stream_worker_name: str
        :param worker_type: Key of the data
        :type worker_type: str
        :param environment: The environment in which the edge worker is activated.
        STAGING | PRODUCTION
        :type environment: str

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        query_parameters = {
            "type": worker_type,
            "name": name,
            "streamWorkerName": stream_worker_name,
            "environment": environment,
        }

        if worker_type != "akamai":
            query_parameters["type"] = worker_type
        if environment != "production":
            query_parameters["environment"] = environment

        request = Request(
            method="post",
            endpoint="/function/generate/query",
            params=query_parameters,
            data={},
        )

        return self.execute(request, FunctionServerError)

    def get_function_worker_info(self, function_name):
        """
        Get function worker information.

        :param function_name: Function name
        :type function_name: str

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        request = Request(method="get", endpoint="/function/{}".format(function_name))

        return self.execute(request, FunctionServerError)

    def remove_function_worker(self, function_name):
        """
        Remove edge function.

        :param function_name: Function name
        :type function_name: str

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        request = Request(
            method="delete",
            endpoint="/function/{}".format(function_name),
            headers={"content-type": "text/plain"},
        )

        return self.execute(request, FunctionServerError)

    def invoke_function_worker(self, function_name, parameters):
        """
        Remove edge function.

        :param function_name: Function name
        :type function_name: str

        :param parameters: Key-Value pair of parameters that EV receives
        :type parameters: dict

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        query_parameters = {"params": dumps(parameters)}
        request = Request(
            method="post",
            endpoint="/function/invoke/{}".format(function_name),
            params=query_parameters,
            data={},
        )

        return self.execute(request, FunctionServerError)

    def get_edge_worker_metadata(self):
        """
        Get metadata about the edge worker.

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        request = Request(method="get", endpoint="/function/metadata")

        return self.execute(request, FunctionServerError)

    def modify_edge_worker_metadata(
        self,
        worker_type,
        access_token,
        base_uri,
        client_secret,
        client_token,
        resource_tier_id,
        group_id,
        host_name,
    ):
        """
        Modify the edge worker metadata.

        :param worker_type: Key of the data
        :type worker_type: str
        :param access_token: Akamai platform access token
        :type access_token: str
        :param base_uri: Akamai endpoint URL, format akab-***.luna.akamaiapis.net
        :type base_uri: str
        :param client_secret: Akamai client secret
        :type client_secret: str
        :param client_token: Akamai client token
        :type client_token: str
        :param resource_tier_id: Akamai resource tier id
        :type resource_tier_id: str
        :param group_id: Akamai group id
        :type group_id: str
        :param host_name: Akamai property host name
        :type host_name: str

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        data = {
            "type": worker_type,
            "accessToken": access_token,
            "baseUri": base_uri,
            "clientSecret": client_secret,
            "clientToken": client_token,
            "resourceTierId": resource_tier_id,
            "groupId": group_id,
            "hostName": host_name,
        }
        request = Request(method="put", endpoint="/function/metadata", data=dumps(data))

        return self.execute(request, FunctionServerError)

    def delete_edge_worker_metadata(self):
        """
        Remove metadata for an edge worker.

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        request = Request(
            method="delete",
            endpoint="/function/metadata",
            headers={"content-type": "text/plain"},
        )

        return self.execute(request, FunctionServerError)

    def create_edge_worker_metadata(
        self,
        worker_type,
        access_token,
        base_uri,
        client_secret,
        client_token,
        resource_tier_id,
        group_id,
        host_name,
    ):
        """
        Create the edge worker metadata.

        :param worker_type: Key of the data
        :type worker_type: str
        :param access_token: Akamai platform access token
        :type access_token: str
        :param base_uri: Akamai endpoint URL, format akab-***.luna.akamaiapis.net
        :type base_uri: str
        :param client_secret: Akamai client secret
        :type client_secret: str
        :param client_token: Akamai client token
        :type client_token: str
        :param resource_tier_id: Akamai resource tier id
        :type resource_tier_id: str
        :param group_id: Akamai group id
        :type group_id: str
        :param host_name: Akamai property host name
        :type host_name: str

        :returns: Returns response in format {"code": xx, "error": xx "result": xx}
        :rtype: dict
        """
        data = {
            "type": worker_type,
            "accessToken": access_token,
            "baseUri": base_uri,
            "clientSecret": client_secret,
            "clientToken": client_token,
            "resourceTierId": resource_tier_id,
            "groupId": group_id,
            "hostName": host_name,
        }
        request = Request(
            method="post", endpoint="/function/metadata", data=dumps(data)
        )

        return self.execute(request, FunctionServerError)
