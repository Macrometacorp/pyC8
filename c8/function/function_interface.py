from c8.api import APIWrapper


class FunctionInterface(APIWrapper):
    """Redis API wrapper.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    def __init__(self, connection, executor):
        super(FunctionInterface, self).__init__(connection, executor)

    def __repr__(self):
        return "<FunctionInterface in {}>".format(self._conn.fabric_name)

    def list_function_workers(self, worker_type="all"):
        """
        List multiple function workers.

        :param worker_type: Worker type (ex. Akamai)
        :type worker_type: str

        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        pass

    def deploy_query_worker_to_edge_worker(
        self, worker_type, name, environment, query_worker_name
    ):
        """
        Deploy the query worker to the edge worker environment. Note: The edge worker
        activation is delayed while the environment is set up. Use the get function
        worker information API to get the activation status of the edge worker.

        :param worker_type: Key of the data
        :type worker_type: str
        :param name: Name of the edge worker
        :type name: str
        :param query_worker_name: Name of the Macrometa query worker
        :type query_worker_name: str
        :param environment: The environment in which the edge worker is activated.
        STAGING | PRODUCTION
        :type environment: str

        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        pass

    def deploy_stream_publisher_to_edge_worker(
        self, worker_type, name, environment, stream_worker_name, stream_name
    ):
        """
        Deploy the query worker to the edge worker environment. Note: The edge worker
        activation is delayed while the environment is set up. Use the get function
        worker information API to get the activation status of the edge worker.

        :param worker_type: Key of the data
        :type worker_type: str
        :param name: Name of the edge worker
        :type name: str
        :param stream_worker_name: Name of the Macrometa stream worker
        :type stream_worker_name: str
        :param stream_name: Name of the Macrometa stream
        :type stream_name: str
        :param environment: The environment in which the edge worker is activated.
        STAGING | PRODUCTION
        :type environment: str

        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        pass

    def deploy_stream_adhoc_query_to_edge_worker(
        self, worker_type, name, environment, stream_worker_name
    ):
        """
        Deploy the stream adhoc query to the edge worker environment. Note: The edge worker
        activation is delayed while the environment is set up. Use the get function
        worker information API to get the activation status of the edge worker.

        :param worker_type: Key of the data
        :type worker_type: str
        :param name: Name of the edge worker
        :type name: str
        :param stream_worker_name: Name of the Macrometa stream worker
        :type stream_worker_name: str
        :param environment: The environment in which the edge worker is activated.
        STAGING | PRODUCTION
        :type environment: str

        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        pass

    def get_function_worker_info(self, function_name):
        """
        Get function worker information.

        :param function_name: Function name
        :type function_name: str

        :returns: Returns response from server in format {"code": xx, "result": xx}
        :rtype: dict
        """
        pass

    def remove_function_worker(self, function_name):
        pass

    def invoke_function_worker(self, function_name, parameters):
        pass

    def get_edge_worker_metadata(self):
        pass

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
        pass

    def delete_edge_woker_metadata(self):
        pass

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
        pass
