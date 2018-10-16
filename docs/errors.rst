Error Handling
--------------

All pyC8 exceptions inherit :class:`c8.exceptions.C8Error`,
which splits into subclasses :class:`c8.exceptions.C8ServerError` and
:class:`c8.exceptions.C8ClientError`.

Server Errors
=============

:class:`c8.exceptions.C8ServerError` exceptions lightly wrap non-2xx
HTTP responses coming from C8 Data Fabric. Each exception object contains the error
message, error code and HTTP request response details.

**Example:**

.. testcode::

    from c8 import C8Client, C8ServerError, DocumentInsertError

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the API wrapper for "students" collection.
    students = db.collection('students')

    try:
        students.insert({'_key': 'John'})
        students.insert({'_key': 'John'})  # duplicate key error

    except DocumentInsertError as exc:

        assert isinstance(exc, C8ServerError)
        assert exc.source == 'server'

        exc.message           # Exception message usually from C8 Data Fabric
        exc.error_message     # Raw error message from C8 Data Fabric
        exc.error_code        # Error code from C8 Data Fabric
        exc.url               # URL (API endpoint)
        exc.http_method       # HTTP method (e.g. "POST")
        exc.http_headers      # Response headers
        exc.http_code         # Status code (e.g. 200)

        # You can inspect the C8 Data Fabric response directly.
        response = exc.response
        response.method       # HTTP method (e.g. "POST")
        response.headers      # Response headers
        response.url          # Full request URL
        response.is_success   # Set to True if HTTP code is 2XX
        response.body         # JSON-deserialized response body
        response.raw_body     # Raw string response body
        response.status_text  # Status text (e.g "OK")
        response.status_code  # Status code (e.g. 200)
        response.error_code   # Error code from C8 Data Fabric

        # You can also inspect the request sent to C8 Data Fabric.
        request = exc.request
        request.method        # HTTP method (e.g. "post")
        request.endpoint      # API endpoint starting with "/_api"
        request.headers       # Request headers
        request.params        # URL parameters
        request.data          # Request payload
        request.read          # Read collections (used for transactions only)
        request.write         # Write collections (used for transactions only)
        request.command       # C8Sh command (used for transactions only)

See :ref:`Response` and :ref:`Request` for reference.

Client Errors
=============

:class:`c8.exceptions.C8ClientError` exceptions originate from
pyC8 client itself. They do not contain error codes nor HTTP request
response details.

**Example:**

.. testcode::

    from c8 import C8Client, C8ClientError, DocumentParseError

    # Initialize the C8 Data Fabric client.
    client = C8Client(protocol='https', host='MY-C8-EDGE-DATA-FABRIC-URL', port=443)

    # For the "mytenant" tenant, connect to "test" database as tenant admin.
    # This returns an API wrapper for the "test" database on tenant 'mytenant'
    # Note that the 'mytenant' tenant should already exist.
    db = client.db(tenant='mytenant', name='test', username='root', password='passwd')

    # Get the API wrapper for "students" collection.
    students = db.collection('students')

    try:
        students.get({'_id': 'invalid_id'})  # malformed document

    except DocumentParseError as exc:

        assert isinstance(exc, C8ClientError)
        assert exc.source == 'client'

        # Only the error message is set.
        error_message = exc.message
        assert exc.error_code is None
        assert exc.error_message is None
        assert exc.url is None
        assert exc.http_method is None
        assert exc.http_code is None
        assert exc.http_headers is None
        assert exc.response is None
        assert exc.request is None

Exceptions
==========

Below are all exceptions from pyC8.

.. automodule:: c8.exceptions
    :members:

exception c8.exceptions.C8Error

   Base class for all exceptions in pyC8.

exception c8.exceptions.C8ClientError(msg)

   Base class for errors originating from pyC8 client.

   Parameters:
      **msg** (*str | unicode*) -- Error message.

   Variables:
      * **source** (*str | unicode*) -- Source of the error (always
        set to "client").

      * **message** (*str | unicode*) -- Error message.

exception c8.exceptions.C8ServerError(resp, request, msg=None)

   Base class for errors originating from C8Db server.

   Parameters:
      * **resp** (*c8.response.Response*) -- HTTP response.

      * **msg** (*str | unicode*) -- Error message override.

   Variables:
      * **source** (*str | unicode*) -- Source of the error (always
        set to "server").

      * **message** (*str | unicode*) -- Exception message.

      * **url** (*str | unicode*) -- API URL.

      * **response** (*c8.response.Response*) -- HTTP response
        object.

      * **request** (*c8.request.Request*) -- HTTP request object.

      * **http_method** (*str | unicode*) -- HTTP method in
        lowercase (e.g. "post").

      * **http_code** (*int*) -- HTTP status code.

      * **http_headers** (*requests.structures.CaseInsensitiveDict |
        dict*) -- Response headers.

      * **error_code** (*int*) -- Error code from C8Db server.

      * **error_message** (*str | unicode*) -- Raw error message
        from C8Db server.

exception c8.exceptions.StreamProducerError(msg)

   Failed to create Stream Producer

exception c8.exceptions.StreamSubscriberError(msg)

   Failed to create Stream Subscriber

exception c8.exceptions.StreamConnectionError(resp, request, msg=None)

   Failed to connect to C8 stream.

exception c8.exceptions.StreamListError(resp, request, msg=None)

   Failed to retrieve streams.

exception c8.exceptions.StreamPropertiesError(resp, request, msg=None)

   Failed to retrieve stream properties.

exception c8.exceptions.StreamCreateError(resp, request, msg=None)

   Failed to create stream.

exception c8.exceptions.StreamDeleteError(resp, request, msg=None)

   Failed to delete stream.

exception c8.exceptions.StreamUpdateError(resp, request, msg=None)

   Failed to update stream content.

exception c8.exceptions.StreamStatisticsError(resp, request, msg=None)

   Failed to get stream stats.

exception c8.exceptions.StreamPermissionError(resp, request, msg=None)

   Don't have permission

exception c8.exceptions.CompactionCreateError(resp, request, msg=None)

   Compaction already exists

exception c8.exceptions.StreamCommunicationError(resp, request, msg=None)

   If an error related to C8Streams communication was encountered.

exception c8.exceptions.StreamEventError(msg)

   Failed to process the event from C8 stream.

exception c8.exceptions.StreamBadInputError(msg)

   If the request doesn’t have the expected format.

exception c8.exceptions.TenantListError(resp, request, msg=None)

   Failed to retrieve tenants.

exception c8.exceptions.TenantDcListError(resp, request, msg=None)

   Failed to retrieve list of C8 Data Centers.

exception c8.exceptions.TenantPropertiesError(resp, request, msg=None)

   Failed to retrieve tenant properties.

exception c8.exceptions.TenantCreateError(resp, request, msg=None)

   Failed to create tenant.

exception c8.exceptions.TenantUpdateError(msg)

   Failed to update tenant.

exception c8.exceptions.TenantDeleteError(resp, request, msg=None)

   Failed to delete tenant.

exception c8.exceptions.TopicListError(resp, request, msg=None)

   Failed to retrieve topic.

exception c8.exceptions.TopicPropertiesError(resp, request, msg=None)

   Failed to retrieve topic properties.

exception c8.exceptions.TopicCreateError(resp, request, msg=None)

   Failed to create topic.

exception c8.exceptions.TopicDeleteError(resp, request, msg=None)

   Failed to delete topic.

exception c8.exceptions.SubscriptionDeleteError(resp, request, msg=None)

   Failed to delete subscription.

exception c8.exceptions.SubscriptionUpdateError(resp, request, msg=None)

   Failed to update subscription.

exception c8.exceptions.TopicStatisticsError(resp, request, msg=None)

   Failed to get topic stats.

exception c8.exceptions.C8QLQueryListError(resp, request, msg=None)

   Failed to retrieve running C8QL queries.

exception c8.exceptions.C8QLQueryExplainError(resp, request, msg=None)

   Failed to parse and explain query.

exception c8.exceptions.C8QLQueryValidateError(resp, request, msg=None)

   Failed to parse and validate query.

exception c8.exceptions.C8QLQueryExecuteError(resp, request, msg=None)

   Failed to execute query.

exception c8.exceptions.C8QLQueryKillError(resp, request, msg=None)

   Failed to kill the query.

exception c8.exceptions.C8QLQueryClearError(resp, request, msg=None)

   Failed to clear slow C8QL queries.

exception c8.exceptions.C8QLFunctionListError(resp, request, msg=None)

   Failed to retrieve C8QL user functions.

exception c8.exceptions.C8QLFunctionCreateError(resp, request, msg=None)

   Failed to create C8QL user function.

exception c8.exceptions.C8QLFunctionDeleteError(resp, request, msg=None)

   Failed to delete C8QL user function.

exception c8.exceptions.AsyncExecuteError(resp, request, msg=None)

   Failed to execute async API request.

exception c8.exceptions.AsyncJobListError(resp, request, msg=None)

   Failed to retrieve async jobs.

exception c8.exceptions.AsyncJobCancelError(resp, request, msg=None)

   Failed to cancel async job.

exception c8.exceptions.AsyncJobStatusError(resp, request, msg=None)

   Failed to retrieve async job status.

exception c8.exceptions.AsyncJobResultError(resp, request, msg=None)

   Failed to retrieve async job result.

exception c8.exceptions.AsyncJobClearError(resp, request, msg=None)

   Failed to clear async job results.

exception c8.exceptions.BatchStateError(msg)

   The batch object was in a bad state.

exception c8.exceptions.BatchJobResultError(msg)

   Failed to retrieve batch job result.

exception c8.exceptions.BatchExecuteError(resp, request, msg=None)

   Failed to execute batch API request.

exception c8.exceptions.CollectionListError(resp, request, msg=None)

   Failed to retrieve collections.

exception c8.exceptions.CollectionCreateError(resp, request, msg=None)

   Failed to create collection.

exception c8.exceptions.CollectionDeleteError(resp, request, msg=None)

   Failed to delete collection.

exception c8.exceptions.CollectionRenameError(resp, request, msg=None)

   Failed to rename collection.

exception c8.exceptions.CollectionTruncateError(resp, request, msg=None)

   Failed to truncate collection.

exception c8.exceptions.CursorStateError(msg)

   The cursor object was in a bad state.

exception c8.exceptions.CursorEmptyError(msg)

   The current batch in cursor was empty.

exception c8.exceptions.CursorNextError(resp, request, msg=None)

   Failed to retrieve the next result batch from server.

exception c8.exceptions.CursorCloseError(resp, request, msg=None)

   Failed to delete the cursor result from server.

exception c8.exceptions.DatabaseListError(resp, request, msg=None)

   Failed to retrieve databases.

exception c8.exceptions.DatabasePropertiesError(resp, request, msg=None)

   Failed to retrieve database properties.

exception c8.exceptions.DatabaseCreateError(resp, request, msg=None)

   Failed to create database.

exception c8.exceptions.DatabaseDeleteError(resp, request, msg=None)

   Failed to delete database.

exception c8.exceptions.DocumentParseError(msg)

   Failed to parse document input.

exception c8.exceptions.DocumentCountError(resp, request, msg=None)

   Failed to retrieve document count.

exception c8.exceptions.DocumentInError(resp, request, msg=None)

   Failed to check whether document exists.

exception c8.exceptions.DocumentGetError(resp, request, msg=None)

   Failed to retrieve document.

exception c8.exceptions.DocumentKeysError(resp, request, msg=None)

   Failed to retrieve document keys.

exception c8.exceptions.DocumentIDsError(resp, request, msg=None)

   Failed to retrieve document IDs.

exception c8.exceptions.DocumentInsertError(resp, request, msg=None)

   Failed to insert document.

exception c8.exceptions.DocumentReplaceError(resp, request, msg=None)

   Failed to replace document.

exception c8.exceptions.DocumentUpdateError(resp, request, msg=None)

   Failed to update document.

exception c8.exceptions.DocumentDeleteError(resp, request, msg=None)

   Failed to delete document.

exception c8.exceptions.DocumentRevisionError(resp, request, msg=None)

   The expected and actual document revisions mismatched.

exception c8.exceptions.GraphListError(resp, request, msg=None)

   Failed to retrieve graphs.

exception c8.exceptions.GraphCreateError(resp, request, msg=None)

   Failed to create the graph.

exception c8.exceptions.GraphDeleteError(resp, request, msg=None)

   Failed to delete the graph.

exception c8.exceptions.GraphPropertiesError(resp, request, msg=None)

   Failed to retrieve graph properties.

exception c8.exceptions.GraphTraverseError(resp, request, msg=None)

   Failed to execute graph traversal.

exception c8.exceptions.VertexCollectionListError(resp, request, msg=None)

   Failed to retrieve vertex collections.

exception c8.exceptions.VertexCollectionCreateError(resp, request, msg=None)

   Failed to create vertex collection.

exception c8.exceptions.VertexCollectionDeleteError(resp, request, msg=None)

   Failed to delete vertex collection.

exception c8.exceptions.EdgeDefinitionListError(resp, request, msg=None)

   Failed to retrieve edge definitions.

exception c8.exceptions.EdgeDefinitionCreateError(resp, request, msg=None)

   Failed to create edge definition.

exception c8.exceptions.EdgeDefinitionReplaceError(resp, request, msg=None)

   Failed to replace edge definition.

exception c8.exceptions.EdgeDefinitionDeleteError(resp, request, msg=None)

   Failed to delete edge definition.

exception c8.exceptions.EdgeListError(resp, request, msg=None)

   Failed to retrieve edges coming in and out of a vertex.

exception c8.exceptions.IndexListError(resp, request, msg=None)

   Failed to retrieve collection indexes.

exception c8.exceptions.IndexCreateError(resp, request, msg=None)

   Failed to create collection index.

exception c8.exceptions.IndexDeleteError(resp, request, msg=None)

   Failed to delete collection index.

exception c8.exceptions.ServerConnectionError(msg)

   Failed to connect to C8Db server.

exception c8.exceptions.ServerVersionError(resp, request, msg=None)

   Failed to retrieve server version.

exception c8.exceptions.ServerDetailsError(resp, request, msg=None)

   Failed to retrieve server details.

exception c8.exceptions.ServerTimeError(resp, request, msg=None)

   Failed to retrieve server system time.

exception c8.exceptions.TransactionStateError(msg)

   The transaction object was in bad state.

exception c8.exceptions.TransactionJobResultError(msg)

   Failed to retrieve transaction job result.

exception c8.exceptions.TransactionExecuteError(resp, request, msg=None)

   Failed to execute transaction API request

exception c8.exceptions.UserListError(resp, request, msg=None)

   Failed to retrieve users.

exception c8.exceptions.UserGetError(resp, request, msg=None)

   Failed to retrieve user details.

exception c8.exceptions.UserCreateError(resp, request, msg=None)

   Failed to create user.

exception c8.exceptions.UserUpdateError(resp, request, msg=None)

   Failed to update user.

exception c8.exceptions.UserReplaceError(resp, request, msg=None)

   Failed to replace user.

exception c8.exceptions.UserDeleteError(resp, request, msg=None)

   Failed to delete user.

exception c8.exceptions.PermissionListError(resp, request, msg=None)

   Failed to list user permissions.

exception c8.exceptions.PermissionGetError(resp, request, msg=None)

   Failed to retrieve user permission.

exception c8.exceptions.PermissionUpdateError(resp, request, msg=None)

   Failed to update user permission.

exception c8.exceptions.PermissionResetError(resp, request, msg=None)

   Failed to reset user permission.
