from __future__ import absolute_import, unicode_literals


class C8Error(Exception):
    """Base class for all exceptions in pyC8."""


class C8ClientError(C8Error):
    """Base class for errors originating from pyC8 client.

    :param msg: Error message.
    :type msg: str | unicode

    :cvar source: Source of the error (always set to "client").
    :vartype source: str | unicode
    :ivar message: Error message.
    :vartype message: str | unicode
    """
    source = 'client'

    def __init__(self, msg):
        super(C8ClientError, self).__init__(msg)
        self.message = msg
        self.error_message = None
        self.error_code = None
        self.url = None
        self.response = None
        self.request = None
        self.http_method = None
        self.http_code = None
        self.http_headers = None


class C8ServerError(C8Error):
    """Base class for errors originating from C8Db server.

    :param resp: HTTP response.
    :type resp: c8.response.Response
    :param msg: Error message override.
    :type msg: str | unicode

    :cvar source: Source of the error (always set to "server").
    :vartype source: str | unicode
    :ivar message: Exception message.
    :vartype message: str | unicode
    :ivar url: API URL.
    :vartype url: str | unicode
    :ivar response: HTTP response object.
    :vartype response: c8.response.Response
    :ivar request: HTTP request object.
    :vartype request: c8.request.Request
    :ivar http_method: HTTP method in lowercase (e.g. "post").
    :vartype http_method: str | unicode
    :ivar http_code: HTTP status code.
    :vartype http_code: int
    :ivar http_headers: Response headers.
    :vartype http_headers: requests.structures.CaseInsensitiveDict | dict
    :ivar error_code: Error code from C8Db server.
    :vartype error_code: int
    :ivar error_message: Raw error message from C8Db server.
    :vartype error_message: str | unicode
    """
    source = 'server'

    def __init__(self, resp, request, msg=None):
        msg = msg or resp.error_message or resp.status_text
        self.error_message = resp.error_message
        self.error_code = resp.error_code
        if self.error_code is not None:
            msg = '[HTTP {}][ERR {}] {}'.format(
                resp.status_code, self.error_code, msg)
        else:
            msg = '[HTTP {}] {}'.format(resp.status_code, msg)
        super(C8ServerError, self).__init__(msg)
        self.message = msg
        self.url = resp.url
        self.response = resp
        self.request = request
        self.http_method = resp.method
        self.http_code = resp.status_code
        self.http_headers = resp.headers


#######################
# Stream Exceptions #
#######################

class StreamProducerError(C8ClientError):
    """Failed to create Stream Producer"""

class StreamSubscriberError(C8ClientError):
    """Failed to create Stream Subscriber"""

class StreamConnectionError(C8ServerError):
    """Failed to connect to C8 stream."""

class StreamListError(C8ServerError):
    """Failed to retrieve streams."""


class StreamPropertiesError(C8ServerError):
    """Failed to retrieve stream properties."""


class StreamCreateError(C8ServerError):
    """Failed to create stream."""

class StreamDeleteError(C8ServerError):
    """Failed to delete stream."""


class StreamUpdateError(C8ServerError):
    """Failed to update stream content."""    


class StreamStatisticsError(C8ServerError):
    """Failed to get stream stats."""


class StreamPermissionError(C8ServerError):
    """Don't have permission"""


class CompactionCreateError(C8ServerError):
    """Compaction already exists"""


class StreamCommunicationError(C8ServerError):
    """If an error related to C8Streams communication was encountered."""


class StreamEventError(C8ClientError):
    """Failed to process the event from C8 stream."""

class StreamBadInputError(C8ClientError):
    """If the request doesn’t have the expected format."""

#######################
# Tenant Exceptions #
#######################

class TenantListError(C8ServerError):
    """Failed to retrieve tenants."""


class TenantDcListError(C8ServerError):
    """Failed to retrieve list of C8 Data Centers."""


class TenantPropertiesError(C8ServerError):
    """Failed to retrieve tenant properties."""


class TenantCreateError(C8ServerError):
    """Failed to create tenant."""

class TenantUpdateError(C8ClientError):
    """Failed to update tenant."""


class TenantDeleteError(C8ServerError):
    """Failed to delete tenant."""



#######################
# Topic Exceptions #
#######################

class TopicListError(C8ServerError):
    """Failed to retrieve topic."""


class TopicPropertiesError(C8ServerError):
    """Failed to retrieve topic properties."""


class TopicCreateError(C8ServerError):
    """Failed to create topic."""


class TopicDeleteError(C8ServerError):
    """Failed to delete topic."""


class SubscriptionDeleteError(C8ServerError):
    """Failed to delete subscription."""

class SubscriptionUpdateError(C8ServerError):
    """Failed to update subscription."""

class TopicStatisticsError(C8ServerError):
    """Failed to get topic stats."""


##################
# C8QL Exceptions #
##################


class C8QLQueryListError(C8ServerError):
    """Failed to retrieve running C8QL queries."""


class C8QLQueryExplainError(C8ServerError):
    """Failed to parse and explain query."""


class C8QLQueryValidateError(C8ServerError):
    """Failed to parse and validate query."""


class C8QLQueryExecuteError(C8ServerError):
    """Failed to execute query."""


class C8QLQueryKillError(C8ServerError):
    """Failed to kill the query."""


class C8QLQueryClearError(C8ServerError):
    """Failed to clear slow C8QL queries."""


class C8QLFunctionListError(C8ServerError):
    """Failed to retrieve C8QL user functions."""


class C8QLFunctionCreateError(C8ServerError):
    """Failed to create C8QL user function."""


class C8QLFunctionDeleteError(C8ServerError):
    """Failed to delete C8QL user function."""


##############################
# Async Execution Exceptions #
##############################


class AsyncExecuteError(C8ServerError):
    """Failed to execute async API request."""


class AsyncJobListError(C8ServerError):
    """Failed to retrieve async jobs."""


class AsyncJobCancelError(C8ServerError):
    """Failed to cancel async job."""


class AsyncJobStatusError(C8ServerError):
    """Failed to retrieve async job status."""


class AsyncJobResultError(C8ServerError):
    """Failed to retrieve async job result."""


class AsyncJobClearError(C8ServerError):
    """Failed to clear async job results."""


##############################
# Batch Execution Exceptions #
##############################


class BatchStateError(C8ClientError):
    """The batch object was in a bad state."""


class BatchJobResultError(C8ClientError):
    """Failed to retrieve batch job result."""


class BatchExecuteError(C8ServerError):
    """Failed to execute batch API request."""


#########################
# Collection Exceptions #
#########################


class CollectionListError(C8ServerError):
    """Failed to retrieve collections."""


class CollectionCreateError(C8ServerError):
    """Failed to create collection."""


class CollectionDeleteError(C8ServerError):
    """Failed to delete collection."""


class CollectionRenameError(C8ServerError):
    """Failed to rename collection."""


class CollectionTruncateError(C8ServerError):
    """Failed to truncate collection."""


#####################
# Cursor Exceptions #
#####################


class CursorStateError(C8ClientError):
    """The cursor object was in a bad state."""


class CursorEmptyError(C8ClientError):
    """The current batch in cursor was empty."""


class CursorNextError(C8ServerError):
    """Failed to retrieve the next result batch from server."""


class CursorCloseError(C8ServerError):
    """Failed to delete the cursor result from server."""


#######################
# Fabric Exceptions #
#######################


class FabricListError(C8ServerError):
    """Failed to retrieve fabrics."""


class FabricPropertiesError(C8ServerError):
    """Failed to retrieve fabric properties."""


class FabricCreateError(C8ServerError):
    """Failed to create fabric."""


class FabricDeleteError(C8ServerError):
    """Failed to delete fabric."""

#######################
# Document Exceptions #
#######################


class DocumentParseError(C8ClientError):
    """Failed to parse document input."""


class DocumentCountError(C8ServerError):
    """Failed to retrieve document count."""


class DocumentInError(C8ServerError):
    """Failed to check whether document exists."""


class DocumentGetError(C8ServerError):
    """Failed to retrieve document."""


class DocumentKeysError(C8ServerError):
    """Failed to retrieve document keys."""


class DocumentIDsError(C8ServerError):
    """Failed to retrieve document IDs."""


class DocumentInsertError(C8ServerError):
    """Failed to insert document."""


class DocumentReplaceError(C8ServerError):
    """Failed to replace document."""


class DocumentUpdateError(C8ServerError):
    """Failed to update document."""


class DocumentDeleteError(C8ServerError):
    """Failed to delete document."""


class DocumentRevisionError(C8ServerError):
    """The expected and actual document revisions mismatched."""


####################
# Graph Exceptions #
####################


class GraphListError(C8ServerError):
    """Failed to retrieve graphs."""


class GraphCreateError(C8ServerError):
    """Failed to create the graph."""


class GraphDeleteError(C8ServerError):
    """Failed to delete the graph."""


class GraphPropertiesError(C8ServerError):
    """Failed to retrieve graph properties."""


class GraphTraverseError(C8ServerError):
    """Failed to execute graph traversal."""


class VertexCollectionListError(C8ServerError):
    """Failed to retrieve vertex collections."""


class VertexCollectionCreateError(C8ServerError):
    """Failed to create vertex collection."""


class VertexCollectionDeleteError(C8ServerError):
    """Failed to delete vertex collection."""


class EdgeDefinitionListError(C8ServerError):
    """Failed to retrieve edge definitions."""


class EdgeDefinitionCreateError(C8ServerError):
    """Failed to create edge definition."""


class EdgeDefinitionReplaceError(C8ServerError):
    """Failed to replace edge definition."""


class EdgeDefinitionDeleteError(C8ServerError):
    """Failed to delete edge definition."""


class EdgeListError(C8ServerError):
    """Failed to retrieve edges coming in and out of a vertex."""


####################
# Index Exceptions #
####################


class IndexListError(C8ServerError):
    """Failed to retrieve collection indexes."""


class IndexCreateError(C8ServerError):
    """Failed to create collection index."""


class IndexDeleteError(C8ServerError):
    """Failed to delete collection index."""


#####################
# Server Exceptions #
#####################


class ServerConnectionError(C8ClientError):
    """Failed to connect to C8Db server."""


class ServerVersionError(C8ServerError):
    """Failed to retrieve server version."""


class ServerDetailsError(C8ServerError):
    """Failed to retrieve server details."""


class ServerTimeError(C8ServerError):
    """Failed to retrieve server system time."""


##########################
# Transaction Exceptions #
##########################


class TransactionStateError(C8ClientError):
    """The transaction object was in bad state."""


class TransactionJobResultError(C8ClientError):
    """Failed to retrieve transaction job result."""


class TransactionExecuteError(C8ServerError):
    """Failed to execute transaction API request"""


###################
# User Exceptions #
###################


class UserListError(C8ServerError):
    """Failed to retrieve users."""


class UserGetError(C8ServerError):
    """Failed to retrieve user details."""


class UserCreateError(C8ServerError):
    """Failed to create user."""


class UserUpdateError(C8ServerError):
    """Failed to update user."""


class UserReplaceError(C8ServerError):
    """Failed to replace user."""


class UserDeleteError(C8ServerError):
    """Failed to delete user."""


#########################
# Permission Exceptions #
#########################


class PermissionListError(C8ServerError):
    """Failed to list user permissions."""


class PermissionGetError(C8ServerError):
    """Failed to retrieve user permission."""


class PermissionUpdateError(C8ServerError):
    """Failed to update user permission."""


class PermissionResetError(C8ServerError):
    """Failed to reset user permission."""
