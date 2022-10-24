from __future__ import absolute_import, unicode_literals

from c8.api import APIWrapper
from c8.exceptions import (
    AnalyzerGetDefinitionError,
    AnalyzerListError,
    AnalyzerNotFoundError,
    SearchCollectionForbiddenError,
    SearchCollectionInvalidArgument,
    SearchCollectionSetError,
    SearchError,
    SearchForbiddenError,
    SearchInvalidArgumentError,
    SearchNotExistError,
    ViewCreateError,
    ViewCreateViewNameMissingError,
    ViewCreateViewNameUnknownError,
    ViewDeleteError,
    ViewGetError,
    ViewGetPropertiesError,
    ViewNotFoundError,
    ViewRenameError,
    ViewUpdatePropertiesError,
)
from c8.request import Request


class Search(APIWrapper):
    """Base class for search api wrapper

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    :param name: Name of resource.
    :type name: str | unicode
    :param is_analyzer: View name.
    :type is_analyzer: bool
    """

    def __init__(self, connection, executor):
        super(Search, self).__init__(connection, executor)
        self._view_prefix = "/search/view"
        self._analyzer_prefix = "/search/analyzer"

    def set_search(self, collection, enable, field):
        """Set search capability of a collection (enabling or disabling it).
        If the collection does not exist, it will be created.
        :param collection: Collection name on which search capabilities has
        to be enabled/disabled
        :type collection: str | unicode
        :param enable: Whether to enable / disable search capabilities
        :type enable: string ("true" or "false")
        :param field: For which field to enable search capability.
        :type field: str | unicode
        :return: True if set operation is successfull
        :rtype: bool
        """

        request = Request(
            method="POST",
            endpoint="/search/collection/{}".format(collection),
            params={"enable": enable, "field": field},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise SearchCollectionSetError(resp, request)
            if resp.status_code == 400:
                raise SearchCollectionInvalidArgument(resp, request)
            if resp.status_code == 403:
                raise SearchCollectionForbiddenError(resp, request)
            return True

        return self._execute(request, response_handler)

    def search_in_collection(self, collection, search, bindVars=None, ttl=60):
        """Search a collection for string matches.

        :param collection: Collection name on which search has to be performed
        :type collection: str | unicode
        :param search: search string needs to be search in given collection
        :type search: str | unicode
        :param bindVars: if there is c8ql in search text, we can pass bindVars for
        c8ql query using bindVars param
        :type bindVars: dict | None
        :param ttl: default ttl will be 60 seconds
        :type ttl: int
        :return: The specified search query will be executed for the collection.
        The results of the search will be in the response. If there are too
        many results, an "id" will be specified for the cursor that can be
        used to obtain the remaining results.
        :rtype: [dict]
        """
        # create request object
        request = Request(
            method="POST",
            endpoint="/search",
            data={
                "collection": collection,
                "search": search,
                "bindVars": bindVars,
                "ttl": ttl,
            },
        )
        # create response handler

        def response_handler(resp):
            if not resp.is_success:
                raise SearchError(resp, request)
            if resp.status_code == 400:
                raise SearchInvalidArgumentError(resp, request)
            if resp.status_code == 403:
                raise SearchForbiddenError(resp, request)
            if resp.status_code == 404:
                raise SearchNotExistError(resp, request)
            return resp.body

        # execute request
        return self._execute(request, response_handler)

    def list_all_views(self):
        """List all views

        :return: Returns an object containing an array of all view descriptions.
        :rtype: [dict]
        """
        # create request
        request = Request(method="GET", endpoint=self._view_prefix)
        # response handler

        def response_handler(resp):
            if not resp.is_success:
                raise ViewGetError(resp, request)
            return resp.body["result"]

        # execute request
        return self._execute(request, response_handler)

    def create_view(self, name, links={}, primary_sort=[]):
        """Creates a new view with a given name and properties if it does not
        already exist.

        :param name: The name of the view
        :type name: str | unicode
        :param links: Link properties related with the view
        :type links: dict
        :param primary_sort: Array of object containg the fields on which
        sorting needs to be done and whether the sort is asc or desc
        :type primary_sort: [dict]
        :return: object of new view
        :rtype: dict
        """
        # create request
        request = Request(
            method="POST",
            endpoint=self._view_prefix,
            data={
                "name": name,
                "links": links,
                "primarySort": primary_sort,
                "type": "search",
            },
        )
        # response handler

        def response_handler(resp):
            if not resp.is_success:
                raise ViewCreateError(resp, request)
            if resp.status_code == 400:
                raise ViewCreateViewNameMissingError(resp, request)
            if resp.status_code == 404:
                raise ViewCreateViewNameUnknownError(resp, request)
            return resp.body

        # execute request
        return self._execute(request, response_handler)

    def get_view_info(self, view):
        """Returns information about view

        :param view: name of the view
        :type view: str | unicode
        :return returns information about view
        :rtype: dict
        """
        # create request
        request = Request(method="GET", endpoint=self._view_prefix + "/{}".format(view))
        # create response handler

        def response_handler(resp):
            if not resp.is_success:
                raise ViewGetError(resp, request)
            if resp.status_code == 404:
                raise ViewNotFoundError(resp, request)
            return resp.body

        # execute request
        return self._execute(request, response_handler)

    def rename_view(self, old_name, new_name):
        """Rename given view to new name

        :param old_name: Old view name
        :type old_name: str | unicode
        :param new_name: New view name
        :type new_name: str | unicode
        :return: True if view name renamed
        :rtype: bool
        """
        # create request
        request = Request(
            method="PUT",
            endpoint=self._view_prefix + "/{}/rename".format(old_name),
            data={"name": new_name},
        )
        # create response handler

        def response_handler(resp):
            if not resp.is_success:
                raise ViewRenameError(resp, request)
            if resp.status_code == 404:
                raise ViewCreateViewNameMissingError(resp, request)
            if resp.status_code == 400:
                raise ViewCreateViewNameUnknownError(resp, request)
            return True

        # execute request
        return self._execute(request, response_handler)

    def delete_view(self, view):
        """Deletes given view

        :param view: Name of the view to be deleted
        :type view: str | unicode
        :return: True if view deleted successfully
        :rtype: bool
        """
        # create request
        request = Request(
            method="DELETE", endpoint=self._view_prefix + "/{}".format(view)
        )
        # create response handler

        def response_handler(resp):
            if not resp.is_success:
                raise ViewDeleteError(resp, request)
            if resp.status_code == 404:
                raise ViewCreateViewNameMissingError(resp, request)
            if resp.status_code == 400:
                raise ViewCreateViewNameUnknownError(resp, request)
            return True

        # execute request
        return self._execute(request, response_handler)

    def get_view_properties(self, view):
        """Get view properties

        :param view: View name whos properties we need to get.
        :type view: str | unicode
        :return: returns properties of given view
        :rtype: dict
        """
        # create request
        request = Request(
            method="GET", endpoint=self._view_prefix + "/{}/properties".format(view)
        )
        # create response handler

        def response_handler(resp):
            if not resp.is_success:
                raise ViewGetPropertiesError(resp, request)
            if resp.status_code == 404:
                raise ViewCreateViewNameMissingError(resp, request)
            if resp.status_code == 400:
                raise ViewCreateViewNameUnknownError(resp, request)
            return resp.body

        return self._execute(request, response_handler)

    def update_view_properties(self, view, properties):
        """Updates properties of given view

        :param view: Name of the view
        :type view: str | unicode
        :param properties: Properties to be updated in given view
        :type properties: dict
        :return: True if properties updated successfully
        :rtype: bool
        """
        # create request
        request = Request(
            method="PUT",
            endpoint=self._view_prefix + "/{}/properties".format(view),
            data=properties,
        )
        # create response handler

        def response_handler(resp):
            if not resp.is_success:
                raise ViewUpdatePropertiesError(resp, request)
            if resp.status_code == 404:
                raise ViewCreateViewNameMissingError(resp, request)
            if resp.status_code == 400:
                raise ViewCreateViewNameUnknownError(resp, request)
            return True

        return self._execute(request, response_handler)

    def get_list_of_analyzer(self):
        """Get list of all available analyzers

        :returns: Returns list of all available analyzers
        :rtype: [dict]
        """
        # create request
        request = Request(method="GET", endpoint=self._analyzer_prefix)
        # create response handler

        def response_handler(resp):
            if not resp.is_success:
                raise AnalyzerListError(resp, request)
            return resp.body["result"]

        return self._execute(request, response_handler)

    def get_analyzer_definition(self, name):
        """Gets given analyzer definition

        :param name: Name of the view
        :type name: str | unicode
        :return: Definition of the given analyzer
        :rtype: dict
        """
        # create request
        request = Request(
            method="GET", endpoint=self._analyzer_prefix + "/{}".format(name)
        )
        # create response handler

        def response_handler(resp):
            if not resp.is_success:
                raise AnalyzerGetDefinitionError(resp, request)
            if resp.status_code == 404:
                raise AnalyzerNotFoundError(resp, request)
            return resp.body

        # execute request
        return self._execute(request, response_handler)
