import os

from dotenv import load_dotenv

load_dotenv()


def test_list_function_workers(get_client_instance):
    response = get_client_instance.function.list_function_workers()
    # Response from platform
    assert {"code": 200, "error": False, "result": []} == response


def test_deploy_query_worker_to_edge_worker(get_client_instance):
    response = get_client_instance.function.deploy_query_worker_to_edge_worker(
        "test_edge_worker_query_worker", "query_worker"
    )
    # Response from platform
    assert 202 == response["code"]
    assert response["error"] is False


def test_deploy_stream_publisher_to_edge_worker(get_client_instance):
    response = get_client_instance.function.deploy_stream_publisher_to_edge_worker(
        "test_edge_worker_stream_publisher", "test_stream_worker", "test_stream"
    )
    # Response from platform
    assert 202 == response["code"]
    assert response["error"] is False


def test_deploy_stream_adhoc_query_to_edge_worker(get_client_instance):
    response = get_client_instance.function.deploy_stream_adhoc_query_to_edge_worker(
        "test_edge_worker_adhoc_query",
        "test_stream_worker_adhoc",
    )
    # Response from platform
    assert 202 == response["code"]
    assert response["error"] is False


def test_get_function_worker_info(get_client_instance):
    response = get_client_instance.function.get_function_worker_info(
        "test_edge_worker_query_worker"
    )
    # Response from platform
    assert 202 == response["code"]
    assert response["error"] is False


def test_remove_function_worker(get_client_instance):
    response = get_client_instance.function.remove_function_worker(
        "test_edge_worker_query_worker"
    )
    # Response from platform
    assert "" == response


def test_invoke_function_worker(get_client_instance):
    response = get_client_instance.function.invoke_function_worker(
        "test_invoke", {"offset": 0, "limit": 10}
    )
    # Response from platform
    assert "" == response


def test_get_edge_worker_metadata(get_client_instance):
    response = get_client_instance.function.get_edge_worker_metadata()
    # Response from platform
    assert {"code": 200, "error": False, "result": []} == response


def test_create_edge_worker_metadata(get_client_instance):
    response = get_client_instance.function.create_edge_worker_metadata(
        "akamai",
        os.environ.get("ACCESS_TOKEN"),
        os.environ.get("BASE_URL"),
        os.environ.get("CLIENT_SECRET"),
        os.environ.get("CLIENT_TOKEN"),
        "200",
        os.environ.get("GROUP_ID"),
        "macrometa-akamai-ew.macrometa.io",
    )
    # Response from platform
    assert 201 == response["code"]
    assert response["error"] is False


def test_modify_edge_worker_metadata(get_client_instance):
    response = get_client_instance.function.modify_edge_worker_metadata(
        "akamai",
        os.environ.get("ACCESS_TOKEN"),
        os.environ.get("BASE_URL"),
        os.environ.get("CLIENT_SECRET"),
        os.environ.get("CLIENT_TOKEN"),
        "200",
        os.environ.get("GROUP_ID"),
        "macrometa-akamai-ew.macrometa.io",
    )
    # Response from platform
    assert 200 == response["code"]
    assert response["error"] is False


def test_delete_edge_worker_metadata(get_client_instance):
    response = get_client_instance.function.delete_edge_woker_metadata()
    # Response from platform
    assert "" == response
