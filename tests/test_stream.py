from __future__ import absolute_import, unicode_literals

import base64
import json
import threading


from c8.exceptions import (
    StreamConnectionError,
    StreamCreateError,
    StreamListError,
    StreamProducerError,
    StreamSubscriberError,
)
from tests.helpers import assert_raises, generate_stream_name


def test_stream_methods(tst_fabric):
    # Test stream apis
    stream_name_1 = generate_stream_name()
    stream_name_2 = generate_stream_name()
    tst_fabric.create_stream(stream_name_1)
    tst_fabric.create_stream(stream_name_2, True)

    stream_1 = "c8globals." + stream_name_1
    stream_2 = "c8locals." + stream_name_2

    assert tst_fabric.has_stream(stream_name_1) is True
    stream = tst_fabric.stream()

    assert stream.set_message_expiry_stream(stream_2, 3600) is True

    # Create subscriber
    subscriber = stream.subscribe(stream_name_1, local=False, subscription_name="topic_1")
    # Producer publishing message
    producer = stream.create_producer(stream_name_1, local=False)
    msg = "Hello from  user"
    producer.send(msg)

    # Subscriber reading message
    m1 = json.loads(subscriber.recv())
    msg1 = base64.b64decode(m1["payload"]).decode('utf-8')
    msg = "Hello from  user"
    assert msg1 == msg
    subscriber.send(json.dumps({'messageId': m1['messageId']}))#Acknowledge the received msg.

    producer.close()
    subscriber.close()

    stream.get_stream_backlog(stream_name_2, local=True)
    assert stream.clear_streams_backlog() == 'OK'
    assert stream.clear_stream_backlog("topic_1") == 'OK'
    stream.get_stream_stats(stream_name_1)

    reader = stream.create_reader(stream_name_2, "latest", local=True)
    subscriber_2 = stream.subscribe(stream_name_1, subscription_name="topic_2")

    stream.set_message_stream_ttl(1000)
    assert stream.get_message_stream_ttl() == 1000

    resp = stream.get_stream_subscriptions(stream_name_1)
    assert resp == ['topic_1', 'topic_2']

    assert stream.publish_message_stream(stream_1, "Hello World") is True
    m1 = json.loads(subscriber_2.recv())
    msg1 = base64.b64decode(m1["payload"]).decode("utf-8")
    assert msg1 == "Hello World"

    subscriber_2.send(json.dumps({'messageId': m1['messageId']}))#Acknowledge the received msg.
    subscriber_2.close()
    reader.close()
 
    assert stream.unsubscribe(subscription="topic_2") == 'OK'
    assert stream.delete_stream_subscription(stream_1, "topic_1") == 'OK'


    assert tst_fabric.delete_stream(stream_1) is True
    assert tst_fabric.delete_stream(stream_2) is True


def test_stream_exceptions(client, bad_fabric_name, tst_fabric, tst_fabric_name):
    # Test stream exceptions
    stream_name_1 = generate_stream_name()
    with assert_raises(StreamCreateError) as err:
        tst_fabric.create_stream(stream_name_1 + "%")
    assert err.value.http_code == 400

    bad_fabric = client._tenant.useFabric(bad_fabric_name)
    with assert_raises(StreamListError) as err:
        bad_fabric.has_stream(stream_name_1)
    assert err.value.http_code == 404

    tst_fabric = client._tenant.useFabric(tst_fabric_name)
    stream = tst_fabric.stream()

    with assert_raises(StreamProducerError) as err:
        stream.create_producer(stream_name_1)

    with assert_raises(StreamSubscriberError) as err:
        stream.create_reader(stream_name_1, "latest")

    with assert_raises(StreamConnectionError) as err:
        stream.set_message_expiry_stream(stream_name_1, 3600)
    assert err.value.http_code == 404

    with assert_raises(StreamSubscriberError) as err:
        stream.subscribe(stream_name_1, subscription_name="topic_1")

    with assert_raises(StreamConnectionError) as err:
        stream.publish_message_stream(stream_name_1, "Hello World")
    assert err.value.http_code == 404

    with assert_raises(StreamConnectionError) as err:
        stream.get_stream_subscriptions(stream_name_1)
    assert err.value.http_code == 404

    with assert_raises(StreamConnectionError) as err:
        stream.get_stream_backlog(stream_name_1)
    assert err.value.http_code == 404

    with assert_raises(StreamConnectionError) as err:
        stream.get_stream_stats(stream_name_1)
    assert err.value.http_code == 404

    with assert_raises(StreamConnectionError) as err:
        stream.delete_stream_subscription(stream_name_1, "topic_1")
    assert err.value.http_code == 404

    with assert_raises(StreamConnectionError) as err:
        tst_fabric.delete_stream(stream_name_1)
    assert err.value.http_code == 404
