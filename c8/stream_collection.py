from __future__ import absolute_import, unicode_literals

from urllib.parse import urlparse, urlencode

import json
import random

import websocket

from c8.api import APIWrapper
from c8 import exceptions as ex
from c8 import constants
from c8.request import Request

__all__ = ['StreamCollection']
ENDPOINT = "/streams/"

class StreamCollection(APIWrapper):
    """Stream Client.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    types = {4: 'persistent'}

    def enum(**enums):
        return type('Enum', (), enums)

    def close(self):
        """
            Close the client and all the associated producers and consumers
        """
    
    CONSUMER_TYPES = enum(EXCLUSIVE="Exclusive",
                          SHARED="Shared",
                          FAILOVER="Failover")

    COMPRESSION_TYPES = enum(LZ4="LZ4",
                             ZLIB="ZLib",
                             NONE=None)

    ROUTING_MODE = enum(
        SINGLE_PARTITION="SinglePartition",
        ROUND_ROBIN_PARTITION="RoundRobinPartition",  # noqa
        CUSTOM_PARTITION="CustomPartition"
    )

    def __init__(self, fabric, connection, executor, url, port,
                 operation_timeout_seconds):
        """Create a new Stream client instance.
        """
        super(StreamCollection, self).__init__(connection, executor)
        url = urlparse(url)
        self.header = connection.headers
        self.fabric = fabric
        dcl_local = self.fabric.localdc(detail=True)
        ws_url = "wss://api-%s/_ws/ws/v2/"
        self._ws_url = ws_url % (dcl_local["tags"]["url"])
           
    def create_producer(self, stream, isCollectionStream=False, local=False, producer_name=None,
                        initial_sequence_id=None, send_timeout_millis=30000,
                        compression_type=COMPRESSION_TYPES.NONE,
                        max_pending_messages=1000,
                        batching_enabled=False,
                        batching_max_messages=1000,
                        batching_max_allowed_size_in_bytes=131072,
                        batching_max_publish_delay_ms=10,
                        message_routing_mode=ROUTING_MODE.ROUND_ROBIN_PARTITION
                        ):
        """Create a new producer on a given stream.
        **Args**
        * `stream`: The stream name
        **Options**
        * `persistent`: If the stream_stream is persistent or non-persistent
                        default its persitent
        * `local`: If the stream_stream is local or global default its global
        * `producer_name`: Specify a name for the producer. If not assigned,
                           the system will generate a globally unique name
                           which can be accessed with
                           `Producer.producer_name()`. When specifying a name,
                           it is app to the user to ensure that, for a given
                           topic, the producer name is unique across all
                           Pulsar's clusters.
        * `initial_sequence_id`: Set the baseline for the sequence ids for
                                 messages published by the producer. First
                                 message will be using
                                 `(initialSequenceId + 1)`` as its sequence id
                                 and subsequent messages will be assigned
                                 incremental sequence ids, if not otherwise
                                 specified.
        * `send_timeout_seconds`: If a message is not acknowledged by the
                                  server before the `send_timeout` expires, an
                                  error will be reported.
        * `compression_type`: Set the compression type for the producer. By
                              default, message payloads are not compressed.
                              Supported compression types are
                              `CompressionType.LZ4` and `CompressionType.ZLib`.
        * `max_pending_messages`: Set the max size of the queue holding the
                                  messages pending to receive
                                  an acknowledgment from the broker.
        * `block_if_queue_full`: Set whether `send_async` operations should
                                 block when the outgoing message queue is full.
        * `message_routing_mode`: Set the message routing mode for the
                                  partitioned producer. Default is
                                  `PartitionsRoutingMode.RoundRobinDistribution`,  # noqa
                                  other option is
                                  `PartitionsRoutingMode.UseSinglePartition`
        """
        if isCollectionStream is False:
            if local is True:
                type_constant = constants.STREAM_LOCAL_NS_PREFIX
            elif local is False:
                type_constant = constants.STREAM_GLOBAL_NS_PREFIX

            stream = type_constant.replace(".", "")+"s."+stream
        elif isCollectionStream is False:
            stream = stream
        print("Calling has steram from create_producer: ", stream, local)
        flag = self.fabric.has_stream(stream, local=local, isCollectionStream=isCollectionStream)
        if flag:
            namespace = type_constant + self.fabric_name
            topic = "producer/persistent/%s/%s/%s" % (self.tenant_name, namespace,
                                               stream)
            params =  {
                "producerName":producer_name,
                "initialSequenceId":initial_sequence_id,
                "sendTimeoutMillis":send_timeout_millis,
                "compressionType":compression_type,
                "maxPendingMessages":max_pending_messages,
                "batchingEnabled":batching_enabled,
                "batchingMaxMessages":batching_max_messages,
                "batchingMaxPublishDelay":batching_max_publish_delay_ms,
                "messageRoutingMode":message_routing_mode
            }

            params = {k: v for k, v in params.items() if v is not None}
            url = self._ws_url + topic 
            print(url)
            return websocket.create_connection(url, header={'Authorization' : self.header['Authorization']})
        
        raise ex.StreamProducerError(
            "No stream present with name:" + stream +
            ". Please create a stream and then stream producer"
        )

    def create_reader(self, stream, start_message_id,
                      local=False, isCollectionStream=False,
                      reader_listener=None,
                      receiver_queue_size=1000,
                      reader_name=None,
                      subscription_role_prefix=None,
                      ):
        """
        Create a reader on a particular topic
        **Args**
        * `stream`: The name of the stream.
        * `start_message_id`: The initial reader positioning is done by
                              specifying a message id.
        **Options**
        * `local`: If the stream_stream is local or global default its global
        * `reader_listener`:
            Sets a message listener for the reader. When the listener is set,
            the application will receive messages through it. Calls to
            `reader.read_next()` will not be allowed. The listener function
            needs to accept (reader, message), for example:
        * `receiver_queue_size`:
            Sets the size of the reader receive queue. The reader receive
            queue controls how many messages can be accumulated by the reader
            before the application calls `read_next()`. Using a higher value
            could potentially increase the reader throughput at the expense of
            higher memory utilization.
        * `reader_name`: Sets the reader name.
        * `subscription_role_prefix`: Sets the subscription role prefix.
        """
        if isCollectionStream is False:
            if local is True:
                type_constant = constants.STREAM_LOCAL_NS_PREFIX
            elif local is False:
                type_constant = constants.STREAM_GLOBAL_NS_PREFIX

            stream = type_constant.replace(".", "")+"s."+stream
        flag = self.fabric.has_stream(stream, local=local, isCollectionStream=isCollectionStream)
        if flag:
            namespace = type_constant + self.fabric_name

            topic = "reader/persistent/%s/%s/%s" % (self.tenant_name, namespace,
                                               stream)

            params = {
                "readerName": reader_name,
                "receiverQueueSize": receiver_queue_size,
                "messageId": start_message_id
            }

            params = {k: v for k, v in params.items() if v is not None}
            url = self._ws_url + topic + "?" + urlencode(params)
            
            return websocket.create_connection(url, header={'Authorization' : self.header['Authorization']})

        raise ex.StreamSubscriberError(
            "No stream present with name:" + stream +
            ". Please create a stream and then stream reader."
        )

    def subscribe(self, stream, isCollectionStream=False, local=False,
                  subscription_name=None,
                  consumer_type=CONSUMER_TYPES.EXCLUSIVE,
                  message_listener=None,
                  receiver_queue_size=1000,
                  consumer_name=None,
                  unacked_messages_timeout_ms=None,
                  broker_consumer_stats_cache_time_ms=30000,
                  is_read_compacted=False,
                  ):
        """
        Subscribe to the given topic and subscription combination.
        **Args**
        * `stream`: The name of the stream.
        * `subscription`: The name of the subscription.
        **Options**
        * `local`: If the stream_stream is local or global default its global
        * `consumer_type`: Select the subscription type to be used when
                           subscribing to the topic.
        * `message_listener`: Sets a message listener for the consumer. When
                              the listener is set, the application will receive
                              messages through it. Calls to
                              `consumer.receive()` will not be allowed. The
                              listener function needs to accept
                              (consumer, message)
        * `receiver_queue_size`:
            Sets the size of the consumer receive queue. The consumer receive
            queue controls how many messages can be accumulated by the consumer
            before the application calls `receive()`. Using a higher value
            could potentially increase the consumer throughput at the expense
            of higher memory utilization. Setting the consumer queue size to
            zero decreases the throughput of the consumer by disabling
            pre-fetching of messages. This approach improves the message
            distribution on shared subscription by pushing messages only to
            those consumers that are ready to process them. Neither receive
            with timeout nor partitioned topics can be used if the consumer
            queue size is zero. The `receive()` function call should not be
            interrupted when the consumer queue size is zero. The default value
            is 1000 messages and should work well for most use cases.
        * `consumer_name`: Sets the consumer name.
        * `unacked_messages_timeout_ms`:
            Sets the timeout in milliseconds for unacknowledged messages.
            The timeout needs to be greater than 10 seconds. An exception is
            thrown if the given value is less than 10 seconds. If a successful
            acknowledgement is not sent within the timeout, all the
            unacknowledged messages are redelivered.
        * `broker_consumer_stats_cache_time_ms`:
            Sets the time duration for which the broker-side consumer stats
            will be cached in the client.
        """
        if local is True:
            type_constant = constants.STREAM_LOCAL_NS_PREFIX
        elif local is False:
            type_constant = constants.STREAM_GLOBAL_NS_PREFIX
  
        if isCollectionStream is False:
            stream = type_constant.replace(".", "")+"s."+stream

        if isCollectionStream is True:
            stream = stream
        flag = self.fabric.has_stream(stream, local=local, isCollectionStream=isCollectionStream)
        if flag:

            namespace = type_constant + self.fabric_name

            if not subscription_name:
                subscription_name = "%s-%s-subscription-%s" % (
                    self.tenant_name, self.fabric_name,
                    str(random.randint(1, 1000)))


            topic = "consumer/persistent/%s/%s/%s/%s" % (self.tenant_name,
                                               namespace,
                                               stream, 
                                               subscription_name)


            params = {
                "subscriptionType": consumer_type,
                "receiverQueueSize": receiver_queue_size,
                "consumerName": consumer_name,
            }

            params = {k: v for k, v in params.items() if v is not None}
            url = self._ws_url + topic + "?" + urlencode(params)
            return websocket.create_connection(url, header={'Authorization' : self.header['Authorization']})

        raise ex.StreamSubscriberError(
            "No stream present with name:" + stream +
            ". Please create a stream and then stream subscriber."
        )
    
    def unsubscribe(self, subscription):
        """Unsubscribes the given subscription on all streams on a stream fabric
        :param subscription
        :return: 200, OK if operation successful
        raise c8.exceptions.StreamPermissionError: If unsubscribing fails.
        """
        request = Request(
            method='delete',
            endpoint='/streams/subcription/{}'.format(subscription)
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)
    
    def clear_streams_backlog(self):
        """Clear backlog for all streams on a stream fabric
        :return: 200, OK if operation successful
        :raise c8.exceptions.StreamPermissionError: If clearing backlogs for
                                                    all streams fails.
        """

        request = Request(
            method='post',
            endpoint='/streams/clearbacklog'
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def clear_stream_backlog(self, subscription):
        """Clear backlog for the given stream on a stream fabric
        :param: name of subscription
        :return: 200, OK if operation successful
        :raise c8.exceptions.StreamPermissionError: If clearing backlogs for
                                                    all streams fails.
        """

        request = Request(
            method='post',
            endpoint='/streams/clearbacklog/{}'.format(subscription)
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def get_stream_subscriptions(self, stream, local=False, isCollectionStream=False):
        """Get the list of persistent subscriptions for a given stream.

        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one.
        :return: List of stream subscription, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: If getting subscriptions
                                                     for a stream fails.
        """
        type_constant = constants.STREAM_GLOBAL_NS_PREFIX
        if local:

            type_constant = constants.STREAM_LOCAL_NS_PREFIX
        
        if isCollectionStream is False:

            stream = type_constant.replace(".", "")+"s."+stream

        if local is True:
            endpoint = '/streams/{}/subscriptions?global=false'.format(stream)
        
        elif local is False:
            endpoint = '/streams/{}/subscriptions?global=true'.format(stream)

        request = Request(method='get', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return json.loads(resp.body['result'])
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)


    def get_stream_backlog(self, stream, local=False, isCollectionStream=False):
        """Get estimated backlog for offline stream.

        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: If getting subscriptions
                                                     for a stream fails.
        """
        #endpoint = '{}/{}/backlog?local={}'.format(ENDPOINT, stream, local)
        type_constant = constants.STREAM_GLOBAL_NS_PREFIX
        if local:

            type_constant = constants.STREAM_LOCAL_NS_PREFIX
        
        if isCollectionStream is False:

            stream = type_constant.replace(".", "")+"s."+stream
        
        if local is False:
            endpoint = '/streams/{}/backlog?global=true'.format(stream)
        elif local is True:
            endpoint = '/streams/{}/backlog?global=false'.format(stream)
        request = Request(method='get', endpoint=endpoint)
        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return json.loads(resp.body['result'])
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def get_stream_stats(self, stream, isCollectionStream=False, local=False):
        """Get the stats for the given stream

        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: If getting subscriptions
                                                     for a stream fails.
        """
        if isCollectionStream is False:
            if local is True:
                stream = "c8locals." + stream
            else:
                stream = "c8globals." + stream
        if local is True:
            endpoint = '{}/{}/stats?global=False'.format(ENDPOINT, stream)
        elif local is False:
            endpoint = '{}/{}/stats?global=True'.format(ENDPOINT, stream)

        request = Request(method='get', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return json.loads(resp.body['result'])
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)
    
    # api not in latest swagger
    '''
    def reset_message_subscription(self, stream, subscription, message_id,
                                   isCollectionStream=False, local=False):
        """Reset subscription to message position closest to given position.

        :param stream: name of stream
        :param subscription: name of subscription
        :param message_id: message object consisting of
            { ledgerId	integer, entryId	integer, partitionIndex	integer }
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamUpdateError: If Subscription has active
                                                 consumers
        """
        if isCollectionStream is False:
            if local is True:
                stream = "c8locals." + stream
            else:
                stream = "c8globals." + stream
        endpoint = '{}/{}/subscription/{}?local={}'.format(ENDPOINT, stream,
                                                           subscription, local)
        request = Request(method='put', endpoint=endpoint, data=message_id)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            elif code == 412:
                raise ex.StreamUpdateError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)
    '''

    def delete_stream_subscription(self, stream, subscription, local=False):
        """Delete a subscription.

        :param stream: name of stream
        :param subscription: name of subscription
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If Subscription has active
                                                 consumers
        """
        #endpoint = '{}/{}/subscription/{}?local={}'.format(ENDPOINT, stream,
        #                                                   subscription, local)
        if local is False:
            endpoint='/streams/{}/subscriptions/{}?global=true'.format(stream, subscription)
        elif local is True:
            endpoint='/streams/{}/subscriptions/{}?global=false'.format(stream, subscription)

        request = Request(method='delete', endpoint=endpoint)
        def response_handler(resp):
            print(resp.body)
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            elif code == 412:
                raise ex.StreamDeleteError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    # api not in latest swagger
    '''
    def skip_all_messages_for_subscription(self, stream, subscription,
                                           local=False, isCollectionStream=False):
        """Skip all messages on a stream subscription

        :param stream: name of stream
        :param subscription: name of subscription
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError:Don't have permission
        """

        type_constant = constants.STREAM_GLOBAL_NS_PREFIX
        if local:

            type_constant = constants.STREAM_LOCAL_NS_PREFIX
        
        if isCollectionStream is False:

            stream = type_constant.replace(".", "")+"s."+stream

        endpoint = '{}/{}/subscription/{}/skip_all?local={}'.format(
            ENDPOINT, stream, subscription, local)
        
        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)


    def skip_messages_for_subscription(self, stream, subscription,
                                       num_of_messages, local=False, isCollectionStream=False):
        """Skip num messages on a topic subscription

        :param stream: Name of stream
        :param subscription: Name of subscription
        :param num_of_messages: Number of messages
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError:Don't have permission
        """
        type_constant = constants.STREAM_GLOBAL_NS_PREFIX
        if local:

            type_constant = constants.STREAM_LOCAL_NS_PREFIX
        
        if isCollectionStream is False:

            stream = type_constant.replace(".", "")+"s."+stream

        endpoint = '{}/{}/subscription/{}/skip/{}?local={}'.format(
            ENDPOINT, stream, subscription, num_of_messages, local)
        
        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)
    '''

    # api not in latest swagger
    '''
    def expire_messages_for_all_subscription(self, stream, expire_time,
                                             isCollectionStream=False, local=False):
        """Expire messages on a stream subscription

        :param stream:
        :param subscription:
        :param expire_time:
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError:Don't have permission
        """
        if isCollectionStream is False:
            if local is True:
                stream = "c8locals." + stream
            else:
                stream = "c8globals." + stream
        endpoint = '{}/{}/all_subscription/expireMessages/{}?local={}'.format(
            ENDPOINT, stream, expire_time, local)
        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def expire_messages_for_subscription(self, stream, subscription,
                                         expire_time, isCollectionStream=False,
                                         local=False):

        if isCollectionStream is False:
            if local is True:
                stream = "c8locals." + stream
            else:
                stream = "c8globals." + stream
        endpoint = '{}/{}/subscription/{}/expireMessages/{}?local={}'.format(
            ENDPOINT, stream, subscription, expire_time, local)
        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 400:
                raise ex.StreamBadInputError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def expire_messages_for_subscriptions(self, stream, expire_time,
                                          isCollectionStream=False, local=False):
        """Expire messages on all subscriptions of stream

        :param stream:
        :param subscription:
        :param expire_time:
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError:Don't have permission
        """
        if isCollectionStream is False:
            if local is True:
                stream = "c8locals." + stream
            else:
                stream = "c8globals." + stream
        endpoint = '{}/{}/all_subscription/expireMessages/{}?local={}'.format(
            ENDPOINT, stream, expire_time, local)
        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)
    '''
    
    def reset_message_subscription_by_timestamp(self, stream, subscription,
                                                timestamp, isCollectionStream=False, 
                                                local=False):
        """Reset subscription to message position closest to absolute timestamp

        :param stream:
        :param subscription:
        :param timestamp:
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError:Don't have permission
        """
        if isCollectionStream is False:
            if local is True:
                stream = "c8locals." + stream
            else:
                stream = "c8globals." + stream
        
        endpoint = '{}/{}/subscription/{}/resetcursor/{}'.format(
            ENDPOINT, stream, subscription, timestamp)
        print("----", endpoint)
        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            print(code)
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)


    def reset_message_for_subscription(self, stream, subscription,
                                       isCollectionStream=False, local=False):
        """Reset subscription to message position closest to given position

        :param stream: Name of stream
        :param subscription: Name of subscription
        :param timestamp: Timestamp
        :param local: Operate on a local stream instead of a global one.
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If Subscription has active
                                                 consumers
        """
        if isCollectionStream is False:
            if local is True:
                stream = "c8locals." + stream
            else:
                stream = "c8globals." + stream
        endpoint = '{}/{}/subscription/{}/resetcursor?local={}'.format(
            ENDPOINT, stream, subscription, local)
        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)


    def reset_message_subscription_by_position(self, stream, subscription,
                                               message_position, isCollectionStream=False, 
                                               local=False):
        """It fence cursor and disconnects all active consumers before reseting
        cursor.

        :param stream: Name of stream
        :param subscription: Name of subscription
        :param message_position: Integer
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If Subscription has active
                                                 consumers
        """
        if isCollectionStream is False:
            if local is True:
                stream = "c8locals." + stream
            else:
                stream = "c8globals." + stream
        endpoint = '{}/{}/subscription/{}/position/{}'.format(
            ENDPOINT, stream, subscription, message_position)
        request = Request(method='post', endpoint=endpoint)

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            elif code == 400:
                raise ex.StreamBadInputError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)