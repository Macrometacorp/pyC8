from __future__ import absolute_import, unicode_literals

__all__ = ['StreamCollection']

from c8 import constants
from urllib.parse import urlparse
from c8.api import APIWrapper
from c8.request import Request
import pulsar
import random
from enum import Enum
from c8 import exceptions as ex
import json

class StreamCollection(APIWrapper):
    """Stream Client.

    :param connection: HTTP connection.
    :type connection: c8.connection.Connection
    :param executor: API executor.
    :type executor: c8.executor.Executor
    """

    types = {
        4: 'persistent'
    }

    def enum(**enums):
        return type('Enum', (), enums)

    CONSUMER_TYPES = enum(EXCLUSIVE=pulsar.ConsumerType.Exclusive, SHARED=pulsar.ConsumerType.Shared,
                          FAILOVER=pulsar.ConsumerType.Failover)

    def __init__(self, fabric, connection, executor, url, port,
                 operation_timeout_seconds,
                 ):
        """
         Create a new Stream client instance.
         **Args**
         * `service_url`: The Streams service url eg: pulsar://my-broker.com:6650/
         **Options**
         * `authentication`:
           Set the authentication provider to be used with the broker.
         * `operation_timeout_seconds`:
           Set timeout on client operations (subscribe, create producer, close,
           unsubscribe).
         * `io_threads`:
           Set the number of IO threads to be used by the Pulsar client.
         * `message_listener_threads`:
           Set the number of threads to be used by the Pulsar client when
           delivering messages through message listener. The default is 1 thread
           per Pulsar client. If using more than 1 thread, messages for distinct
           `message_listener`s will be delivered in different threads, however a
           single `MessageListener` will always be assigned to the same thread.
         * `concurrent_lookup_requests`:
           Number of concurrent lookup-requests allowed on each broker connection
           to prevent overload on the broker.
         * `log_conf_file_path`:
           Initialize log4cxx from a configuration file.
         * `use_tls`:
           Configure whether to use TLS encryption on the connection. This setting
           is deprecated. TLS will be automatically enabled if the `serviceUrl` is
           set to `pulsar+ssl://` or `https://`
         * `tls_trust_certs_file_path`:
           Set the path to the trusted TLS certificate file.
         * `tls_allow_insecure_connection`:
           Configure whether the Pulsar client accepts untrusted TLS certificates
           from the broker.
         """
        super(StreamCollection, self).__init__(connection, executor)
        url = urlparse(url)
        hostname = url.hostname
        self.fabric = fabric
        dcl_local = self.fabric.dclist_local()
        self.persistent = True
        self._server_url = 'pulsar://' + constants.PLUSAR_URL_PREFIX + dcl_local['tags']['url'] + ":" + str(port)
        self._client = pulsar.Client(self._server_url, operation_timeout_seconds=operation_timeout_seconds)

    def close(self):
        """
            Close the client and all the associated producers and consumers
        """
        self._client.close()

    def create_producer(self, stream, local=False, producer_name=None,
                        initial_sequence_id=None, send_timeout_millis=30000,
                        compression_type=pulsar.CompressionType.NONE,
                        max_pending_messages=1000,
                        block_if_queue_full=False, batching_enabled=False,
                        batching_max_messages=1000, batching_max_allowed_size_in_bytes=131072,
                        batching_max_publish_delay_ms=10,
                        message_routing_mode=pulsar.PartitionsRoutingMode.RoundRobinDistribution
                        ):
        """
           Create a new producer on a given stream.
           **Args**
           * `stream`:
             The stream name
           **Options**
           * `persistent`:
             If the stream_stream is persistent or non-persistent default its persitent
           * `local`:
             If the stream_stream is local or global default its global
           * `producer_name`:
              Specify a name for the producer. If not assigned,
              the system will generate a globally unique name which can be accessed
              with `Producer.producer_name()`. When specifying a name, it is app to
              the user to ensure that, for a given topic, the producer name is unique
              across all Pulsar's clusters.
           * `initial_sequence_id`:
              Set the baseline for the sequence ids for messages
              published by the producer. First message will be using
              `(initialSequenceId + 1)`` as its sequence id and subsequent messages will
              be assigned incremental sequence ids, if not otherwise specified.
           * `send_timeout_seconds`:
             If a message is not acknowledged by the server before the
             `send_timeout` expires, an error will be reported.
           * `compression_type`:
             Set the compression type for the producer. By default, message
             payloads are not compressed. Supported compression types are
             `CompressionType.LZ4` and `CompressionType.ZLib`.
           * `max_pending_messages`:
             Set the max size of the queue holding the messages pending to receive
             an acknowledgment from the broker.
           * `block_if_queue_full`: Set whether `send_async` operations should
             block when the outgoing message queue is full.
           * `message_routing_mode`:
             Set the message routing mode for the partitioned producer. Default is `PartitionsRoutingMode.RoundRobinDistribution`,
             other option is `PartitionsRoutingMode.UseSinglePartition`
        """
        if self.persistent:
            flag = self.fabric.has_persistent_stream(stream, local=local)
        # else:
        #     flag = self.fabric.has_nonpersistent_stream(stream, local=local)

        if flag:
            type_constant = constants.STREAM_GLOBAL_NS_PREFIX
            if local:
                type_constant = constants.STREAM_LOCAL_NS_PREFIX

            namespace = type_constant + self.tenant_name + '.' + self.fabric_name
            if self.tenant_name == "_mm":
                namespace = type_constant + self.fabric_name
            if self.persistent:
                topic = "persistent://" + self.tenant_name + "/" + namespace + "/" + stream
            # else:
            #     topic = "non-persistent://" + self.tenant_name + "/" + namespace + "/" + stream

            return self._client.create_producer(topic, producer_name,
                                                initial_sequence_id, send_timeout_millis,
                                                compression_type,
                                                max_pending_messages,
                                                block_if_queue_full, batching_enabled,
                                                batching_max_messages, batching_max_allowed_size_in_bytes,
                                                batching_max_publish_delay_ms,
                                                message_routing_mode)
        raise ex.StreamProducerError("No stream present with name:" + stream + ". Please create a stream and then stream producer")

    def create_reader(self, stream, start_message_id,
                      local=False,
                      reader_listener=None,
                      receiver_queue_size=1000,
                      reader_name=None,
                      subscription_role_prefix=None,
                      ):
        """
        Create a reader on a particular topic
        **Args**
        * `stream`: The name of the stream.
        * `start_message_id`: The initial reader positioning is done by specifying a message id.
           The options are:
            * `MessageId.earliest`: Start reading from the earliest message available in the topic
            * `MessageId.latest`: Start reading from the end topic, only getting messages published
               after the reader was created
            * `MessageId`: When passing a particular message id, the reader will position itself on
               that specific position. The first message to be read will be the message next to the
               specified messageId. Message id can be serialized into a string and deserialized
               back into a `MessageId` object:
                   # Serialize to string
                   s = msg.message_id().serialize()
                   # Deserialize from string
                   msg_id = MessageId.deserialize(s)
        **Options**
        * `persistent`:
             If the stream_stream is persistent or non-persistent
        * `local`:
            If the stream_stream is local or global default its global
        * `reader_listener`:
          Sets a message listener for the reader. When the listener is set,
          the application will receive messages through it. Calls to
          `reader.read_next()` will not be allowed. The listener function needs
          to accept (reader, message), for example:
                def my_listener(reader, message):
                    # process message
                    pass
        * `receiver_queue_size`:
          Sets the size of the reader receive queue. The reader receive
          queue controls how many messages can be accumulated by the reader
          before the application calls `read_next()`. Using a higher value could
          potentially increase the reader throughput at the expense of higher
          memory utilization.
        * `reader_name`:
          Sets the reader name.
        * `subscription_role_prefix`:
          Sets the subscription role prefix.
        """
        if self.persistent:
            flag = self.fabric.has_persistent_stream(stream, local=local)
        # else:
        #     flag = self.fabric.has_nonpersistent_stream(stream, local=local)

        if flag:
            type_constant = constants.STREAM_GLOBAL_NS_PREFIX
            if local:
                type_constant = constants.STREAM_LOCAL_NS_PREFIX
            
            namespace = type_constant + self.tenant_name + self.fabric_name
            if self.tenant_name == "_mm":
                namespace = type_constant + self.fabric_name

            if self.persistent:
                topic = "persistent://" + self.tenant_name + "/" + namespace + "/" + stream
            # else:
            #     topic = "non-persistent://" + self.tenant_name + namespace + "/" + stream

            return self._client.create_reader(topic, start_message_id,
                                            reader_listener, receiver_queue_size,
                                            reader_name, subscription_role_prefix)
        raise ex.StreamSubscriberError("No stream present with name:" + stream + ". Please create a stream and then stream reader.")

    def subscribe(self, stream, local=False, subscription_name=None,
                  consumer_type= CONSUMER_TYPES.EXCLUSIVE,
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
        * `persistent`:
             If the stream_stream is persistent or non-persistent
        * `local`:
            If the stream_stream is local or global default its global
        * `consumer_type`:
          Select the subscription type to be used when subscribing to the topic.
        * `message_listener`:
          Sets a message listener for the consumer. When the listener is set,
          the application will receive messages through it. Calls to
          `consumer.receive()` will not be allowed. The listener function needs
          to accept (consumer, message), for example:
                #!python
                def my_listener(consumer, message):
                    # process message
                    consumer.acknowledge(message)
        * `receiver_queue_size`:
          Sets the size of the consumer receive queue. The consumer receive
          queue controls how many messages can be accumulated by the consumer
          before the application calls `receive()`. Using a higher value could
          potentially increase the consumer throughput at the expense of higher
          memory utilization. Setting the consumer queue size to zero decreases
          the throughput of the consumer by disabling pre-fetching of messages.
          This approach improves the message distribution on shared subscription
          by pushing messages only to those consumers that are ready to process
          them. Neither receive with timeout nor partitioned topics can be used
          if the consumer queue size is zero. The `receive()` function call
          should not be interrupted when the consumer queue size is zero. The
          default value is 1000 messages and should work well for most use
          cases.
        * `consumer_name`:
          Sets the consumer name.
        * `unacked_messages_timeout_ms`:
          Sets the timeout in milliseconds for unacknowledged messages. The
          timeout needs to be greater than 10 seconds. An exception is thrown if
          the given value is less than 10 seconds. If a successful
          acknowledgement is not sent within the timeout, all the unacknowledged
          messages are redelivered.
        * `broker_consumer_stats_cache_time_ms`:
          Sets the time duration for which the broker-side consumer stats will
          be cached in the client.
        """
        if self.persistent:
            flag = self.fabric.has_persistent_stream(stream, local=local)
        # else:
        #     flag = self.fabric.has_nonpersistent_stream(stream, local=local)

        if flag:
            type_constant = constants.STREAM_GLOBAL_NS_PREFIX
            if local:
                type_constant=constants.STREAM_LOCAL_NS_PREFIX

            namespace = type_constant + self.tenant_name + '.' + self.fabric_name
            if self.tenant_name == "_mm":
                namespace = type_constant + self.fabric_name

            if self.persistent:
                topic = "persistent://" + self.tenant_name + "/" + namespace + "/" + stream
            # else:
            #     topic = "non-persistent://" + self.tenant_name + "/" + namespace + "/" + stream

            if not subscription_name:
                subscription_name = self.tenant_name + "-" + self.fabric_name + "-subscription-" + str(random.randint(1,1000))
            
            # KARTIK : 20181211 : Fix dangling consumer problem as mentioned in streams CPP client test code
            # First open a dummy subscription with the same name, then close it. This will help recover the
            # broker state in case a subscription with the same name was left hanging
            try:
                consumer =  self._client.subscribe(topic, subscription_name, consumer_type,
                                            message_listener, receiver_queue_size, consumer_name,
                                            unacked_messages_timeout_ms, broker_consumer_stats_cache_time_ms,
                                            is_read_compacted)
                consumer.close()
            except Exception:
                pass

            return self._client.subscribe(topic, subscription_name, consumer_type,
                                        message_listener, receiver_queue_size, consumer_name,
                                        unacked_messages_timeout_ms, broker_consumer_stats_cache_time_ms,
                                        is_read_compacted)
        raise ex.StreamSubscriberError("No stream present with name:" + stream + ". Please create a stream and then stream subscriber.")
                                      
    def unsubscribe(self, subscription):
        """Unsubscribes the given subscription on all streams on a stream fabric
        :param subscription
        :return: 200, OK if operation successful
        raise c8.exceptions.StreamPermissionError: If unsubscribing fails.
        """
        request = Request(
            method='post',
            endpoint='/streams/unsubscribe/{}'.format(subscription)
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
        :raise c8.exceptions.StreamPermissionError: If clearing backlogs for all streams fails.
        """
       
        request = Request(
            method='post',
            endpoint= '/streams/clearbacklog'
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
        :raise c8.exceptions.StreamPermissionError: If clearing backlogs for all streams fails.
        """

        request = Request(
            method='post',
            endpoint= '/streams/clearbacklog/{}'.format(subscription)
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def get_stream_subscriptions(self, stream, local=False):
        """
        Get the list of persistent/non-persistent subscriptions for a given stream.
        :param stream: name of stream
        :param local: Operate on a local stream instead of a global one. Default value: false
        :return: List of stream subscription, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: If getting subscriptions for a stream fails.
        """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/subscriptions?local={}'.format(stream,local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/subscriptions?local={}'.format(stream,local)

        request = Request(
            method='get',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return json.loads(resp.body['result'])
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def get_stream_backlog(self, stream, local=False):
        """
       Get estimated backlog for offline stream.
       :param stream: name of stream
       :param local: Operate on a local stream instead of a global one. Default value: false
       :return: 200, OK if operation successful
       :raise: c8.exceptions.StreamPermissionError: If getting subscriptions for a stream fails.
       """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/backlog?local={}'.format(stream, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/backlog?local={}'.format(stream, local)

        request = Request(
            method='get',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return json.loads(resp.body['result'])
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def get_stream_stats(self, stream, local=False):
        """
       Get the stats for the given stream
       :param stream: name of stream
       :param local: Operate on a local stream instead of a global one. Default value: false
       :return: 200, OK if operation successful
       :raise: c8.exceptions.StreamPermissionError: If getting subscriptions for a stream fails.
       """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/stats?local={}'.format(stream, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/stats?local={}'.format(stream, local)

        request = Request(
            method='get',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return json.loads(resp.body['result'])
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def reset_message_subscription(self, stream, subscription, message_id, local=False):
        """
       Reset subscription to message position closest to given position.
       :param stream: name of stream
       :param subscription: name of subscription
       :param message_id: message object consisting of
                            { ledgerId	integer
                            entryId	integer
                            partitionIndex	integer }
       :param local: Operate on a local stream instead of a global one. Default value: false
       :return: 200, OK if operation successful
       :raise: c8.exceptions.StreamUpdateError: If Subscription has active consumers

       """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/subscription/{}?local={}'.format(stream, subscription, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/subscription/{}?local={}'.format(stream, subscription, local)

        request = Request(
            method='put',
            endpoint=url_endpoint,
            data=message_id
        )

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

    def delete_stream_subscription(self, stream, subscription, local=False):
        """
       Delete a subscription.
      :param stream: name of stream
      :param subscription: name of subscription
      :param local: Operate on a local stream instead of a global one. Default value: false
      :return: 200, OK if operation successful
      :raise: c8.exceptions.StreamDeleteError: If Subscription has active consumers

      """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/subscription/{}?local={}'.format(stream, subscription, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/subscription/{}?local={}'.format(stream, subscription, local)

        request = Request(
            method='delete',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            elif code == 412:
                raise ex.StreamDeleteError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def skip_all_messages_for_subscription(self, stream, subscription, local=False):
        """
      Skip all messages on a stream subscription
     :param stream: name of stream
     :param subscription: name of subscription
     :param local: Operate on a local stream instead of a global one. Default value: false
     :return: 200, OK if operation successful
     :raise: c8.exceptions.StreamPermissionError:Don't have permission

     """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/subscription/{}/skip_all?local={}'\
                .format(stream, subscription, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/subscription/{}/skip_all?local={}'\
        #         .format(stream, subscription, local)

        request = Request(
            method='post',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def skip_messages_for_subscription(self, stream, subscription, num_of_messages, local=False):
        """
      Skip num messages on a topic subscription
     :param stream: Name of stream
     :param subscription: Name of subscription
     :param num_of_messages: Number of messages
     :param local: Operate on a local stream instead of a global one. Default value: false
     :return: 200, OK if operation successful
     :raise: c8.exceptions.StreamPermissionError:Don't have permission

     """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/subscription/{}/skip/{}?local={}'\
                .format(stream, subscription, num_of_messages, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/subscription/{}/skip/{}?local={}'\
        #         .format(stream, subscription, num_of_messages, local)

        request = Request(
            method='post',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def expire_messages_for_all_subscription(self, stream, expire_time, local=False):
        """
      Expire messages on a stream subscription
      :param stream:
     :param subscription:
     :param expire_time:
     :param local: Operate on a local stream instead of a global one. Default value: false
     :return: 200, OK if operation successful
     :raise: c8.exceptions.StreamPermissionError:Don't have permission

     """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/all_subscription/expireMessages/{}?local={}'\
                .format(stream, expire_time, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/all_subscription/expireMessages/{}?local={}'\
        #         .format(stream, expire_time, local)

        request = Request(
            method='post',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def expire_messages_for_subscription(self, stream, subscription, expire_time, local=False):
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/subscription/{}/expireMessages/{}?local={}' \
                .format(stream, subscription, expire_time, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/subscription/{}/expireMessages/{}?local={}' \
        #         .format(stream, subscription, expire_time, local)

        request = Request(
            method='post',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 400:
                raise ex.StreamBadInputError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def expire_messages_for_subscriptions(self, stream, expire_time, local=False):
        """
        Expire messages on all subscriptions of stream
      :param stream:
     :param subscription:
     :param expire_time:
     :param local: Operate on a local stream instead of a global one. Default value: false
     :return: 200, OK if operation successful
     :raise: c8.exceptions.StreamPermissionError:Don't have permission

     """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/all_subscription/expireMessages/{}?local={}'\
                .format(stream, expire_time, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/all_subscription/expireMessages/{}?local={}'\
        #         .format(stream, expire_time, local)

        request = Request(
            method='post',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def reset_message_subscription_by_timestamp(self, stream, subscription, timestamp):
        """
        Reset subscription to message position closest to absolute timestamp (in ms)
        :param stream:
        :param subscription:
        :param timestamp:
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError:Don't have permission

        """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/subscription/{}/resetcursor/{}'.format(stream, subscription, timestamp)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/subscription/{}/resetcursor/{}'.format(stream, subscription, timestamp)

        request = Request(
            method='post',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def reset_message_for_subscription(self, stream, subscription, local=False):
        """
        Reset subscription to message position closest to given position
        :param stream: Name of stream
        :param subscription: Name of subscription
        :param timestamp: Timestamp
        :param local: Operate on a local stream instead of a global one. Default value: false
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If Subscription has active consumers

     """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/subscription/{}/resetcursor?local={}'\
                .format(stream, subscription, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/subscription/{}/resetcursor?local={}'\
        #         .format(stream, subscription, local)

        request = Request(
            method='post',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return resp.body['result']
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)

    def reset_message_subscription_by_position(self, stream, subscription, message_position):
        """
        It fence cursor and disconnects all active consumers before reseting cursor.
        :param stream: Name of stream
        :param subscription: Name of subscription
        :param message_position: Integer
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamDeleteError: If Subscription has active consumers

     """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/subscription/{}/position/{}'.format(stream, subscription, message_position)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/subscription/{}/position/{}'.format(stream, subscription, message_position)

        request = Request(
            method='post',
            endpoint=url_endpoint
        )

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
    
    def get_stream_compaction_status(self, stream, local=False):
        """
        Get the status of a compaction operation for a stream
        :param stream: Name of stream
        :param local: Operate on a local stream instead of a global one. Default value: false
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: Dont have permission.

     """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/compaction?local={}'.format(stream, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/compaction?local={}'.format(stream, local)

        request = Request(
            method='get',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return json.loads(resp.body['result'])
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)
    
    def put_stream_compaction_status(self, stream, local=False):
        """
        Trigger a compaction operation on a stream
        :param stream: Name of stream
        :param local: Operate on a local stream instead of a global one. Default value: false
        :return: 200, OK if operation successful
        :raise: c8.exceptions.StreamPermissionError: Dont have permission.

     """
        if self.persistent:
            url_endpoint = '/streams/persistent/stream/{}/compaction?local={}'.format(stream, local)
        # else:
        #     url_endpoint = '/streams/non-persistent/stream/{}/compaction?local={}'.format(stream, local)

        request = Request(
            method='put',
            endpoint=url_endpoint
        )

        def response_handler(resp):
            code = resp.status_code
            if resp.is_success:
                return 'OK'
            elif code == 403:
                raise ex.StreamPermissionError(resp, request)
            raise ex.StreamConnectionError(resp, request)

        return self._execute(request, response_handler)
