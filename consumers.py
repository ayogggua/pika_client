# -*- coding: utf-8 -*-

import logging
import functools

from mill_common.base import PubSubInterface
from mill_common.mixins import CallbackMixin

LOGGER = logging.getLogger(__name__)


class BaseConsumer(CallbackMixin, object):
    def __init__(
            self,
            connector,
            app_id='',
            queue='',
            durable=True):  # make the queue store the message for recovery if ever RabbitMQ goes down.
        super().__init__()
        self._consumer_tag = None
        self._prefetch_count = 1
        self.was_consuming = False
        self._consuming = False

        self.connector = connector
        self.APP_ID = app_id
        self.QUEUE = queue
        self.durable = durable

    def start(self):
        """
        Method to start whatever the interface is designated to do.
        """
        self.setup_queue(self.QUEUE)    

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('Declaring queue %s', queue_name)
        self.connector.channel.queue_declare(
            queue=queue_name,
            durable=self.durable,
            callback=self.on_queue_declareok)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will set QOS so that
        we can control how the messages are consumed.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info("Queue declared. Setting QOS.")
        self.set_qos()

    def set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.
        """
        self.connector.channel.basic_qos(
            prefetch_count=self._prefetch_count,
            callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.
        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame
        """
        LOGGER.info('QOS set to: %d.', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOGGER.info('Issuing consumer related RPC commands.')
        self.add_on_cancel_callback()
        self._consumer_tag = self.connector.channel.basic_consume(
            self.QUEUE,
            self.on_message)

        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        LOGGER.info('Adding consumer cancellation callback')
        self.connector.channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        self.connector.close_channel()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        self.process_callbacks('on_message')
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self.connector.channel.basic_ack(delivery_tag)

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self.connector._closing:
            self.connector._closing = True
            LOGGER.info('Stopping.')
            if self._consuming:
                self.stop_consuming()
                self.connector.ioloop.start()
            else:
                self.connector.ioloop.stop()
            LOGGER.info('Stopped.')

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        channel = self.connector.channel
        if channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ.')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        self._consuming = False
        LOGGER.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.connector.close_channel()


class BasePubSubConsumer(BaseConsumer, PubSubInterface):
    def __init__(
            self,
            connector,
            app_id='',
            queue='',
            durable=True,
            exchange='',
            exchange_type='topic',
            routing_key=''):
        super().__init__(
            connector=connector,
            app_id=app_id,
            queue=queue,
            durable=durable)
        self.EXCHANGE = exchange
        self.EXCHANGE_TYPE = exchange_type
        self.ROUTING_KEY = routing_key

    def start(self):
        """
        Method to start whatever the interface is designated to do.
        """
        self.setup_exchange(self.EXCHANGE)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('Declaring exchange %s', exchange_name)
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self.connector.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb,
            durable=self.durable)

    def on_exchange_declareok(self, unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('Exchange declared: %s.', userdata)
        self.setup_queue(self.QUEUE)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info('Binding %s to %s with %s', self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(
            self.QUEUE,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=self.on_bindok)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        LOGGER.info('Queue bound. Setting QOS.')
        self.set_qos()
