# -*- coding: utf-8 -*-

import logging
import json
import functools
import pika

from pika_client.base import PubSubInterface
from pika_client.mixins import CallbackMixin

LOGGER = logging.getLogger(__name__)


class BasePublisher(CallbackMixin, object):
    def __init__(self, connector, app_id='', queue='', durable=True, delivery_mode=2):
        super().__init__()
        self.connector = connector
        self.APP_ID = app_id
        self.EXCHANGE = ''
        self.QUEUE = queue
        # defaults to True and makes the queue store the message for recovery if ever RabbitMQ goes down.
        self.durable = durable
        # defaults to 2 and persists the message to the disk even if RabbitMQ dies.
        self.delivery_mode = delivery_mode

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
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info("Queue declared. Enabing delivery confirmations.")
        self.enable_delivery_confirmations()

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        LOGGER.info('Issuing Confirm.Select RPC command')
        self.connector.channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.info('Received %s for delivery tag: %i',
                    confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            LOGGER.info('Acknowledged message %i.', method_frame.method.delivery_tag)
        elif confirmation_type == 'nack':
            # TODO: retry logic.
            raise NotImplementedError

    def publish_message(self, message):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """
        channel = self.connector.channel
        if channel is None or not channel.is_open:
            LOGGER.error("Cannot publish the message. Channel unavailable or closed.")
            return

        headers = {}
        properties = pika.BasicProperties(
            app_id=self.APP_ID,
            delivery_mode=self.delivery_mode,
            content_type='application/json',
            headers=headers)

        channel.basic_publish(
            self.EXCHANGE,
            self.ROUTING_KEY,
            json.dumps(message, ensure_ascii=False),
            properties)

        LOGGER.info('Published message # %s', message)

    def stop(self):
        """Stop the service by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.
        """
        LOGGER.info('Stopping.')
        self.connector._closing = True
        self.connector.stop()
        LOGGER.info("Stopped.")


class BasePubSubPublisher(BasePublisher, PubSubInterface):
    def __init__(
            self,
            connector,
            app_id='',
            queue=None,
            durable=True,
            delivery_mode=2,
            exchange='',
            exchange_type='topic',
            routing_key=''):
        # we omit queue in the call to the super bcos, pubsub producer should not send to a queue directly.
        # all messages are routed through an exchange and received by queues bound to it.
        super().__init__(connector=connector, app_id=app_id, delivery_mode=delivery_mode)
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
            durable=self.durable,
            callback=cb)

    def on_exchange_declareok(self, unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('Exchange declared: %s.', userdata)
        self.enable_delivery_confirmations()
