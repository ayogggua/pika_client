# -*- coding: utf-8 -*-

import logging
import functools
import pika

from mill_common.mixins import CallbackMixin

LOGGER = logging.getLogger(__name__)


class BaseConnector(CallbackMixin, object):
    """This is base class that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    RECONNECT_INTERVAL = 5  # seconds.

    def __init__(
            self,
            amqp_url,
            reconnect_interval=None):
        """Create a new instance of the subscriber/publisher class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        super().__init__()
        self._url = amqp_url
        if reconnect_interval is not None:
            self.RECONNECT_INTERVAL = reconnect_interval  # seconds.

        self._connection = None
        self._channel = None
        self._closing = False

    @property
    def channel(self):
        return self._channel

    @property
    def connection(self):
        return self._connection

    @property
    def ioloop(self):
        return self.connection.ioloop

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        LOGGER.info('Connecting to %s', self._url)
        self._connection = pika.SelectConnection(
            pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)
        return self._connection

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(self.RECONNECT_INTERVAL, self._connection.ioloop.stop)

    def on_connection_closed(self, connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param str reason: The server provided reason if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in %d seconds: %s.', self.RECONNECT_INTERVAL, reason)
            self._connection.ioloop.call_later(self.RECONNECT_INTERVAL, self._connection.ioloop.stop)

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        LOGGER.info('Creating a new channel.')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.process_callbacks('on_channel_open')

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param str reason: The text reason the channel was closed

        """
        LOGGER.warning('Channel %i was closed: %s',
                       channel, reason)
        self._channel = None
        if not self._closing:
            self.close_connection()    

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        if self._channel is not None:
            LOGGER.info('Closing the channel')
            self._channel.close()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None and not self._connection.is_closed:
            LOGGER.info('Closing connection')
            self._connection.close()

    def run(self):
        try:
            self._connection = self.connect()
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            self.stop()
            if (self._connection is not None and
                    not self._connection.is_closed):
                # Finish closing
                self._connection.ioloop.start()


class Connector(BaseConnector):
    pass


class PubSubInterface(object):
    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        raise NotImplementedError

    def on_exchange_declareok(self, unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        raise NotImplementedError

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        raise NotImplementedError
