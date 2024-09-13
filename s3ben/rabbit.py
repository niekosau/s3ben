import pika
import functools
import json
from logging import getLogger
from pika.exchange_type import ExchangeType

_logger = getLogger(__name__)


class Amqp():
    """
    Class to setup amqp (rabbit)
    :param str hostname: rabbitmq server address
    :param str user: username for connection
    :param str password: password for user
    :param int port: port of mq server, default: 5672
    :param str virtualhost: virtual host to connect, default: /
    """

    EXCHANGE = 'message'
    EXCHANGE_TYPE = ExchangeType.topic
    QUEUE = 'text'
    ROUTING_KEY = 'example.text'

    def __init__(self, hostname: str, user: str, password: str, port: int, virtualhost: str) -> None:
        mq_credentials = pika.credentials.PlainCredentials(user, password)
        self.mq_params = pika.ConnectionParameters(host=hostname, port=port, virtual_host=virtualhost, credentials=mq_credentials)
        self.should_reconnect: bool = False
        self.was_consuming = False

        self._connection: pika.connection.Connection = None
        self._channel: pika.channel.Channel = None
        self._consuming: bool = False
        self._closing: bool = False
        self._consumer_tag = None
        self._prefetch_count = 1

    def connect(self) -> None:
        """
        This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection
        """
        _logger.info("Connecting to MQ")
        self._connection = pika.SelectConnection(
                parameters=self.mq_params,
                on_open_callback=self.on_connection_open,
                on_open_error_callback=self.on_connection_open_error,
                on_close_callback=self.on_connection_closed)
        self._connection.ioloop.start()

    def on_connection_open(self, _unused_connection) -> None:
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection
        """
        _logger.debug("Connection opened")
        self.open_channel()

    def open_channel(self) -> None:
        """
        Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        _logger.debug("Creating new channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel: pika.channel.Channel) -> None:
        """
        This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object
        """
        _logger.debug("Channel opened")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self) -> None:
        """
        This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        _logger.debug("Adding channel close callback")
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channle_closed(self, channel, reason) -> None:
        """
        Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        _logger.warning(f"Channel {channel} was closed: {reason}")
        self.close_connection()

    def close_connection(self) -> None:
        """
        Close connection

        :return: None
        """
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            _logger.info("Connection closing or already closed")
            return
        _logger.info("Closing connection")
        self._connection.close()

    def on_connection_open_error(self, _unused_connection, err) -> None:
        """
        This method is called by pika if the connection to RabbitMQ
        can't be established.

        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        _logger.error(f"Connection open failed: {err}")
        self.reconnect()

    def reconnect(self) -> None:
        """
        Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def stop(self) -> None:
        """
        Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            _logger.info("Stopping")
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            _logger.info("Stopped")

    def stop_consuming(self) -> None:
        """
        Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            _logger.debug("Sending a Basic.Cancel RPC command to RabbitMQ")
            callback = functools.partial(self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, callback)

    def on_cancelok(self, _unused_frame, userdata) -> None:
        """
        This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)
        """
        self._consuming = False
        _logger.debug(f"RabbitMQ acknowledged the cancellation of the consumer: {userdata}")
        self.close_channel()

    def close_channel(self) -> None:
        """
        Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        _logger.info("Closing the channel")
        self._channel.close()

    def on_connection_closed(self, _unused_connection, reason) -> None:
        """
        This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
            return
        _logger.warning(f"Connection closed, reconnecting because: {reason}")
        self.reconnect()

    def on_channel_closed(self, channel, reason) -> None:
        """
        Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        _logger.warning(f"Channel {channel} was closed: {reason}")
        self.close_connection()

    def setup_queue(self, queue) -> None:
        """
        Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.
        """
        _logger.debug(f"Creating queue: {queue}")
        callback = functools.partial(self.on_queue_declareok, userdata=queue)
        self._channel.queue_declare(
                queue=queue,
                durable=True,
                auto_delete=False,
                arguments={"x-queue-type": "quorum"},
                callback=callback)

    def on_queue_declareok(self, _unused_frame, userdata) -> None:
        """
        Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode userdata: Extra user data (queue name)

        """
        queue = userdata
        _logger.debug(f"Binding {self.EXCHANGE} to {queue} with {self.ROUTING_KEY}")
        callback = functools.partial(self.on_bindok, userdata=queue)
        self._channel.queue_bind(
                queue=queue,
                exchange=self.EXCHANGE,
                routing_key=self.ROUTING_KEY,
                callback=callback)

    def on_bindok(self, _unused_frame, userdata) -> None:
        """
        Invoked by pika when the Queue.Bind method has completed. At this
        point we will set the prefetch count for the channel.

        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame
        :param str|unicode userdata: Extra user data (queue name)
        """
        _logger.debug(f"queue bound: {userdata}")
        self.start_consuming()

    def setup_exchange(self, exchange) -> None:
        """
        Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare
        """
        _logger.debug(f"Declaring exhange {exchange}")
        callback = functools.partial(self.on_exchange_declareok, userdata=exchange)
        self._channel.exchange_declare(
                exchange=exchange,
                durable=True,
                exchange_type="direct",
                auto_delete=False,
                internal=False,
                callback=callback)

    def on_exchange_declareok(self, _unused_frame, userdata) -> None:
        """
        Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        _logger.debug(f"Exhange declared: {userdata}")
        self.setup_queue(self.QUEUE)

    def start_consuming(self) -> None:
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        _logger.info("Starting to consume messages")
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.QUEUE, self.on_message)

    def add_on_cancel_callback(self) -> None:
        """
        Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        _logger.debug("Adding consumer cancel callback")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame) -> None:
        """
        Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        _logger.info(f"Consumer canceled, shuting down {method_frame}")
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body) -> None:
        """
        Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel _unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body
        """
        body = json.loads(body.decode())
        _logger.debug(f"Received message;\n{basic_deliver.delivery_tag}, {properties.app_id}\n{json.dumps(body, indent=2)}")
        for record in body["Records"]:
            obj_size = record["s3"]["object"]["size"]
            if obj_size == 0:
                continue
            event = record["eventName"]
            bucket = record["s3"]["bucket"]["name"]
            obj_key = record["s3"]["object"]["key"]
            _logger.info(f"Bucket: {bucket} event: {event} file: {obj_key} size: {obj_size}")
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag) -> None:
        """
        Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        _logger.info(f"Ack message: {delivery_tag}")
        self._channel.basic_ack(delivery_tag)


def setup_rabbit(
        host: str,
        user: str,
        password: str,
        exchange: str,
        queue: str,
        routing_key: str,
        port: int = 5672,
        virtualhost: str = "/") -> None:
    """
    Function to setup rabbitmq
    :param str hostname: rabbitmq address
    :param str user: username for connection string
    :param str password: password for provided username
    :param str exchange: exchange to declare
    :param str queue: queue to declare
    :param str routing_key: routing key which routes from exchange to queue
    :param int port: port which to connect, default: 5672
    :param str virtualhost: rabbitmq virtualhos, default /
    :return: None
    """
    _logger.debug("Running rabbit setup")
    mq = Amqp(
            hostname=host,
            user=user,
            password=password,
            port=port,
            virtualhost=virtualhost)
    mq.QUEUE = queue
    mq.EXCHANGE = exchange
    mq.ROUTING_KEY = routing_key
    try:
        mq.connect()
    except KeyboardInterrupt:
        mq.stop()
