import pika
from logging import getLogger

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

    def __init__(self, hostname: str, user: str, password: str, port: int = 5672, virtualhost: str = "/") -> None:
        mq_credentials = pika.credentials.PlainCredentials(user, password)
        mq_params = pika.ConnectionParameters(host=hostname, port=port, virtual_host=virtualhost, credentials=mq_credentials)
        mq_connection = pika.BlockingConnection(parameters=mq_params)
        self.channel = mq_connection.channel()

    def _create_exchange(self, name: str) -> None:
        """
        Create mq exchange
        :param str name: Name of exchange
        :return: None
        """
        _logger.debug(f"creating exchange: {name}")
        self.channel.exchange_declare(exchange=name, durable=True, exchange_type="direct", auto_delete=False, internal=False)

    def _create_queue(self, name: str) -> None:
        """
        Create mq queue
        :param str name: Name of queu
        :return: None
        """
        self.channel.queue_declare(queue=name, durable=True, auto_delete=False)

    def _bind_queue(self, queue: str, exchange: str, key: str) -> None:
        """
        Bind queu to exchange by routing key
        """
        self.channel.queue_bind(queue=queue, exchange=exchange, routing_key=key)

    def setup(self, exchange: str, queue: str, key: str) -> None:
        """
        Run setup tasks
        :param str exchange: exhange name
        :param str queue: queue name
        :param str key: routing key for binding
        """
        self._create_queue(name=queue)
        self._create_exchange(name=exchange)
        self._bind_queue(queue=queue, exchange=exchange, key=key)


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
    mq.setup(exchange=exchange, queue=queue, key=routing_key)
