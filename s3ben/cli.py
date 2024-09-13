from s3ben.logger import init_logger
from s3ben.sentry import init_sentry
from s3ben.decorators import command
from s3ben.arguments import base_args
from s3ben.s3 import setup_buckets
from s3ben.config import parse_config
from s3ben.rabbit import setup_rabbit
from logging import getLogger

_logger = getLogger(__name__)
args = base_args()
subparser = args.add_subparsers(dest="subcommand")


def main() -> None:
    """
    Entry point
    :raises ValueError: if config file not found
    :return: None
    """
    parsed_args = args.parse_args()
    if parsed_args.subcommand is None:
        args.print_help()
        return
    init_logger(name="s3ben", level=parsed_args.log_level)
    if parsed_args.sentry_conf:
        _logger.debug("Initializing sentry")
        init_sentry(config=parsed_args.sentry_conf)
    config = parse_config(parsed_args.config)
    parsed_args.func(config)


@command(parent=subparser)
def setup(config: dict) -> None:
    """
    Cli command to add required cofiguration to s3 buckets and mq
    :param dict config: Parsed configuration dictionary
    :return: None
    """
    _logger.info("Creating required configs")
    amqp = config.pop("amqp")
    amqp["routing_key"] = amqp["exchange"]
    s3 = config.pop("s3")
    s3["mq_host"] = amqp["host"]
    s3["mq_user"] = amqp["user"]
    s3["mq_password"] = amqp["password"]
    s3["mq_exchange"] = amqp["exchange"]
    s3["mq_routing_key"] = amqp["routing_key"]
    s3["mq_port"] = amqp["port"]
    setup_rabbit(**amqp)
    # setup_buckets(**s3)
