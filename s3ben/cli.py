"""
S3ben entry module for cli commands
"""

import multiprocessing
import multiprocessing.managers
import os
import pwd
import signal
from argparse import Namespace
from logging import getLogger

from s3ben.arguments import base_args
from s3ben.backup import BackupManager
from s3ben.config import parse_config
from s3ben.decorators import argument, command
from s3ben.logger import init_logger
from s3ben.rabbit import MqConnection, MqExQue, RabbitMQ
from s3ben.remapper import ResolveRemmaping
from s3ben.s3 import S3Config, S3Events
from s3ben.sentry import init_sentry

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
    if os.path.isfile(parsed_args.sentry_conf):
        _logger.debug("Initializing sentry")
        init_sentry(config=parsed_args.sentry_conf)
    config = parse_config(parsed_args.config)
    parsed_args.func(config, parsed_args)


def run_remmaper(
    data_queue: multiprocessing.Queue,
    end_event: multiprocessing.Event,
    backup_root: str,
) -> None:
    """
    Function to start remmaper process

    :param multiprocessing.Queue data_queue: MP queue for data exchange
    :param multiprocessing.Event end_event: MP Event for finishing process
    :param str backup_root: Backup main directory
    :return: None
    """
    remap_resolver = ResolveRemmaping(backup_root=backup_root)
    remap_resolver.run(queue=data_queue, event=end_event)


def run_consumer(
    end_event: multiprocessing.Event, data_queue: multiprocessing.Queue, config: dict
) -> None:
    """
    Function to start consumers
    """
    main_section = config.pop("s3ben")
    mq_section: dict = config.pop("amqp")
    mq_ex_queue = MqExQue(
        exchange=mq_section.pop("exchange"), queue=mq_section.pop("queue")
    )
    mq_connection = MqConnection(**mq_section)

    backup_root = main_section.pop("backup_root")
    s3 = config.pop("s3")
    s3.pop("exclude")
    s3_config = S3Config(**s3)
    s3_events = S3Events(
        config=s3_config,
        backup_root=backup_root,
    )
    mq = RabbitMQ(conn_params=mq_connection.conn_params)
    backup = BackupManager(
        backup_root=backup_root,
        user=main_section.pop("user"),
        mq=mq,
        mq_queue=mq_ex_queue.queue,
    )
    backup.start_consumer(
        s3_client=s3_events, mp_data_queue=data_queue, mp_end_event=end_event
    )


def signal_handler(signal_no, stack_frame) -> None:
    """
    Function to hanle signlas
    :raises SystemExit: In all cases
    """
    raise SystemExit


@command(parent=subparser)  # type: ignore
def setup(config: dict, *_) -> None:
    """
    Cli command to add required cofiguration to s3 buckets and mq
    :param dict config: Parsed configuration dictionary
    :return: None
    """
    _logger.info("Checking backup root")
    main_section: dict = config.pop("s3ben")
    backup_root: str = main_section.pop("backup_root")
    user = pwd.getpwnam(main_section.pop("user"))
    if not os.path.exists(backup_root):
        os.mkdir(path=backup_root, mode=0o700)
        os.chown(path=backup_root, uid=user.pw_uid, gid=user.pw_gid)
    _logger.info("Setting up RabitMQ")
    mq_section = config.pop("amqp")
    mq_ex_queue = MqExQue(
        exchange=mq_section.pop("exchange"), queue=mq_section.pop("queue")
    )
    mq_connection = MqConnection(**mq_section)
    mq = RabbitMQ(conn_params=mq_connection.conn_params)
    mq.prepare(
        exchange=mq_ex_queue.exchange,
        queue=mq_ex_queue.queue,
        routing_key=mq_ex_queue.exchange,
    )
    _logger.info("Setting up S3")
    s3 = config.pop("s3")
    exclude_buckets = s3.pop("exclude").split(",")
    exclude_buckets = [b.strip() for b in exclude_buckets]
    s3_config = S3Config(**s3)
    s3_events = S3Events(config=s3_config)
    all_buckets = s3_events.get_admin_buckets()
    filtered_buckets = list(set(all_buckets) - set(exclude_buckets))
    s3_events.create_topic(
        config=mq_connection,
        exchange=mq_ex_queue.exchange,
    )
    for bucket in filtered_buckets:
        _logger.debug("Setting up bucket: %s", bucket)
        s3_events.create_notification(bucket=bucket, exchange=mq_ex_queue.exchange)


@command(
    [
        argument("--bucket", required=True, help="Bucket name which to sync", type=str),
        argument(
            "--transfers",
            help="Number of transfer processes, default: %(default)d",
            type=int,
            default=4,
        ),
        argument(
            "--checkers",
            help="Number of checker processes, default: %(default)d",
            type=int,
            default=4,
        ),
        argument("--skip-checksum", help="Skip checksum check", action="store_true"),
        argument("--skip-filesize", help="Skip filesize check", action="store_true"),
        argument(
            "--page-size",
            help="Bucket object page size, default: %(default)s",
            type=int,
            default=1000,
        ),
        argument(
            "--ui",
            help="Use experimental ui, default: %(default)s",
            action="store_true",
        ),
    ],
    parent=subparser,  # type: ignore
)
def sync(config: dict, parsed_args: Namespace):
    """
    Entry point for sync cli option
    """
    _logger.debug("Initializing sync")
    s3 = config.pop("s3")
    s3.pop("exclude")
    s3_config = S3Config(**s3)
    backup_root = config["s3ben"].pop("backup_root")
    s3_events = S3Events(
        config=s3_config,
        backup_root=backup_root,
    )
    backup = BackupManager(
        backup_root=backup_root,
        user=config["s3ben"].pop("user"),
        s3_client=s3_events,
        curses=parsed_args.ui,
    )
    backup.sync_bucket(
        parsed_args.bucket,
        parsed_args.transfers,
        parsed_args.page_size,
        parsed_args.checkers,
        parsed_args.skip_checksum,
        parsed_args.skip_filesize,
    )


@command(
    [
        argument("--show-excluded", help="Show excluded buckets", action="store_true"),
        argument("--show-obsolete", help="Show obsolete bucklets", action="store_true"),
        argument(
            "--only-enabled",
            help="Show only backup enabled buckets",
            action="store_true",
        ),
        argument(
            "--sort",
            help="Sort results by select collump, default: %(default)s",
            choices=["bucket", "owner", "size", "objects"],
            default="bucket",
        ),
        argument(
            "--sort-reverse", help="Reverse order for sorting", action="store_true"
        ),
    ],
    parent=subparser,  # type: ignore
)
def buckets(config: dict, parsed_args: Namespace) -> None:
    """
    Cli command: buckets
    """
    _logger.debug("Listing buckets")
    s3 = config.pop("s3")
    exclude = s3.pop("exclude").replace('"', "").replace("'", "").split(",")
    exclude = [e.strip() for e in exclude]
    s3_config = S3Config(**s3)
    backup_root = config["s3ben"].pop("backup_root")
    s3_events = S3Events(
        config=s3_config,
        backup_root=backup_root,
    )
    backup = BackupManager(
        backup_root=backup_root,
        user=config["s3ben"].pop("user"),
        s3_client=s3_events,
    )
    backup.list_buckets(
        exclude=exclude,
        show_excludes=parsed_args.show_excluded,
        show_obsolete=parsed_args.show_obsolete,
        only_enabled=parsed_args.only_enabled,
        sort=parsed_args.sort,
        sort_revers=parsed_args.sort_reverse,
    )


@command(
    [
        argument(
            "--days-keep",
            help="How long to keep, default: %(default)d",
            default=30,
            type=int,
        )
    ],
    parent=subparser,  # type: ignore
)
def cleanup(config: dict, parsed_args: Namespace) -> None:
    """
    Cli function to call deleted items cleanup method
    from BackupManager
    """
    _logger.debug("Starting deleted items cleanup")
    backup_root = config["s3ben"].pop("backup_root")
    backup = BackupManager(
        backup_root=backup_root,
        user=config["s3ben"].pop("user"),
    )
    backup.cleanup_deleted_items(days=parsed_args.days_keep)


@command(
    [
        argument(
            "--consumers",
            type=int,
            default=4,
            help="Number of consumer processes (max limited to cpu cores), default: %(default)s",
        )
    ],
    parent=subparser,  # type: ignore
)
def consume_v2(config: dict, parsed_args: Namespace) -> None:
    """
    Function to start/restart consumers and other needed processes

    :param str backup_root: Backup root
    :param int n_proc: number of consumer processes to start, default 4
    :return: None
    """
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    max_proc = multiprocessing.cpu_count()
    n_consumers = min(max_proc, parsed_args.consumers)
    backup_root = config["s3ben"].get("backup_root")
    processes = []
    with multiprocessing.managers.SyncManager() as p_manager:
        data_queue = p_manager.Queue()
        end_event = p_manager.Event()
        try:
            remapper_proc = multiprocessing.Process(
                target=run_remmaper,
                args=(
                    data_queue,
                    end_event,
                    backup_root,
                ),
                name="remmaper",
            )
            remapper_proc.start()
            processes.append(remapper_proc)
            for _ in range(n_consumers):
                consumer = multiprocessing.Process(
                    target=run_consumer,
                    args=(
                        end_event,
                        data_queue,
                        config,
                    ),
                )
                consumer.start()
                processes.append(consumer)
            for proc in processes:
                proc.join()
        except (KeyboardInterrupt, SystemExit):
            for proc in processes:
                proc.terminate()
