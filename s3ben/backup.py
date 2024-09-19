import os
import signal
from s3ben.s3 import S3Events
from s3ben.rabbit import RabbitMQ
from s3ben.helpers import drop_privileges
from logging import getLogger
from pathlib import Path

_logger = getLogger(__name__)


class BackupManager():
    """
    Class to coordinate all tasks

    :param str backup_root: Destination directory were all files will be placed
    :param str user: username to which change privileges
    :param str mq_queue: rabbitmq queue name
    :param RabbitMQ mq: RabbitMQ class object
    """

    def __init__(
            self,
            backup_root: str,
            user: str,
            mq_queue: str = None,
            mq: RabbitMQ = None,
            s3_client: S3Events = None):
        self._backup_root = backup_root
        self._user = user
        self._mq = mq
        self._mq_queue = mq_queue
        self._s3_client = s3_client
        signal.signal(signal.SIGTERM, self.__exit)
        signal.signal(signal.SIGINT, self.__exit)

    def __exit(self, signal_no, stack_frame) -> None:
        raise SystemExit("Exiting")

    def _check_destination(self, path: Path) -> None:
        if not os.path.exists(path):
            _logger.debug(f"Creating: {path}")
            os.makedirs(path, mode=0o750)
            os.chown(path, uid=self._uuid, gid=self._guid)

    def start_consumer(self, s3_client: S3Events) -> None:
        _logger.debug(f"Dropping privileges to {self._user}")
        drop_privileges(user=self._user)
        try:
            self._mq.consume(queue=self._mq_queue, s3_client=s3_client)
        except KeyboardInterrupt:
            self._mq.stop()
        except SystemExit:
            self._mq.stop()

    def sync_bucket_files(self, bucket: str, threads: int) -> None:
        _logger.info(f"Syncing bucket: {bucket}")
        # bucket_info = self._s3_client.get_bucket(bucket=bucket)
        # _logger.debug(json.dumps(bucket_info, indent=2, default=str))
        self._s3_client.download_all_objects(
                bucket_name=bucket,
                dest=self._backup_root,
                threads=threads)
