import os
import pwd
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
            mq_queue: str,
            mq: RabbitMQ):
        self._backup_root = backup_root
        self._user = user
        u_details = pwd.getpwnam(self._user)
        self._uuid = u_details.pw_uid
        self._guid = u_details.pw_gid
        self._mq = mq
        self._mq_queue = mq_queue
        if not os.path.exists(self._backup_root):
            _logger.warning("Creating backup root")
            os.mkdir(path=self._backup_root, mode=0o700)
            os.chown(path=self._backup_root, uid=self._uuid, gid=self._guid)
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
