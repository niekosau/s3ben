import os
import signal
import multiprocessing
import time
from s3ben.s3 import S3Events
from s3ben.rabbit import RabbitMQ
from s3ben.helpers import drop_privileges, list_split, ProgressBar
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

    def _make_dir_tree(self, bucket_name: str, paths: list) -> None:
        """
        Create directory tree from list of paths
        :param str bucket_name: buclet name
        :param list paths: list with paths
        """
        _logger.info("Creating directory tree")
        uniq_dirs = {os.path.dirname(p) for p in paths}
        for dir in uniq_dirs:
            fp = os.path.join(self._backup_root, "active", bucket_name, dir)
            if os.path.isdir(fp):
                continue
            os.makedirs(fp)

    def sync_bucket_files(self, bucket: str, threads: int, page_size: int = 2000) -> None:
        """
        Sync files using paginator
        """
        _logger.info("Starting bucket sync-v2")
        start = time.perf_counter()
        progress = ProgressBar()
        info = self._s3_client.get_bucket(bucket)
        total_objects = info["usage"]["rgw.main"]["num_objects"]
        progress.total = total_objects
        progress.draw()
        paginator = self._s3_client.client_s3.get_paginator("list_objects_v2")
        page_config = {
                'PageSize': page_size
                }
        pages = paginator.paginate(Bucket=bucket, PaginationConfig=page_config)
        for page in pages:
            objects = [o.get("Key") for o in page["Contents"]]
            obf_fetched = len(objects)
            progress.progress += obf_fetched
            self._make_dir_tree(bucket_name=bucket, paths=objects)
            elements = 1 if len(objects) <= threads else len(objects) // threads
            splited_objects = list_split(objects, elements)
            processes = []
            for split in splited_objects:
                process = multiprocessing.Process(
                        target=self._s3_client.download_all_objects,
                        args=(bucket, split, ))
                processes.append(process)
            for proc in processes:
                _logger.debug(f"Starting {proc}")
                proc.start()
            for proc in processes:
                proc.join()
            progress.draw()
        progress.progress = total_objects
        progress.draw()
        print()
        end = time.perf_counter()
        _logger.info(f"Sync took: {round(end - start, 2)} seconds")
