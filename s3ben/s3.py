import datetime
import itertools
import json
import multiprocessing
import os
import pathlib
import queue
import shutil
import sys
import threading
import time
from dataclasses import dataclass, field
from logging import getLogger
from multiprocessing.synchronize import Event as EventClass
from queue import Empty, Full
from typing import Dict, Final, Generator, List, Optional, Union

import boto3
import botocore
import botocore.errorfactory
import rgwadmin
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.paginator import ListObjectsV2Paginator
from rgwadmin import RGWAdmin
from typing_extensions import TypeAlias

# from s3ben.backup import BackupLocations
from s3ben.constants import AMQP_HOST, NOTIFICATION_EVENTS, TOPIC_ARN
from s3ben.progress import ConsoleBar

_logger = getLogger(__name__)

Path: TypeAlias = pathlib.Path
ServiceResource: TypeAlias = boto3.resources.base.ServiceResource
Queue: TypeAlias = queue.Queue
Event: TypeAlias = threading.Event
SyncManager: TypeAlias = multiprocessing.managers.SyncManager


@dataclass
class S3Config:
    """
    Dataclas for s3 configuration
    """

    hostname: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    secure: bool
    exclude: str


# TODO: Remove this class


class S3Clients:
    """
    S3 client class
    """

    def __init__(self, config: S3Config, **kwargs) -> None:
        del kwargs
        self._config = config
        self._protocol = "https" if config.secure else "http"

    @property
    def s3_client(self):
        """
        Method to create s3 default client
        """
        return boto3.client(
            service_name="s3",
            region_name="default",
            endpoint_url=self._protocol + "://" + self._config.hostname,
            aws_access_key_id=self._config.access_key,
            aws_secret_access_key=self._config.secret_key,
        )

    @property
    def sns_client(self):
        """
        Method to create sns client, needed for some actions
        """
        return boto3.client(
            service_name="sns",
            region_name="default",
            endpoint_url=self._protocol + "://" + self._config.hostname,
            aws_access_key_id=self._config.access_key,
            aws_secret_access_key=self._config.secret_key,
            config=botocore.client.Config(signature_version="s3"),
        )

    @property
    def admin_client(self):
        """
        Method to create admin client
        """
        return RGWAdmin(
            access_key=self._config.access_key,
            secret_key=self._config.secret_key,
            server=self._config.hostname,
            secure=self._config.secure,
        )

    @property
    def s3_resouce(self) -> ServiceResource:
        """
        Method to create s3 resouce
        """
        s3_session = boto3.Session(
            region_name="default",
            aws_access_key_id=self._config.access_key,
            aws_secret_access_key=self._config.secret_key,
        )
        return s3_session.resource(
            service_name="s3",
            endpoint_url=self._protocol + "://" + self._config.hostname,
        )


@dataclass
class BackupLocations:
    """
    Dataclass to represent paths used in tool
    """

    root: Path
    active: Path = field(init=False)
    deleted: Path = field(init=False)

    def __post_init__(self) -> None:
        self.active = pathlib.Path(os.path.join(self.root, "active"))
        self.deleted = pathlib.Path(os.path.join(self.root, "deleted"))


# TODO: Implemnt for bucket listing
@dataclass
class BucketExtended:
    """
    Dataclass for bucket extended info
    """

    owner: str
    size: int
    enabled: bool
    excluded: bool
    obsolete: bool

    # def __get_bucket(self, bucket: str) -> Bucket:
    #     """
    #     Get bucket stats via admin api
    #     """
    #     bucket_stats: dict = self._clients.admin_client.get_bucket(bucket=bucket)
    #     size = 0
    #     objects = 0
    #     if "rgw.main" in bucket_stats["usage"].keys():
    #         size = bucket_stats["usage"]["rgw.main"].pop("size_actual")
    #         objects = bucket_stats["usage"]["rgw.main"].pop("num_objects")
    #     return Bucket(
    #         name=bucket_stats.pop("bucket"),
    #         owner=bucket_stats.pop("owner"),
    #         size=size,
    #         objects=objects,
    #     )


class Bucket:
    """
    Dataclass to represent bucket object and bucket related actions
    """

    def __init__(
        self, name: str, locations: BackupLocations, s3_config: S3Config
    ) -> None:
        self.name = name
        self._active_files = pathlib.Path(os.path.join(locations.active, name))
        self.locations = locations
        self.s3_config = s3_config

    def __str__(self) -> str:
        """
        Default str overide
        """
        return f"Bucket: {self.name}"

    @property
    def bucket_metrics(self) -> tuple:
        """
        Returns bucket metrics: size and num objects
        :rtype: tuple
        :return: (size, n_objects)
        """
        size = 0
        n_objects = 0
        client = self._s3_client(service_name="admin")
        metrics = client.get_bucket(bucket=self.name)
        if "rgw.main" in metrics["usage"].keys():
            size = metrics["usage"]["rgw.main"].pop("size_actual")
            n_objects = metrics["usage"]["rgw.main"].pop("num_objects")
        return size, n_objects

    @property
    def backup_path(self) -> Path:
        """
        Property method to return current bucket backup location
        """
        return self._active_files

    def _s3_client(self, service_name: str = "s3") -> S3Client:
        """
        Method to create and return S3 client
        :param str service_name: One of: s3, sns, admin
        :rtype: S3Client
        :return: S3 client for specified service
        """
        protocol = "https" if self.s3_config.secure else "http"
        if service_name == "admin":
            return RGWAdmin(
                access_key=self.s3_config.access_key,
                secret_key=self.s3_config.secret_key,
                secure=self.s3_config.secure,
                server=self.s3_config.hostname,
            )
        return boto3.client(
            service_name=service_name,
            region_name="default",
            endpoint_url=protocol + "://" + self.s3_config.hostname,
            aws_access_key_id=self.s3_config.access_key,
            aws_secret_access_key=self.s3_config.secret_key,
            config=botocore.client.Config(signature_version="v4"),
        )

    def download_object(self, s3_key: str) -> None:
        """
        Get an object from S3
        :param client: S3 client
        :param str s3_key
        """
        # TODO: Not implemented yet
        if s3_key[0] == "/":
            _logger.warning("Forward slash found as first simbol, skipping")
            return
        dst = os.path.join(self.backup_path, s3_key)
        # TODO: Add more verification options, like file size, md5
        if os.path.exists(dst):
            _logger.debug("%s already exists", s3_key)
            return
        dst_dir = os.path.dirname(dst)
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir, exist_ok=True)
        try:
            client = self._s3_client()
            _logger.debug("Downloading %s", s3_key)
            client.download_file(Bucket=self.name, Key=s3_key, Filename=dst)
        except botocore.exceptions.ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                _logger.warning("Object %s not found", s3_key)
                return
            raise err

    def upload_object(self, local_file: str, s3_key: str) -> None:
        """
        Upload file to bucket
        """
        try:
            client = self._s3_client()
            response = client.put_object(
                Body=local_file, Bucket=self.name, Key=s3_key
            ).get("ResponseMetadata")
        except botocore.exceptions.ClientError as e:
            response = e.response["ResponseMetadata"]
        finally:
            if response.get("HTTPStatusCode") != 200:
                _logger.error("Cannot upload %s", local_file)
                sys.exit(1)

    def check_object(self, s3_key: str) -> bool:
        """
        Method to check if given S3 key exists
        """
        _logger.debug("Checking: %s", s3_key)
        try:
            client = self._s3_client()
            data = client.head_object(Bucket=self.name, Key=s3_key).get(
                "ResponseMetadata"
            )
        except botocore.exceptions.ClientError as e:
            data = e.response["ResponseMetadata"]
        if data.get("HTTPStatusCode") == 200:
            return True
        return False

    def bucket_pages(self, size: int):
        """
        Get bucket paginator
        """
        p_size = min(size, 1000)
        client = self._s3_client()
        paginator = client.get_paginator("list_objects_v2")
        page_config = {"PageSize": p_size}
        return paginator.paginate(Bucket=self.name, PaginationConfig=page_config)


class S3manager:
    """
    S3 multiprocess manager
    """

    def __init__(self, bucket: Bucket) -> None:
        self._bucket = bucket

    def __file_traverser(
        self,
        size: int,
        q_data: Queue,
        q_progress: Queue,
        e_interrupt: Event,
        e_end: Event,
        **kwargs,
    ) -> None:
        """
        Creates a generator for files
        """
        backup_path = self._bucket.backup_path
        for key, value in kwargs.items():
            if key == "backup_location":
                backup_path = value
                break
        page = []
        for file in backup_path.glob("**/*"):
            if e_interrupt.is_set():
                break
            if not file.is_file():
                continue
            try:
                page.append(file.relative_to(backup_path).as_posix())
                if len(page) == size:
                    self.__put_data(data=page, q=q_data, e_interrupt=e_interrupt)
                    q_progress.put({"total": size})
                    page = []
            except KeyboardInterrupt:
                break
        e_end.set()

    def _page_reader(
        self, page_size: int, q: Queue, e_interrupt: Event, e_end: Event
    ) -> None:
        """
        Read page by page
        """
        for page in self._bucket.bucket_pages(page_size):
            try:
                data = [key.get("Key") for key in page["Contents"]]
                self.__put_data(data, q=q, e_interrupt=e_interrupt)
            except KeyboardInterrupt:
                e_interrupt.set()
                break
        e_end.set()

    def __put_data(self, data: list, q: Queue, e_interrupt: Event) -> None:
        """
        Loop until queue not empty
        """
        while True:
            try:
                q.put(data, timeout=0.1, block=True)
            except Full:
                if e_interrupt.is_set():
                    break
                continue
            else:
                break

    def __verify_files(
        self, q_data: Queue, q_progress: Queue, e_interrupt: Event, e_end: Event
    ) -> None:
        """
        For multiprocess support
        """
        while True:
            try:
                data = q_data.get(timeout=0.2)
                for f in data:
                    if e_interrupt.is_set():
                        break
                    exists = self._bucket.check_object(s3_key=f)
                    if exists:
                        progress_update = {"vrf": 1}
                        q_progress.put(progress_update)

            except Empty:
                if e_end.is_set():
                    break
                continue
            except KeyboardInterrupt:
                break

    def __upload_objects(
        self,
        dst_bucket: Bucket,
        q_data: Queue,
        q_progress: Queue,
        e_interrupt: Event,
        e_end: Event,
    ) -> None:
        """
        Method to call bucket upload method
        """
        while True:
            try:
                data = q_data.get(timeout=0.2)
                for f in data:
                    if dst_bucket.check_object(f):
                        q_progress.put({"vrf": 1})
                        continue
                    src = os.path.join(self._bucket.backup_path, f)
                    dst_bucket.upload_object(src, f)
                    q_progress.put({"dl": 1})
                    if e_interrupt.is_set():
                        break
            except Empty:
                if e_end.is_set():
                    break
                continue

    def __check_local_object(
        self,
        q_check: Queue,
        q_download: Queue,
        q_progress: Queue,
        e_interrupt: Event,
        e_end: Event,
    ) -> None:
        """
        Ceck if object exists
        """
        while True:
            try:
                dl_list = []
                data = q_check.get(timeout=0.2, block=True)
                for o in data:
                    if e_interrupt.is_set():
                        return
                    local_file = os.path.join(self._bucket.backup_path, o)
                    if os.path.exists(local_file):
                        progress_update = {"vrf": 1}
                        q_progress.put(progress_update)
                        continue
                    dl_list.append(o)
                if len(dl_list) > 0:
                    q_download.put(dl_list)
            except Empty:
                if e_end.is_set():
                    break
                continue
            except KeyboardInterrupt:
                break

    def __download_objects(
        self, q_data: Queue, q_progress: Queue, e_interrupt: Event, e_end: Event
    ) -> None:
        """
        Method to download all missing objects from S3 bucket
        :rtype: None
        """
        while True:
            try:
                data = q_data.get(timeout=0.2)
                for o in data:
                    if e_interrupt.is_set():
                        return
                    progress_update = {"dl": 1}
                    self._bucket.download_object(o)
                    q_progress.put(progress_update)
            except Empty:
                if e_end.is_set():
                    break
                continue
            except KeyboardInterrupt:
                break

    def __download_progress(
        self, q: Queue, e_interrupt: Event, e_end: Event, interval: int
    ) -> None:
        """
        Method to draw download progress
        :rtype: ConsoleBar
        """
        _, n_objects = self._bucket.bucket_metrics
        progress = ConsoleBar(
            total=n_objects, avg_interval=interval, exit_event_queue=e_end
        )
        p_bar = threading.Thread(target=progress.draw_bar, args=(1,))
        p_bar.start()
        while True:
            try:
                if e_interrupt.is_set():
                    break
                data = q.get(block=True, timeout=0.1)
            except Empty:
                if e_end.is_set():
                    break
                continue
            except KeyboardInterrupt:
                break
            else:
                progress.data.update_counters(data)
        p_bar.join()
        del progress

    def __verify_progress(
        self, q: Queue, e_interrupt: Event, e_end: Event, interval: int
    ) -> None:
        """
        Draw verify progress
        """
        progress = ConsoleBar(total=0, avg_interval=interval, exit_event_queue=e_end)
        p_bar = threading.Thread(target=progress.draw_bar, args=(1,))
        p_bar.start()
        while True:
            try:
                if e_interrupt.is_set():
                    break
                data = q.get(block=True, timeout=0.1)
            except Empty:
                if e_end.is_set():
                    break
                continue
            except KeyboardInterrupt:
                break
            else:
                progress.data.update_counters(data)
        p_bar.join()
        del progress

    def __upload_progress(
        self, q: Queue, e_interrupt: Event, e_end: Event, interval: int
    ) -> None:
        """
        Draw upload progress
        """
        progress = ConsoleBar(total=0, avg_interval=interval, exit_event_queue=e_end)
        p_bar = threading.Thread(target=progress.draw_bar, args=(1,))
        p_bar.start()
        while True:
            try:
                if e_interrupt.is_set():
                    break
                data = q.get(block=True, timeout=0.1)
            except Empty:
                if e_end.is_set():
                    break
                continue
            except KeyboardInterrupt:
                break
            else:
                progress.data.update_counters(data)
        p_bar.join()
        del progress

    def verify_files(self, n_proc: int, batch_size: int, **kwargs) -> None:
        """
        Method to verify local files agains remote
        """
        update_interval = 60
        # TODO: Not implemented yet
        for k, v in kwargs.items():
            if k == "save_to_file":
                _logger.warning("Not implemented yet")
            if k == "update_interval":
                update_interval = min(v, update_interval)
        with multiprocessing.managers.SyncManager() as mp_manager:
            data_queue = mp_manager.Queue(maxsize=n_proc * 2)
            progress_queue = mp_manager.Queue()
            end_event = mp_manager.Event()
            interrupt_event = mp_manager.Event()
            progress_end_event = mp_manager.Event()
            try:
                processes = []
                f_process = multiprocessing.Process(
                    target=self.__file_traverser,
                    args=(
                        batch_size,
                        data_queue,
                        progress_queue,
                        interrupt_event,
                        end_event,
                    ),
                )
                processes.append(f_process)
                p_progress = multiprocessing.Process(
                    target=self.__verify_progress,
                    args=(
                        progress_queue,
                        interrupt_event,
                        progress_end_event,
                        update_interval,
                    ),
                )
                p_progress.start()
                for _ in range(n_proc):
                    c_process = multiprocessing.Process(
                        target=self.__verify_files,
                        args=(
                            data_queue,
                            progress_queue,
                            interrupt_event,
                            end_event,
                        ),
                    )
                    processes.append(c_process)
                for t in processes:
                    t.start()
                for p in processes:
                    p.join()
                progress_end_event.set()
                p_progress.join()
            except KeyboardInterrupt:
                interrupt_event.set()
                progress_end_event.set()
                for p in processes:
                    p.join()
                p_progress.join()

    def sync_from_backup(
        self,
        n_proc: int,
        batch_size: int,
        dst_bucket: Bucket,
    ) -> None:
        """
        Check and sync backup to dst bucket
        :param Bucket src_bucket: Bucket name from backup
        :param Bucket dst_bucket: Optional where to sync, if not provided, src_bucket will be used
        :rtype: None
        """
        processes = []
        with multiprocessing.managers.SyncManager() as mp_manager:
            check_queue = mp_manager.Queue(maxsize=2)
            data_queue = mp_manager.Queue(maxsize=n_proc * 10)
            progress_queue = mp_manager.Queue()
            end_event = mp_manager.Event()
            interrupt_event = mp_manager.Event()
            progress_end_event = mp_manager.Event()
            try:
                p_process = multiprocessing.Process(
                    target=self.__upload_progress,
                    args=(
                        progress_queue,
                        interrupt_event,
                        progress_end_event,
                        10,
                    ),
                )
                p_process.start()
                file_process = multiprocessing.Process(
                    target=self.__file_traverser,
                    args=(
                        batch_size,
                        data_queue,
                        progress_queue,
                        interrupt_event,
                        end_event,
                    ),
                )
                file_process.start()
                for _ in range(n_proc):
                    uploader = multiprocessing.Process(
                        target=self.__upload_objects,
                        args=(
                            dst_bucket,
                            data_queue,
                            progress_queue,
                            interrupt_event,
                            end_event,
                        ),
                    )
                    processes.append(uploader)
                for proc in processes:
                    proc.start()
                processes.append(file_process)
                for proc in processes:
                    proc.join()
                progress_end_event.set()
                p_process.join()

            except KeyboardInterrupt:
                interrupt_event.set()
                for proc in processes:
                    proc.join()
                p_process.join()

    def sync_from_s3(self, n_proc: int, batch_size: int, **kwargs) -> None:
        """
        Sync remote objects to local FS
        :param int n_proc: Number of download processes to start
        :param int batch_size: Number of items for one go
        """
        processes: list = []
        avg_interval: int = 60
        for k, v in kwargs.items():
            if k == "avg_interval":
                avg_interval = v
        with multiprocessing.managers.SyncManager() as mp_manager:
            check_queue = mp_manager.Queue(maxsize=2)
            data_queue = mp_manager.Queue(maxsize=n_proc * 2)
            progress_queue = mp_manager.Queue()
            end_event = mp_manager.Event()
            interrupt_event = mp_manager.Event()
            progress_end_event = mp_manager.Event()
            try:
                p_reader = multiprocessing.Process(
                    target=self._page_reader,
                    args=(
                        batch_size,
                        check_queue,
                        interrupt_event,
                        end_event,
                    ),
                )
                processes.append(p_reader)
                p_checker = multiprocessing.Process(
                    target=self.__check_local_object,
                    args=(
                        check_queue,
                        data_queue,
                        progress_queue,
                        interrupt_event,
                        end_event,
                    ),
                )
                processes.append(p_checker)
                for _ in range(n_proc):
                    downloader = multiprocessing.Process(
                        target=self.__download_objects,
                        args=(
                            data_queue,
                            progress_queue,
                            interrupt_event,
                            end_event,
                        ),
                    )
                    processes.append(downloader)
                for p in processes:
                    p.start()
                p_progress = multiprocessing.Process(
                    target=self.__download_progress,
                    args=(
                        progress_queue,
                        interrupt_event,
                        progress_end_event,
                        avg_interval,
                    ),
                )
                p_progress.start()
                for p in processes:
                    p.join()
                progress_end_event.set()
                p_progress.join()
            except KeyboardInterrupt:
                interrupt_event.set()
                progress_end_event.set()


class S3Events:
    """
    Class for configuring or showing config of the bucket
    :param str secret_key: Secret key fro s3
    :param str access_key: Access key for s3
    :param str endpoint: S3 endpoint uri
    """

    def __init__(
        self,
        config: S3Config,
        backup_root: Optional[str] = None,
    ) -> None:
        self._download = os.path.join(backup_root, "active") if backup_root else None
        self._remove = os.path.join(backup_root, "deleted") if backup_root else None
        protocol = "https" if config.secure else "http"
        endpoint = f"{protocol}://{config.hostname}"
        self.client_s3 = boto3.client(
            service_name="s3",
            region_name="default",
            endpoint_url=endpoint,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
        )
        self.client_sns = boto3.client(
            service_name="sns",
            region_name="default",
            endpoint_url=endpoint,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            config=botocore.client.Config(signature_version="s3"),
        )
        self.client_admin = RGWAdmin(
            access_key=config.access_key,
            secret_key=config.secret_key,
            server=config.hostname,
            secure=config.secure,
        )
        session = boto3.Session(
            region_name="default",
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
        )
        self.resouce = session.resource(service_name="s3", endpoint_url=endpoint)

    def get_config(self, bucket: str):
        """
        Method to get bucket notification config
        """
        return self.client_s3.get_bucket_notification_configuration(Bucket=bucket)

    def create_bucket(self, bucket: str) -> None:
        """
        Create empty bucket with no configuration
        :param str bucket: Bucket name to create
        :return: None
        """
        self.client_s3.create_bucket(Bucket=bucket)

    def create_topic(
        self,
        config,
        exchange: str,
    ) -> None:
        """
        Create bucket event notification config
        :param str bucket: Bucket name for config update
        :param str amqp: rabbitmq address
        """
        amqp = AMQP_HOST.format(
            user=config.user,
            password=config.password,
            host=config.host,
            port=config.port,
            virtualhost=config.virtualhost,
        )
        attributes = {
            "push-endpoint": amqp,
            "amqp-exchange": exchange,
            "amqp-ack-level": "broker",
            "persistent": "true",
        }
        self.client_sns.create_topic(Name=exchange, Attributes=attributes)

    def create_notification(self, bucket: str, exchange: str) -> None:
        """
        Create buclet notification config
        :param str bucket: Bucket name
        :param str exchange: Exchange name were to send notification
        """
        notification_config = {
            "TopicConfigurations": [
                {
                    "Id": f"s3ben-{exchange}",
                    "TopicArn": TOPIC_ARN.format(exchange),
                    "Events": NOTIFICATION_EVENTS,
                }
            ]
        }
        self.client_s3.put_bucket_notification_configuration(
            Bucket=bucket, NotificationConfiguration=notification_config
        )

    def get_admin_buckets(self) -> list:
        """
        Admin api get buckets
        :return: list
        """
        return self.client_admin.get_buckets()

    def get_bucket(self, bucket: str) -> dict:
        """
        Get bucket info via admin api
        :param str bucket: Bucket name to fetch info
        :return: dictionary with bucket info
        """
        try:
            return self.client_admin.get_bucket(bucket=bucket)
        except rgwadmin.exceptions.NoSuchBucket:
            _logger.warning("Bucket %s not found", bucket)
            sys.exit()

    def __decuple_download(self, data: tuple) -> None:
        bucket, path = data
        self.download_object(bucket, path)

    def download_object(self, bucket: str, path: Union[str, dict]):
        """
        Get an object from a bucket

        :param str bucket: Bucket name from which to get object
        :param str path: object path
        """

        s3_obj = path
        if isinstance(path, dict):
            dst = next(iter(path.values()))
            s3_obj = "/" + next(iter(path.keys()))
            destination = os.path.join(self._download, bucket, dst)
        else:
            destination = os.path.join(self._download, bucket, path)
        dst_dir = os.path.dirname(destination)
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir, exist_ok=True)
        try:
            self.client_s3.head_object(Bucket=bucket, Key=s3_obj)
        except botocore.exceptions.ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                _logger.warning("%s not found in bucket: %s", path, bucket)
                return
        _logger.info("Downloading: %s:%s to %s", bucket, s3_obj, destination)
        self.client_s3.download_file(Bucket=bucket, Key=s3_obj, Filename=destination)

    def download_object_v2(self, bucket: str, s3_obj: str, local_path: str) -> bool:
        """
        Method to download s3 objects and save to local file system

        :param str bucket: Bucket name
        :param str s3_obj: S3 object key to download
        :param str local_path: relative path from backup_root where to save

        :rtype: bool
        """
        try:
            self.client_s3.head_object(Bucket=bucket, Key=s3_obj)
        except botocore.exceptions.ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                _logger.warning("%s:%s object not found", bucket, s3_obj)
                return False
        dst = os.path.join(self._download, bucket, local_path)
        dst_dir = os.path.dirname(dst)
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir, exist_ok=True)
        _logger.info("Downloading %s:%s", bucket, s3_obj)
        self.client_s3.download_file(Bucket=bucket, Key=s3_obj, Filename=dst)
        return True

    def remove_object(self, bucket: str, path: str) -> None:
        """
        Move object to deleted items
        :param str bucket: Bucket eame
        :param str path: object path which should be moved
        :return: None
        """
        _logger.info("Moving %s to deleted items for bucket: %s", path, bucket)
        current_date = datetime.date.today().strftime("%Y-%m-%d")
        dest = os.path.dirname(os.path.join(self._remove, current_date, bucket, path))
        src = os.path.join(self._download, bucket, path)
        file_name = os.path.basename(path)
        d_file = os.path.join(dest, file_name)
        if not os.path.exists(src):
            _logger.warning("%s doesn't exist", src)
            return
        if not os.path.exists(dest):
            os.makedirs(dest)
        if os.path.isfile(d_file):
            _logger.warning(
                "Removing %s as another with same name must be moved to deleted items",
                d_file,
            )
            os.remove(d_file)
        shutil.move(src, dest)

    def download_all_objects(self, bucket_name: str, obj_keys: list) -> None:
        """
        Method for getting all objects from one bucket
        :param str bucket_name: Name of the bucket
        :param str dest: Directory root to append
        :param int threads: Number of threads to start
        :return: None
        """
        threads = 2
        with multiprocessing.pool.ThreadPool(threads) as threads:
            iterate = zip(itertools.repeat(bucket_name), obj_keys)
            threads.map(self.__decuple_download, iterate)

    def _get_all_objects(self, bucket_name) -> list:
        """
        Method to get all objects from the bucket
        :param str bucket_name: Name of the bucket
        :return: List all objects in the bucket
        """
        objects = self.resouce.Bucket(bucket_name).objects.all()
        return [o.key for o in objects]
