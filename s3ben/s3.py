import datetime
import itertools
import multiprocessing
import os
import shutil
import sys
from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Union

import boto3
import botocore
import botocore.errorfactory
import rgwadmin
from rgwadmin import RGWAdmin

from s3ben.constants import AMQP_HOST, NOTIFICATION_EVENTS, TOPIC_ARN

_logger = getLogger(__name__)


@dataclass
class S3Config:
    """
    Dataclas for s3 configuration
    """

    hostname: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    secure: bool


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
