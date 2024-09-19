import boto3
import time
import os
import sys
import botocore
import shutil
import datetime
import itertools
import rgwadmin
import multiprocessing
import botocore.errorfactory
from s3ben.constants import TOPIC_ARN, NOTIFICATION_EVENTS, AMQP_HOST
from rgwadmin import RGWAdmin
from logging import getLogger

_logger = getLogger(__name__)


class S3Events():
    """
    Class for configuring or showing config of the bucket
    :param str secret_key: Secret key fro s3
    :param str access_key: Access key for s3
    :param str endpoint: S3 endpoint uri
    """

    def __init__(
            self,
            secret_key: str,
            access_key: str,
            hostname: str,
            secure: bool,
            backup_root: str = None) -> None:
        self._download = os.path.join(backup_root, "active") if backup_root else None
        self._remove = os.path.join(backup_root, "deleted") if backup_root else None
        protocol = "https" if secure else "http"
        endpoint = f"{protocol}://{hostname}"
        self.client_s3 = boto3.client(
                service_name="s3",
                region_name="default",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
                )
        self.client_sns = boto3.client(
                service_name="sns",
                region_name="default",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=botocore.client.Config(signature_version='s3'))
        self.client_admin = RGWAdmin(
                access_key=access_key,
                secret_key=secret_key,
                server=hostname,
                secure=secure)
        self.session = boto3.Session(
                region_name="default",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key)
        self.resouce = self.session.resource(service_name="s3", endpoint_url=endpoint)

    def get_config(self, bucket: str):
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
            mq_host: str,
            mq_user: str,
            mq_password: str,
            exchange: str,
            mq_port: int,
            mq_virtualhost: str) -> None:
        """
        Create bucket event notification config
        :param str bucket: Bucket name for config update
        :param str amqp: rabbitmq address
        """
        amqp = AMQP_HOST.format(user=mq_user, password=mq_password, host=mq_host, port=mq_port, virtualhost=mq_virtualhost)
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
                'TopicConfigurations': [{
                    'Id': f"s3ben-{exchange}",
                    'TopicArn': TOPIC_ARN.format(exchange),
                    'Events': NOTIFICATION_EVENTS
                    }]
                }
        self.client_s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration=notification_config
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
            _logger.error(f"Bucket {bucket} not found")
            sys.exit()

    def download_object(self, bucket: str, path: str):
        """
        Get an object from a bucket

        :param str bucket: Bucket name from which to get object
        :param str path: object path
        """
        destination = os.path.join(self._download, bucket, path)
        dir = os.path.dirname(destination)
        if not os.path.exists(dir):
            os.makedirs(dir)
        _logger.debug(f"bucket: {bucket}, obj: {path}, dest: {destination}")
        try:
            self.client_s3.head_object(Bucket=bucket, Key=path)
        except botocore.exceptions.ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                _logger.warning(f"{path} not found in bucket: {bucket}")
        else:
            _logger.info(f"Downloading {path} from {bucket}")
            self.client_s3.download_file(Bucket=bucket, Key=path, Filename=destination)

    def remove_object(self, bucket: str, path: str) -> None:
        """
        Move object to deleted items
        :param str bucket: Bucket eame
        :param str path: object path which should be moved
        :return: None
        """
        _logger.info(f"Moving {path} to deleted items for bucket: {bucket}")
        current_date = datetime.date.today().strftime("%Y-%m-%d")
        dest = os.path.dirname(os.path.join(self._remove, current_date, bucket, path))
        src = os.path.join(self._download, bucket, path)
        file_name = os.path.basename(path)
        d_file = os.path.join(dest, file_name)
        if not os.path.exists(src):
            _logger.warning(f"{src} doesn't exist")
            return
        if not os.path.exists(dest):
            os.makedirs(dest)
        if os.path.isfile(d_file):
            _logger.warning(f"Removing {d_file} as another with same name must be moved to deleted items")
            os.remove(d_file)
        shutil.move(src, dest)

    def download_all_objects(self, bucket_name: str, dest: str, threads: int) -> None:
        """
        Method for getting all objects from one bucket
        :param str bucket_name: Name of the bucket
        :param str dest: Directory root to append
        :param int threads: Number of threads to start
        :return: None
        """
        _logger.debug(f"Downloading all objects from {bucket_name}")
        all_objects = self._get_all_objects(bucket_name)
        start = time.perf_counter()
        with multiprocessing.pool.ThreadPool(threads) as proc_pool:
            iterate = zip(itertools.repeat(bucket_name), all_objects)
            proc_pool.imap(self._download_object, list(iterate),)
            proc_pool.close()
            proc_pool.join()
        end = time.perf_counter()
        _logger.debug(f"Download took: {round(end - start, 2)}")

    def _download_object(self, input: tuple) -> None:
        """
        Method for downloading objects with multiprocessing threadpool
        :params touple input: Tuple containing bucket_name and object in bucket
        :return: none
        """
        bucket, path = input
        destination = os.path.join(self._download, bucket, path)
        if os.path.isfile(destination):
            _logger.warning(f"File already exists: {destination}")
            return
        dir = os.path.dirname(destination)
        if not os.path.exists(dir):
            os.makedirs(dir)
        _logger.debug(f"bucket: {bucket}, obj: {path}, dest: {destination}")
        try:
            self.client_s3.head_object(Bucket=bucket, Key=path)
        except botocore.exceptions.ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                _logger.warning(f"{path} not found in bucket: {bucket}")
        else:
            _logger.info(f"Downloading {path} from {bucket}")
            self.client_s3.download_file(Bucket=bucket, Key=path, Filename=destination)

    def _get_all_objects(self, bucket_name) -> list:
        """
        Method to get all objects from the bucket
        :param str bucket_name: Name of the bucket
        :return: List all objects in the bucket
        """
        objects = self.resouce.Bucket(bucket_name).objects.all()
        return [o.key for o in objects]

    def _create_local_dir_tree(self, paths: list, bucket_name: str) -> None:
        """
        Method to replicate local directory tree from provided list
        :params list paths: List of paths from which we should create directory tree
        :param str bucket_name: Name of the bucket to be included in path
        """
        _logger.debug("Creating local directory tree")
        uniq_dirs = {os.path.dirname(dir) for dir in paths}
        for dir in uniq_dirs:
            full_path = os.path.join(self._download, bucket_name, dir)
            if os.path.isdir(full_path):
                continue
            _logger.debug(f"Creating: {full_path}")
            os.makedirs(full_path)

    def _check_if_object_exists(self, path):
        pass
