import boto3
import os
import botocore
import shutil
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
            mq_port: int) -> None:
        """
        Create bucket event notification config
        :param str bucket: Bucket name for config update
        :param str amqp: rabbitmq address
        """
        amqp = AMQP_HOST.format(user=mq_user, password=mq_password, host=mq_host, port=mq_port)
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

    def download_object(self, bucket: str, path: str):
        """
        Get an object from a bucket

        :param str bucket: Bucket name from which to get object
        :param str path: object path
        """
        _logger.info(f"Downloading {path} from {bucket}")
        destination = os.path.join(self._download, bucket, path)
        dir = os.path.dirname(destination)
        if not os.path.exists(dir):
            os.makedirs(dir)
        _logger.debug(f"bucket: {bucket}, obj: {path}, dest: {destination}")
        self.client_s3.download_file(Bucket=bucket, Key=path, Filename=destination)

    def remove_object(self, path: str) -> None:
        _logger.debug(f"Moving {path} to deleted items")
        dest = os.path.dirname(os.path.join(self._remove, path))
        src = os.path.join(self._download, path)
        if not os.path.exists(dest):
            os.makedirs(dest)
        shutil.move(src, dest)
