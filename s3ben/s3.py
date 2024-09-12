import boto3
import json
from botocore.client import Config
from s3ben.constants import TOPIC_ARN, NOTIFICATION_EVENTS, AMQP_HOST
from rgwadmin import RGWAdmin
from logging import getLogger

_logger = getLogger(__name__)


class BucketNotificationConfig():
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
            secure: bool) -> None:
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
                config=Config(signature_version='s3'))
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

    def create_notification(self, bucket: str, name: str) -> None:
        """
        Create buclet notification config
        :param str bucket: Bucket name
        """
        notification_config = {
                'TopicConfigurations': [{
                    'Id': name,
                    'TopicArn': TOPIC_ARN.format(name),
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


def setup_buckets(
        hostname: str,
        access_key: str,
        secret_key: str,
        exclude: list,
        mq_host: str,
        mq_user: str,
        mq_password: str,
        mq_exchange: str,
        mq_routing_key: str,
        mq_port: int,
        secure: bool
        ) -> None:
    add_config = BucketNotificationConfig(
            hostname=hostname,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure)
    buckets = add_config.get_admin_buckets()
    exclude = exclude.split(",")
    exclude = [i.strip() for i in exclude]
    _logger.debug("Creating topic")
    add_config.create_topic(
            mq_host=mq_host,
            mq_user=mq_user,
            mq_password=mq_password,
            exchange=mq_exchange,
            mq_port=mq_port)
    for bucket in list(set(buckets) - set(exclude)):
        _logger.debug(f"Adding bucket config to: {bucket}")
        add_config.create_notification(bucket=bucket, name=mq_exchange)
