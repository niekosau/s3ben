DEFAULT_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LOG_FORMAT_DEBUG = "[%(asctime)s][%(levelname)-8s]: %(message)s"
DEFAULT_LOG_FORMAT = "%(message)s"
MANDATORY_CONFIG_KEYS = [
        "s3_endpoint",
        "aws_secret_key",
        "aws_access_key"]
TOPIC_ARN = "arn:aws:sns:default::{0}"
NOTIFICATION_EVENTS = [
        's3:ObjectCreated:*',
        's3:ObjectRemoved:*',
        's3:ObjectLifecycle:Expiration:*']
AMQP_HOST = "amqp://{user}:{password}@{host}:{port}"
