import argparse
from pathlib import Path


def base_args() -> argparse.ArgumentParser:
    """
    Base argparse function
    :return: dict
    """
    args = argparse.ArgumentParser()
    args.add_argument(
            "--config",
            help="Base config path, default: %(default)s",
            default="/etc/s3ben/s3ben.ini",
            type=Path)
    logging = args.add_argument_group(title="Logging options")
    logging.add_argument(
            "--log-level",
            help="Logging level for tool, default: %(default)s",
            default="warning")
    logging.add_argument(
            "--sentry-conf",
            type=Path,
            default="/etc/s3ben/sentry.ini",
            help="Path to sentry config file, default: %(default)s")
    return args
