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
            default="/etc/s3ben.conf",
            type=Path)
    logging = args.add_argument_group(title="Logging options")
    logging.add_argument(
            "--log-level",
            help="Logging level for tool, default: %(default)s",
            default="warning")
    logging.add_argument(
            "--sentry-conf",
            type=Path,
            help="Path to sentry config file")
    return args
