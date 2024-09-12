import os
import configparser
from pathlib import Path
from logging import getLogger

_logger = getLogger(__name__)


def init_sentry(config: Path, section: str = "sentry") -> None:
    """
    Initialize sentry sdk
    :params Path config: Path to config file
    :params str section: Section name in config file, default: sentry
    :raises ValueError: if config file not found
    :raise configparser.NoSectionError: if section not found in provided config file
    :raise configparser.NoOptionError: if dsn or env options not found in config file
    :return: None
    """
    if not os.path.exists(config):
        raise ValueError(f"Config {config} doesn't exists")
    conf = configparser.RawConfigParser()
    with open(file=config) as f:
        conf.readfp(f)
    if not conf.has_section(section):
        raise configparser.NoSectionError(section)
    if not conf.has_option(section=section, option="dsn"):
        raise configparser.NoOptionError(section=section, option="dsn")
    dsn = conf.get(section=section, option="dsn")
    if not conf.has_option(section=section, option="env"):
        raise configparser.NoOptionError(section=section, option="env")
    env = conf.get(section=section, option="env")
    try:
        import sentry_sdk
    except ModuleNotFoundError:
        _logger.warning("sentry_sdk not installed")
    else:
        sentry_sdk.init(dsn=dsn, environment=env)
