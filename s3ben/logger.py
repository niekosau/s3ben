import logging
from s3ben.constants import DEFAULT_LOG_FORMAT, DEFAULT_LOG_FORMAT_DEBUG, DEFAULT_LOG_DATE_FORMAT


def init_logger(
        name: str,
        level: str = "warning",
        log_format: str = None,
        date_format: str = DEFAULT_LOG_DATE_FORMAT) -> None:
    """
    Function to initialize logger and all needed parts

    :param str name: logger name to setup
    :param str level: Logging level
    :raises ValueError: If log level doens't exist
    :return: None
    """
    if not logging._checkLevel(level.upper()):
        raise ValueError(f"Log level {level} doesn't exist")
    set_level = logging.getLevelName(level.upper())
    if not log_format:
        log_format = DEFAULT_LOG_FORMAT_DEBUG if level.lower() == "debug" else DEFAULT_LOG_FORMAT
    set_format = logging.Formatter(log_format, datefmt=date_format)
    logger = logging.getLogger(name)
    logger.setLevel(set_level)
    console = logging.StreamHandler()
    console.setFormatter(set_format)
    console.setLevel(set_level)
    logger.addHandler(console)
