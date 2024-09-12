from argparse import ArgumentParser
from typing import List


def argument(*name_or_flags, **kwargs):
    return (list(name_or_flags), kwargs)


def command(args: List[argument] = [], parent: ArgumentParser = None, cmd_aliases: List[str] = None):
    """
    Decorator for argument parser
    :param ArgumentParser parent: parent for arguments
    :param list[argument] args: unknow
    :param list[str] cmd_aliases: aliases for cli option
    """
    if cmd_aliases is None:
        cmd_aliases = []

    def decorator(func):
        parser = parent.add_parser(
            func.__name__.replace("_", "-"),
            description=func.__doc__,
            aliases=cmd_aliases
        )
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)
    return decorator
