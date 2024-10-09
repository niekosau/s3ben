"""
Module for helping functions and classes
"""

import grp
import os
import pwd
from logging import getLogger
from typing import Tuple, Union

from s3ben.constants import SIZE_UNITS, UNITS

_logger = getLogger(__name__)


def drop_privileges(user: str) -> None:
    """
    Drop user privileges.

    :params str user: Username to which we should change permissions
    """
    new_user = pwd.getpwnam(user)
    if new_user.pw_uid == os.getuid():
        return
    new_gids = [new_user.pw_gid]
    new_gids += [
        group.gr_gid for group in grp.getgrall() if new_user.pw_name in group.gr_mem
    ]
    os.setgroups(new_gids[: os.NGROUPS_MAX])
    os.setgid(new_user.pw_uid)
    os.setuid(new_user.pw_uid)
    os.environ["HOME"] = new_user.pw_dir


def convert_to_human_v2(value: Union[int, float]) -> str:
    """
    Convert size to human
    """
    suffix = "B"
    for unit in SIZE_UNITS:
        if abs(value) < 1024.0:
            break
        if unit == UNITS[-1]:
            break
        value /= 1024.0
    return f"{value:3.2f}{unit}{suffix}"


def convert_to_human(value: Union[int, float]) -> tuple:
    """
    Onother convert function, should be renamed or consolidated
    """
    if float(value) <= 1000.0:
        return value, ""
    for unit in UNITS:
        value /= 1000.0
        if float(value) < 1000.0:
            break
        if unit == UNITS[-1]:
            break
    return value, unit


def check_object(path) -> Union[str, dict]:
    """
    Check object path for forward slash and replace
    if found as first character
    :param str path: Object path from s3
    """
    if path[0] == "/":
        remmpaed_obj = "_forward_slash_" + path
        _logger.warning(
            "Forward slash found for object: %s, remmaping to: %s", path, remmpaed_obj
        )
        return {path[1:]: remmpaed_obj}
    return path


def remmaping_message(action: str, remap: dict, bucket: str) -> Tuple[str, str, dict]:
    """
    Function to create remmaping message

    :param str action: Update or delete action
    :param dict remap: Remmaping data
    :param str bucket: Name of the bucket
    """
    remapping_update = {"action": action}
    remapping_update.update({"data": {"bucket": bucket, "remap": remap}})  # type: ignore
    local_path = next(iter(remap.values()))
    object_path = "/" + next(iter(remap.keys()))
    return object_path, local_path, remapping_update
