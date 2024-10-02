import grp
import math
import os
import pwd
import sys
import time
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
    os.setgid(new_user[0])
    os.setuid(new_user.pw_uid)
    os.environ["HOME"] = new_user.pw_dir


def convert_to_human_v2(value: int):
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


def convert_to_human(value: int) -> tuple:
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
    remapping_update.update({"data": {"bucket": bucket, "remap": remap}})
    local_path = next(iter(remap.values()))
    object_path = "/" + next(iter(remap.keys()))
    return object_path, local_path, remapping_update


class ProgressBar:
    """
    Progress bar class
    """

    _skipped: int = 0
    _percents = "[{0:>6.2f}%]"
    _time_left = "[LEFT: {0:0>2}:{1:0>2}:{2:0>2}]"
    _running = "[RUN: {0:0>2}:{1:0>2}:{2:0>2}]"
    _progress = "[{:{done_marker}>{done_size}}{}{:{base_marker}>{left_size}}]"
    _completed: float = 0
    _download: float = 0
    current_marker: list = ["-", "\\", "|", "/"]
    filler_marker: str = "."
    bar_length: int = 0
    bar_size: int = 0
    _preffix: str = ""
    _suffix: str = ""
    _done_marker: str = "█"
    _total: float = 100.00

    def __init__(
        self,
    ):
        self.terminal_size: int = os.get_terminal_size().columns
        self.percents: str = self._percents.format(0)
        self.show_numbers: bool = False
        self.time_start = int(time.perf_counter())
        self.time_left = self._time_left.format(99, 59, 59)
        self.run_time = self._running.format(0, 0, 0)
        self._run_time = int(time.perf_counter())
        self.avg_speed = 0

    def __update_stats(self) -> None:
        self.__run_time()
        self.__update_avg_speed()
        self.__update_percent_done()
        self.__calculate_estimate()
        self.__update_run_time()
        self.__update_terminal_size()

    def __get_current_marker(self) -> str:
        marker = self.current_marker.pop(0)
        self.current_marker.append(marker)
        return self.current_marker[-1]

    def __run_time(self) -> None:
        self._run_time = int(time.perf_counter()) - self.time_start

    def __update_terminal_size(self) -> None:
        self.terminal_size = os.get_terminal_size().columns

    def __split_time(self, seconds: int) -> tuple:
        hours = math.floor(seconds / 3600)
        minutes = math.floor(seconds / 60) - (hours * 60)
        seconds = math.floor(seconds - ((hours * 3600) + (minutes * 60)))
        return (hours, minutes, seconds)

    def __update_run_time(self) -> None:
        r_time = self.__split_time(self._run_time)
        self.run_time = self._running.format(r_time[0], r_time[1], r_time[2])

    def __update_avg_speed(self) -> None:
        try:
            self.avg_speed = self.progress / self._run_time
        except ZeroDivisionError:
            self.avg_speed = 0

    def __calculate_estimate(self) -> None:
        run_data_left = self.total - self.progress
        try:
            run_estimate = run_data_left // self.avg_speed
            e_time = self.__split_time(run_estimate)
            self.time_left = self._time_left.format(e_time[0], e_time[1], e_time[2])
        except ZeroDivisionError:
            self.time_left = self._time_left.format(99, 59, 59)

    def __update_percent_done(self) -> None:
        percents = float(self.progress * 100 / self.total)
        self.percents = self._percents.format(percents)

    def __format_skipped(self) -> str:
        """
        Format skipped progress bar

        :return: String representing skipped files part
        """

        skipped, s_units = convert_to_human(self._skipped)
        _skip_format_int = "[S:{0:>7}"
        _skip_format_float = "[S:{0:>6.2f}{1:1}"

        if self._skipped <= 1000:
            return _skip_format_int.format(skipped)
        return _skip_format_float.format(skipped, s_units)

    def __format_downloaded(self) -> str:
        """
        Method to format download files

        :return: String representing download part of progress
        """

        downloaded, d_units = convert_to_human(self._download)
        _download_format_int = "|DL:{0:7}"
        _download_format_float = "|DL:{0:6.2f}{1:1}"

        if self._download <= 1000:
            return _download_format_int.format(downloaded)
        return _download_format_float.format(downloaded, d_units)

    def __format_total(self) -> str:
        """
        Format total part for pregress bar
        :returns: String
        """
        results = []
        total, t_units = convert_to_human(self.total)
        progress, units = convert_to_human(self.progress)
        _progress_format_int = "|{0:>7}"
        _progress_format_float = "|{0:>6.2f}{1:<1}"
        _total_format_int = "/{0:<4}]"
        _total_format_float = "/{0:<.2f}{1:<1}]"
        if self.progress <= 1000:
            results.append(_progress_format_int.format(progress))
        else:
            results.append(_progress_format_float.format(progress, units))
        if self._total <= 1000:
            results.append(_total_format_int.format(total))
        else:
            results.append(_total_format_float.format(total, t_units))
        return "".join(results)

    def __format_bar(self) -> str:
        p_bar = [""]
        bar_info = 0
        bar_index = 0
        finished = int(self.bar_size * self.progress / self.total)
        skipped = self.__format_skipped()
        download = self.__format_downloaded()
        total = self.__format_total()
        p_bar.append(skipped)
        bar_info += len(skipped)
        p_bar.append(download)
        bar_info += len(download)
        p_bar.append(total)
        bar_info += len(total)
        p_bar.append("")
        bar_index = len(p_bar) - 1
        p_bar.append(self.percents)
        bar_info += len(self.percents)
        p_bar.append(self.run_time)
        bar_info += len(self.run_time)
        p_bar.append(self.time_left)
        bar_info += len(self.time_left)
        if bar_index:
            if self.terminal_size > bar_info:
                self.bar_size = self.terminal_size - bar_info - 3
                left_size = self.bar_size - finished
                p_bar[bar_index] = self._progress.format(
                    self._done_marker if finished > 1 else "",
                    self.__get_current_marker() if left_size > 0 else "",
                    self.filler_marker if left_size > 0 else "",
                    done_marker=self._done_marker,
                    done_size=finished if left_size > 0 else self.bar_size,
                    in_progress_marker=self.filler_marker,
                    base_marker=self.filler_marker,
                    left_size=left_size if left_size > 0 else 0,
                )
            else:
                p_bar.pop(bar_index)
        if self.bar_size <= finished:
            self.current_marker = ""
        p_bar.append("\r")
        line = "".join(p_bar)
        return line

    @property
    def progress(self) -> float:
        return self._completed

    @progress.setter
    def progress(self, data: dict) -> None:
        """
        Update progress done percentage
        """
        if "skipped" in data.keys():
            skipped = data.get("skipped")
            self._skipped += skipped
            self._completed += skipped
        if "downloaded" in data.keys():
            download = data.get("downloaded")
            self._download += download
            self._completed += download

    @property
    def total(self):
        """
        Return total value of progress calculation
        """
        return self._total

    @total.setter
    def total(self, total) -> None:
        """
        Set progress bar total value
        """
        self._total = total

    def draw(self) -> None:
        """
        Draw a progress bar
        """
        self.__update_stats()
        sys.stdout.write(self.__format_bar())
        sys.stdout.flush()

    def __del__(self) -> None:
        sys.stdout.write("\n")
        sys.stdout.flush()
