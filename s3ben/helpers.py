"""
Module for helping functions and classes
"""

import grp
import os
import pwd
import time
from dataclasses import dataclass, field
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


@dataclass
class ProgressMarkers:
    """
    Dataclass for progress markers
    """

    current: list = field(default_factory=lambda: ["-", "\\", "|", "/"])
    done: str = field(default="█")
    filler: str = field(default=".")

    def current_marker(self) -> str:
        """
        Metod to get and rotate current marker
        """
        current_marker = self.current.pop(0)
        self.current.append(current_marker)
        return current_marker


class ProgressSpeed:
    """
    Class to calculate avg and estimate time
    """

    def __init__(self, avg_interval: int) -> None:
        self.__speed_history = [0.0 for _ in range(avg_interval)]
        self.__next_update: int = int(time.perf_counter()) + 1
        self.__current_update: int = 0
        self._speed: float = 0

    @property
    def speed(self) -> float:
        """
        Property to return current speed
        :rtype: int
        """
        return self._speed

    @speed.setter
    def speed(self, value) -> None:
        """
        Method to set current speed
        """
        self.__current_update = int(time.perf_counter())
        self._speed += value
        if self.__current_update >= self.__next_update:
            self.__speed_history.pop(0)
            self.__speed_history.append(self._speed)
            self._speed = 0
            self.__next_update += 1

    @property
    def avg_speed(self) -> float:
        """
        Property to calculate avg speed
        :rtype: float
        """
        return round(sum(self.__speed_history) / len(self.__speed_history), 2)


class ProgressTimings:
    """
    Dataclass to calculate timings for progress
    """

    def __init__(self) -> None:
        self._start: float = time.perf_counter()
        self._current: float = time.perf_counter()
        self._running: float = 0
        self._next_update: int = int(time.perf_counter()) + 1

    @property
    def start(self) -> int:
        """
        Return start time
        :rtype: int
        """
        return int(self._start)

    @property
    def running(self) -> tuple:
        """
        Method to calculate and return running time from start
        :rtype: tuple
        :return: (hours, minuts, seconds)
        """

        hours = int(self._running) // 3600
        minutes = int(self._running) // 60 % 60
        seconds = int(self._running) % 60
        return (hours, minutes, seconds)

    @property
    def current(self) -> int:
        """
        Property to return current running time
        :rtype: int
        """
        return int(self._current)

    @current.setter
    def current(self, value: float) -> None:
        """
        Method to update current time and related variables
        :param float value: time.perf_counter()
        :rtype: None
        """
        self._current = value
        self._running = self._current - self._start

    @property
    def next_update(self) -> int:
        """
        Method to return next update number
        :rtype: int
        """
        return int(self._next_update)

    @next_update.setter
    def next_update(self, value) -> None:
        """
        Setter to update next_update value
        """
        self._next_update += value


# TODO: Split this class to data only and consol decoration
# TODO: Use threads for calculating and painting
class ProgressV2:
    """
    Dataclass to represent progress in console
    """

    def __init__(self, total: int, avg_interval: int) -> None:
        self.times = ProgressTimings()
        self.markers = ProgressMarkers()
        self.speed = ProgressSpeed(avg_interval=avg_interval)
        self._total = total
        self._current: int = 0
        self._download: int = 0
        self.__progress: Union[str, None] = None
        self.__download: Union[str, None] = None
        self.__total: Union[str, None] = None
        self.__running_time: Union[str, None] = None
        self.__estimate_time: Union[str, None] = None
        self.__avg_speed: Union[str, None] = None
        self.__terminal_columns: int = 0
        self.__percents_done: Union[str, None] = None

    @property
    def total(self) -> int:
        """
        Property method for returning total value
        :rtype: int
        """
        return self._total

    @property
    def current(self) -> int:
        """
        Property method to return current value
        :rtype: int
        """
        return self._current

    @property
    def download(self) -> int:
        """
        Property method to return download value
        """
        return self._download

    # @current.setter
    def update_bar(self, value: dict) -> None:
        """
        Method to update current progress value
        """
        dl = 0
        vrf = 0
        if "dl" in value.keys():
            dl = value.pop("dl")
            self._download += dl
        else:
            vrf = value.pop("vrf")
        self._current += vrf + dl
        self.times.current = int(time.perf_counter())
        self.speed.speed = dl + vrf
        if self.times.current >= self.times.next_update:
            self.times.next_update = 1
            self.__update_counters()
            self.__print_line()

    @property
    def estimate_time_left(self) -> Tuple[int, int, int]:
        """
        Method to calculate remaining time
        :rtype: tuple
        :returns: (hours, minutes, seconds)
        """
        try:
            left = int((self.total - self.current) / self.speed.avg_speed)
            hours = left // 3600
            minutes = left // 60 % 60
            seconds = left % 60
            return (hours, minutes, seconds)
        except ZeroDivisionError:
            return (99, 59, 59)

    def __write_progress(self) -> None:
        """
        Method to create progress string with verified
        """
        converted, unit = convert_to_human(self.current)
        if unit:
            self.__progress = f"[V:{converted:>6.2f}{unit}"
            return
        self.__progress = f"[V:{converted:>7}"

    def __write_downloaded(self) -> None:
        """
        Method to create progress string with downloaded info
        """
        converted, unit = convert_to_human(self.download)
        if unit:
            self.__download = f"|D:{converted:>6.2f}{unit}"
            return
        self.__download = f"|D:{converted:>7}"

    def __write_total(self) -> None:
        """
        Method to set total string
        """
        converted, unit = convert_to_human(self.total)
        if unit:
            self.__total = f"|{converted:.2f}{unit}]"
            return
        self.__total = f"|{converted:>}]"

    def __write_running_time(self) -> None:
        """
        Method to create running time string
        """
        h, m, s = self.times.running
        self.__running_time = f"[R:{h:02}:{m:02}:{s:02}]"

    def __write_estimate_time(self) -> None:
        """
        Method to create estimate string
        """
        h, m, s = self.estimate_time_left
        self.__estimate_time = f"[E:{h:02}:{m:02}:{s:02}]"

    def __write_avg_speed(self) -> None:
        """
        Method to format avg speed string
        """
        conveted, unit = convert_to_human(self.speed.avg_speed)
        if unit:
            self.__avg_speed = f"[{conveted:>5.1f}{unit} obj/s]"
            return
        self.__avg_speed = f"[{self.speed.avg_speed:>6.1f} obj/s]"

    def __update_terminal_columns(self) -> None:
        """
        Method to update current terminal columns
        """
        self.__terminal_columns = os.get_terminal_size().columns

    def __write_percents_done(self) -> None:
        """
        Method to calculate and save percents done string
        """
        percents_done = round(self.current * 100 / self.total, 2)
        self.__percents_done = f"[{percents_done:6.2f}%]"

    def __fill_empty_space(self, used_space: int) -> str:
        """
        Method to fill empty line space
        :param int used_space: already used space on terminal
        :rtype: str
        :return: string to fill current line to the end
        """
        available_space = self.__terminal_columns - used_space
        available_space -= 2
        space_done = available_space * self.current // self.total
        space_left = available_space - space_done
        results = "["
        results += self.markers.done * space_done
        in_progress = 0
        if space_left > 0:
            results += self.markers.current_marker()
            in_progress = 1
        results += self.markers.filler * (space_left - in_progress)
        results += "]"
        return results

    def __print_line(self) -> None:
        """
        Print progrss line to console
        """
        self.__update_terminal_columns()
        line = []
        line.append(str(self.__progress))
        line.append(str(self.__download))
        line.append(str(self.__total))
        line.append(str(self.__percents_done))
        line.append(str(self.__avg_speed))
        line.append(str(self.__running_time))
        line.append(str(self.__estimate_time))
        line_length = len("".join(line))
        if line_length < self.__terminal_columns:
            line.insert(3, self.__fill_empty_space(line_length))

        print(f"\r{''.join(line)}", end="", flush=True)

    def __update_counters(self) -> None:
        """
        Method to update counters
        """
        self.__write_progress()
        self.__write_downloaded()
        self.__write_total()
        self.__write_running_time()
        self.__write_estimate_time()
        self.__write_avg_speed()
        self.__write_percents_done()

    def __del__(self) -> None:
        """
        Print end line
        """
        self.__update_counters()
        self.__print_line()
