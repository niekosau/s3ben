import os
import sys
import pwd
import grp
from typing import List
from datetime import datetime
from s3ben.constants import UNITS


def drop_privileges(user: str) -> None:
    """
    Drop user privileges.

    :params str user: Username to which we should change permissions
    """
    new_user = pwd.getpwnam(user)
    if new_user.pw_uid == os.getuid():
        return
    new_gids = [new_user.pw_gid]
    new_gids += [group.gr_gid for group in grp.getgrall() if new_user.pw_name in group.gr_mem]
    os.setgroups(new_gids[:os.NGROUPS_MAX])
    os.setgid(new_user[0])
    os.setuid(new_user.pw_uid)
    os.environ["HOME"] = new_user.pw_dir


def list_split(input: list, size: int) -> List[list]:
    return [input[i:i + size] for i in range(0, len(input), size)]


def convert_to_human(value: int) -> tuple:
    if float(value) <= 1000.0:
        return value, None
    for unit in UNITS:
        value /= 1000
        if float(value) < 1000.0:
            break
        if unit == UNITS[-1]:
            break
    return value, unit


class ProgressBar():
    """
    Progress bar class
    """

    show_suffix: bool = False
    show_preffix: bool = False
    show_bar: bool = True
    _speed = "[SPEED: {0: ^7.2f}{1:<3}]"
    _percents = "[{0:>6.2f}%]"
    _time_left = "[LEFT: {0:0>2}:{1:0>2}:{2:0>2}]"
    _running = "[RUN: {0:0>2}:{1:0>2}:{2:0>2}]"
    _progress = "[{:{done_marker}>{done_size}}{}{:{base_marker}>{left_size}}]"
    _total_progress = "[{0: ^8.2f}{1:<3}]"
    _completed: float = 0.00
    current_marker: list = ["-", "\\", "|", "/"]
    filler_marker: str = " "
    bar_length: int = 0
    bar_size: int = 0
    _preffix: str = ""
    _suffix: str = ""
    _done_marker: str = "#"
    _total: float = 100.00

    def __init__(
                self,
                show_percents: bool = True,
                show_estimate: bool = True,
                show_runtime: bool = True,
                show_speed: bool = False,
                show_transfer: bool = True
            ):
        self.terminal_size: int = os.get_terminal_size().columns
        self.percents: str = self._percents.format(0)
        self.show_percents: bool = show_percents
        self.show_numbers: bool = False
        self.time_start = datetime.now()
        self.show_estimate: bool = show_estimate
        self.time_left = self._time_left.format(99, 59, 59)
        self.run_time = self._running.format(0, 0, 0)
        self._run_time = datetime.now()
        self.speed = self._speed.format(0, "b")
        self.total_progress = self._total_progress.format(0, "b")
        self.show_runtime: bool = show_runtime
        self.show_transfer: bool = show_transfer
        self.avg_speed = 0
        self.show_speed = show_speed

    def __update_stats(self) -> None:
        self.__run_time()
        self.__update_avg_speed()
        if self.show_percents:
            self.__update_percent_done()
        if self.show_estimate:
            self.__calculate_estimate()
        if self.show_runtime:
            self.__update_run_time()
        if self.show_speed:
            self.__update_speed()
        if self.show_transfer:
            self.__total_progress()
        self.__update_terminal_size()

    def __get_current_marker(self) -> str:
        self.current_marker.append(self.current_marker.pop(0))
        return self.current_marker[-1]

    def __update_speed(self) -> None:
        speed, units = convert_to_human(self.avg_speed)
        self.speed = self._speed.format(speed, units)

    def __total_progress(self) -> None:
        progress, units = convert_to_human(self.progress)
        if not units:
            units = " "
        self.total_progress = self._total_progress.format(progress, units)

    def __run_time(self) -> None:
        self._run_time = datetime.now() - self.time_start

    def __update_terminal_size(self) -> None:
        self.terminal_size = os.get_terminal_size().columns

    def __split_time(self, seconds: int) -> list:
        result = []
        hours = int(seconds // 3600)
        minutes = int((seconds // 60) - (hours * 60))
        seconds = int(seconds % 60)
        result.append(hours if hours > 0 else 0)
        result.append(minutes if minutes > 0 else 0)
        result.append(seconds if minutes >= 0 else 0)
        return result

    def __update_run_time(self) -> None:
        time = self.__split_time(self._run_time.total_seconds())
        self.run_time = self._running.format(time[0], time[1], time[2])

    def __update_avg_speed(self) -> None:
        self.avg_speed = self.progress // self._run_time.total_seconds()

    def __calculate_estimate(self) -> None:
        run_data_left = self.total - self.progress
        try:
            run_estimate = run_data_left // self.avg_speed
            time = self.__split_time(run_estimate)
            self.time_left = self._time_left.format(time[0], time[1], time[2])
        except ZeroDivisionError:
            self.time_left = self._time_left.format(99, 59, 59)

    def __update_percent_done(self) -> None:
        percents = float(self.progress * 100 / self.total)
        self.percents = self._percents.format(percents)

    def __format_bar(self) -> str:
        bar = [""]
        bar_info = 0
        finished = int(self.bar_size * self.progress / self.total)
        if self.show_preffix:
            bar.append(self.preffix)
            bar_info += len(self.preffix)
        if self.show_transfer:
            bar.append(self.total_progress)
            bar_info += len(self.total_progress)
        if self.show_bar:
            bar.append("")
            bar_index = len(bar) - 1
        if self.show_percents:
            bar.append(self.percents)
            bar_info += len(self.percents)
        if self.show_runtime:
            bar.append(self.run_time)
            bar_info += len(self.run_time)
        if self.show_estimate:
            bar.append(self.time_left)
            bar_info += len(self.time_left)
        if self.show_speed:
            bar.append(self.speed)
            bar_info += len(self.speed)
        if self.show_suffix:
            bar.append(self.suffix)
            bar_info += len(self.suffix)
        if bar_index:
            if self.terminal_size > bar_info:
                self.bar_size = self.terminal_size - bar_info - 3
                left_size = self.bar_size - finished
                bar[bar_index] = self._progress.format(
                        self._done_marker if finished > 1 else "",
                        self.__get_current_marker() if left_size > 0 else "",
                        self.filler_marker if left_size > 0 else "",
                        done_marker=self._done_marker,
                        done_size=finished if left_size > 0 else self.bar_size,
                        in_progress_marker=self.filler_marker,
                        base_marker=self.filler_marker,
                        left_size=left_size if left_size > 0 else 0
                    )
            else:
                bar.pop(bar_index)
        if self.bar_size <= finished:
            self.current_marker = ""
        bar.append("\r")
        line = "".join(bar)
        return line

    @property
    def prefix(self) -> str:
        return self._preffix

    @prefix.setter
    def prefix(self, preffix: str) -> None:
        """
        Change bar prefix
        """
        self.show_preffix = True
        self._preffix = preffix

    @property
    def suffix(self) -> str:
        return self._suffix

    @suffix.setter
    def suffix(self, suffix: str) -> None:
        """
        Change bar suffix
        """
        self.show_suffix = True
        self._suffix = suffix

    @property
    def progress(self) -> float:
        return self._completed

    @progress.setter
    def progress(self, progress: float) -> None:
        """
        Update progress done percentage
        """
        if float(progress):
            self._completed = float(progress)
        else:
            raise ValueError("Progress must be int or float")

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
        bar = self.__format_bar()
        sys.stdout.write(bar)
        sys.stdout.flush()

    def __del__(self) -> None:
        sys.stdout.write("\n")
        sys.stdout.flush()
