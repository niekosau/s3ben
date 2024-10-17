"""
Module to collect and visualise progress
"""

import multiprocessing
import os
import threading
import time
from dataclasses import dataclass, field
from logging import getLogger
from multiprocessing.synchronize import Event as EventClass
from typing import Tuple

from typing_extensions import TypeAlias

from s3ben.helpers import convert_to_human

_logger = getLogger(__name__)
Queue: TypeAlias = multiprocessing.Queue
Event: TypeAlias = threading.Event


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
        self._speed += value

    @property
    def avg_speed(self) -> float:
        """
        Property to calculate avg speed
        :rtype: float
        """
        return round(sum(self.__speed_history) / len(self.__speed_history), 2)

    def update_speed_history(self) -> None:
        """
        Update speed history
        """
        self.__speed_history.pop(0)
        self.__speed_history.append(self._speed)
        self._speed = 0


class ProgressTimings:
    """
    Dataclass to calculate timings for progress
    """

    def __init__(self) -> None:
        self._start: float = time.perf_counter()
        self._current: float = time.perf_counter()

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
        self._current = time.perf_counter()
        running = self.current - self.start
        hours = int(running) // 3600
        minutes = int(running) // 60 % 60
        seconds = int(running) % 60
        return (hours, minutes, seconds)

    @property
    def current(self) -> int:
        """
        Property to return current running time
        :rtype: int
        """
        return int(self._current)


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

    @property
    def total(self) -> int:
        """
        Property method for returning total value
        :rtype: int
        """
        return self._total

    @total.setter
    def total(self, value: int) -> None:
        """
        Method to updat total value
        :param int value: new total value
        """
        self._total = value

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

    @property
    def estimate(self) -> Tuple[int, int, int]:
        """
        Calculate estimate time left
        """
        try:
            left = (self.total - self.current) / self.speed.avg_speed
        except ZeroDivisionError:
            left = 0
        h = int(left) // 3600
        m = int(left) // 60 % 60
        s = int(left) % 60
        return h, m, s

    @property
    def percents_done(self) -> float:
        """
        Property to calculate and return percents done
        """
        try:
            return round(self.current * 100 / self.total, 2)
        except ZeroDivisionError:
            return 0

    def update_counters(self, value: dict) -> None:
        """
        Method to update current progress value
        """
        dl = 0
        vrf = 0
        if "dl" in value.keys():
            dl = value.pop("dl")
            self._download += dl
        elif "vrf" in value.keys():
            vrf = value.pop("vrf")
        elif "total" in value.keys():
            self.total += value.pop("total")
        self._current += vrf + dl
        self.speed.speed = dl + vrf


class ConsoleBar:
    """
    Class used to decorate console
    """

    def __init__(self, total: int, avg_interval: int, exit_event_queue: Event) -> None:
        self.data = ProgressV2(total=total, avg_interval=avg_interval)
        t, u = convert_to_human(self.data.total)
        self.__terminal_columns: int = os.get_terminal_size().columns
        self.__total = f"T {t:6.2f}{u}" if u else f"T {t:7}"
        self.__progress: str = f"[V {0:7}|D {0:7}|{self.__total}]"
        self.__time: str = f"[R {0:02}:{0:02}:{0:02}][E {99:02}:{59:02}:{59:02}]"
        self.__percents: str = f"[{0:6.2f}%]"
        self.__avg_speed: str = f"[{0:>6.1f} obj/s]"
        self.__exit_event: Event = exit_event_queue
        self.__print_bar()

    def __del__(self) -> None:
        """
        Draw final bar
        """
        self.__print_bar(end="\n")

    def __print_bar(self, end: str = "") -> None:
        """
        Print progress bar to console
        """
        self.__write_time()
        self.__write_avg_speed()
        self.__write_percents_done()
        self.__write_preffix()
        self.__write_total()
        p_bar = []
        p_bar.append(self.__progress)
        p_bar.append(self.__percents)
        p_bar.append(self.__avg_speed)
        p_bar.append(self.__time)
        p_bar_length = len("".join(p_bar))
        if p_bar_length < self.__terminal_columns:
            p_bar.insert(1, self.__fill_empty_space(p_bar_length))
        print(f"\r{''.join(p_bar)}", end=end)

    def __fill_empty_space(self, used_space: int) -> str:
        """
        Method to fill empty line space
        :param int used_space: already used space on terminal
        :rtype: str
        :return: string to fill current line to the end
        """
        available_space = self.__terminal_columns - used_space
        available_space -= 4
        try:
            space_done = available_space * self.data.current // self.data.total
        except ZeroDivisionError:
            space_done = 0
        space_left = available_space - space_done
        results = "["
        results += self.data.markers.done * space_done
        in_progress = 0
        if space_left > 0:
            results += self.data.markers.current_marker()
            in_progress = 1
        results += self.data.markers.filler * (space_left - in_progress)
        results += "]"
        return results

    def __write_total(self) -> None:
        """
        Method to update total part
        """
        t, u = convert_to_human(self.data.total)
        self.__total = f"T {t:6.2f}{u}" if u else f"T {t:7}"

    def __write_time(self) -> None:
        """
        Method to create running time string
        """
        r = self.data.times.running
        self.data.speed.update_speed_history()
        e = self.data.estimate
        self.__time = (
            f"[R {r[0]:02}:{r[1]:02}:{r[2]:02}][E {e[0]:02}:{e[1]:02}:{e[2]:02}]"
        )

    def __write_avg_speed(self) -> None:
        """
        Method to update avg speed
        """
        avg, u = convert_to_human(self.data.speed.avg_speed)
        if u:
            self.__avg_speed = f"[{avg:>5.1f}{u} obj/s]"
        else:
            self.__avg_speed = f"[{avg:>6.1f} obj/s]"

    def __write_percents_done(self) -> None:
        """
        Method to update percents done part
        """
        done = self.data.percents_done
        self.__percents = f"[{done:6.2f}%]"

    def __write_preffix(self) -> None:
        """
        Method to write preffix
        """
        down, d_u = convert_to_human(self.data.download)
        current, c_u = convert_to_human(self.data.current)
        if c_u:
            c_string = f"[V {current:>6.2f}{c_u}"
        else:
            c_string = f"[V {current:>7}"
        if d_u:
            d_string = f"|D {down:>6.2f}{d_u}|{self.__total}]"
        else:
            d_string = f"|D {down:>7}|{self.__total}]"
        self.__progress = c_string + d_string

    def draw_bar(self, update_interval: int = 1) -> None:
        """
        Method to draw bar in another thread
        """
        while True:
            try:
                self.__exit_event.wait(update_interval - 0.001)
            except EOFError:
                break
            self.__print_bar()
            if self.__exit_event.is_set():
                break
