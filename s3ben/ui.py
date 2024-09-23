import curses
import math
import time
from datetime import datetime
from logging import getLogger

_logger = getLogger(__name__)


class S3benGui:
    """
    Class to decorate terminal for s3ben some commands
    """

    _run_format = "RUNING: {0:0>2}:{1:0>2}:{2:0>2}"
    _eta_format = "ETA: {0:0>2}:{1:0>2}:{2:0>2}"
    marker = "█"
    _border = ["|", "|", "-", "-", "+", "+", "+", "+"]

    def __init__(self, total: int, title: str) -> None:
        self._title = title
        self.__init_curses()
        self._total = total
        self._progress = 0
        self._speed = None
        self._start_time = time.perf_counter()
        self._eta_cols = 15
        self._progress_cols = self._cols - self._eta_cols - 1
        self._percents_cols = 9
        self._running_cols = 18
        self._progress_win = curses.newwin(1, self._progress_cols, 2, 1)
        self._percents = curses.newwin(
            1, self._percents_cols, 1, self._progress_cols - self._running_cols
        )
        self._running = curses.newwin(
            1, self._running_cols, 1, self._cols - self._running_cols - 1
        )
        self._eta = curses.newwin(1, self._eta_cols, 2, self._cols - self._eta_cols - 1)

    def __init_curses(self) -> None:
        _logger.debug("Initializing curses")
        self._screen = curses.initscr()
        curses.noecho()
        curses.cbreak()
        curses.curs_set(False)
        self._screen.keypad(True)
        self._lines = curses.LINES
        self._cols = curses.COLS
        self._screen.border(*self._border)
        self._screen.addstr(1, 1, self._title)
        self._screen.refresh()
        self.__init_percentage()

    def __init_percentage(self) -> None:
        pass

    def __del__(self) -> None:
        curses.endwin()
        _logger.debug("Terminal restored")

    def _calculate_fill(self, iteration: int) -> int:
        """
        Method to callculate progress bar fill
        :param int itaration: itaration number
        :return: Int
        """
        if iteration == self._total - 1:
            return self._progress_cols - 2
        if iteration % 2 == 0:
            return math.floor(self._progress_cols / self._total * iteration)
        return math.ceil(self._progress_cols / self._total * iteration)

    def _calaculate_percents(self, iteration: int) -> float:
        """
        Calculate completion percentage
        :param int iteration: current iteration
        :return: float
        """
        return (iteration + 1) / self._total * 100

    def _calculate_running_time(self) -> tuple:
        hours = math.floor(self._running_time / 3600)
        minutes = math.floor(self._running_time / 60) - (hours * 60)
        seconds = math.floor(self._running_time - (hours * 60) - (minutes * 60))
        return (hours, minutes, seconds)

    def _estimate_left(self) -> tuple:
        self._speed = self._progress // self._running_time
        left = self._total - self._progress
        try:
            estimate_seconds = left // self._speed
        except ZeroDivisionError:
            return (0, 0, 0)
        else:
            hours = math.floor(estimate_seconds / 3600)
            minutes = math.floor(estimate_seconds / 60) - (hours * 60)
            seconds = math.floor(estimate_seconds - (hours * 60) - (minutes * 60))
            return (hours, minutes, seconds)

    def __progress_bar(self) -> None:
        fill = self._calculate_fill(self._progress)
        self._progress_win.clear()
        self._progress_win.addstr(0, 0, self.marker * fill)
        self._progress_win.refresh()

    def __percentage(self) -> None:
        percents = self._calaculate_percents(self._progress)
        self._percents.clear()
        # self._percents.border(*self._border)
        self._percents.addstr(0, 0, f"{percents:06.2f}%")
        self._percents.refresh()

    def _running_time_win(self) -> None:
        running_time = self._calculate_running_time()
        self._running.clear()
        # self._running.border(*self._border)
        self._running.addstr(
            0,
            0,
            self._run_format.format(running_time[0], running_time[1], running_time[2]),
        )
        self._running.refresh()

    def __estimate_time_left(self) -> None:
        eta = self._estimate_left()
        self._eta.clear()
        # self._eta.border(*self._border)
        self._eta.addstr(0, 0, self._eta_format.format(*eta))
        self._eta.refresh()

    def progress(self, progress: int) -> None:
        self._progress = progress
        self._running_time = time.perf_counter() - self._start_time
        self._progress = progress
        self.__progress_bar()
        self.__percentage()
        self._running_time_win()
        self.__estimate_time_left()
