import multiprocessing
import signal
import time
from logging import getLogger
from queue import Empty

from s3ben.helpers import ProgressBar, drop_privileges
from s3ben.rabbit import RabbitMQ
from s3ben.s3 import S3Events
from s3ben.ui import S3benGui

_logger = getLogger(__name__)


class BackupManager:
    """
    Class to coordinate all tasks

    :param str backup_root: Destination directory were all files will be placed
    :param str user: username to which change privileges
    :param str mq_queue: rabbitmq queue name
    :param RabbitMQ mq: RabbitMQ class object
    """

    def __init__(
        self,
        backup_root: str,
        user: str,
        mq_queue: str = None,
        mq: RabbitMQ = None,
        s3_client: S3Events = None,
        curses: bool = False,
    ):
        self._backup_root = backup_root
        self._user = user
        self._mq = mq
        self._mq_queue = mq_queue
        self._s3_client = s3_client
        self._bucket_name: str = None
        self._page_size: int = None
        self._progress_queue = None
        self._exchange_queue = None
        self._end_event = None
        self._barrier = None
        self._curses = curses
        signal.signal(signal.SIGTERM, self.__exit)
        signal.signal(signal.SIGINT, self.__exit)

    def __exit(self, signal_no, stack_frame) -> None:
        raise SystemExit("Exiting")

    def start_consumer(self, s3_client: S3Events) -> None:
        _logger.debug(f"Dropping privileges to {self._user}")
        drop_privileges(user=self._user)
        try:
            self._mq.consume(queue=self._mq_queue, s3_client=s3_client)
        except KeyboardInterrupt:
            self._mq.stop()
        except SystemExit:
            self._mq.stop()

    def _progress(self) -> None:
        progress = ProgressBar()
        info = self._s3_client.get_bucket(self._bucket_name)
        total_objects = info["usage"]["rgw.main"]["num_objects"]
        progress.total = total_objects
        progress.draw()
        while progress.total > progress.progress:
            try:
                data = self._progress_queue.get(timeout=0.5)
            except Empty:
                progress.draw()
                continue
            else:
                progress.progress += data
                progress.draw()

    def sync_bucket(
        self, bucket_name: str, threads: int, page_size: int = 1000
    ) -> None:
        _logger.info("Starting bucket sync")
        start = time.perf_counter()
        self._page_size = page_size
        self._bucket_name = bucket_name
        proc_manager = multiprocessing.managers.SyncManager()
        proc_manager.start()
        self._exchange_queue = proc_manager.Queue(maxsize=threads * 2)
        self._progress_queue = proc_manager.Queue()
        self._end_event = proc_manager.Event()
        self._barrier = proc_manager.Barrier(threads)
        reader = multiprocessing.Process(target=self._page_reader)
        reader.start()
        processess = []
        for _ in range(threads):
            proc = multiprocessing.Process(target=self._page_processor)
            processess.append(proc)
        for proc in processess:
            proc.start()
        processess.append(reader)
        if not self._curses:
            progress = multiprocessing.Process(target=self._progress)
            progress.start()
            processess.append(progress)
        else:
            ui = multiprocessing.Process(target=self._curses_ui)
            ui.start()
            processess.append(ui)
        for proc in processess:
            proc.join()
        proc_manager.shutdown()
        proc_manager.join()
        end = time.perf_counter()
        _logger.info(f"Sync took: {round(end - start, 2)} seconds")

    def _curses_ui(self) -> None:
        info = self._s3_client.get_bucket(self._bucket_name)
        total_objects = info["usage"]["rgw.main"]["num_objects"]
        ui = S3benGui(title=f"Syncing buclet: {self._bucket_name}", total=total_objects)
        while True:
            try:
                data = self._progress_queue.get(timeout=0.5)
            except Empty:
                ui.progress(progress=ui._progress)
                continue
            else:
                ui.progress(progress=ui._progress + data)

    def _page_reader(self) -> None:
        _logger.info("Starting page processing")
        self._end_event.clear()
        paginator = self._s3_client.client_s3.get_paginator("list_objects_v2")
        page_config = {"PageSize": self._page_size}
        pages = paginator.paginate(
            Bucket=self._bucket_name, PaginationConfig=page_config
        )
        for page in pages:
            self._exchange_queue.put(page["Contents"])
        _logger.debug("Finished reading pages")
        self._end_event.set()

    def _page_processor(self) -> None:
        proc = multiprocessing.current_process().name
        _logger.debug(f"Running: {proc}")
        self._barrier.wait()
        while True:
            try:
                data = self._exchange_queue.get(block=False)
            except Empty:
                if self._end_event.is_set():
                    break
                continue
            else:
                keys = [i.get("Key") for i in data]
                self._s3_client.download_all_objects(self._bucket_name, keys)
                self._progress_queue.put(len(keys))
