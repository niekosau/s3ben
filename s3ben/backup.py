import hashlib
import multiprocessing
import os
import shutil
import time
from datetime import datetime, timedelta
from logging import getLogger
from queue import Empty

from tabulate import tabulate

from s3ben.helpers import (
    ProgressBar,
    check_object,
    convert_to_human,
    convert_to_human_v2,
    drop_privileges,
)
from s3ben.rabbit import RabbitMQ
from s3ben.remapper import ResolveRemmaping
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
        self._download_queue = None
        self._verify_queue = None
        self._remapped_queue = None
        self._end_event = None
        self._barrier = None
        self._curses = curses

    def start_consumer(
        self,
        s3_client: S3Events,
        mp_data_queue: multiprocessing.Queue,
        mp_end_event: multiprocessing.Event,
    ) -> None:
        """
        Method to run rabbit mq consume
        """
        _logger.debug("Dropping privileges to %s", self._user)
        drop_privileges(user=self._user)
        try:
            self._mq.consume_v2(
                queue=self._mq_queue,
                s3_client=s3_client,
                mp_data_queue=mp_data_queue,
                mp_end_event=mp_end_event,
            )
        except KeyboardInterrupt:
            _logger.debug("Stopping mq")
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
                progress.progress = data
                progress.draw()

    def sync_bucket(
        self,
        bucket_name: str,
        threads: int,
        page_size: int,
        checkers: int,
        skip_checksum: bool,
        skip_filesize: bool,
    ) -> None:
        _logger.info("Starting bucket sync")
        start = time.perf_counter()
        self._page_size = page_size
        self._bucket_name = bucket_name
        proc_manager = multiprocessing.managers.SyncManager()
        proc_manager.start()
        self._download_queue = proc_manager.Queue(maxsize=threads * 2)
        self._verify_queue = proc_manager.Queue(maxsize=threads * 2)
        self._progress_queue = proc_manager.Queue()
        self._remapped_queue = proc_manager.Queue()
        self._end_event = proc_manager.Event()
        self._barrier = proc_manager.Barrier(threads + checkers)
        try:
            reader = multiprocessing.Process(target=self._page_reader)
            reader.start()
            remapper = multiprocessing.Process(target=self.__remapper)
            remapper.start()
            processess = []
            for _ in range(checkers):
                verify = multiprocessing.Process(
                    target=self._page_verfication,
                    args=(
                        skip_checksum,
                        skip_filesize,
                    ),
                )
                processess.append(verify)
            for _ in range(threads):
                download = multiprocessing.Process(target=self._page_processor)
                processess.append(download)
            for proc in processess:
                proc.start()
            processess.append(reader)
            processess.append(remapper)
            if not self._curses:
                self._progress()
            else:
                self._curses_ui()
            for proc in processess:
                proc.join()
        finally:
            proc_manager.shutdown()
        end = time.perf_counter()
        _logger.info("Sync took: %s seconds", round(end - start, 2))

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

    def __remapper(self) -> None:
        """
        Method to run remmaper class
        :return: None
        """
        remap_resolver = ResolveRemmaping(backup_root=self._backup_root)
        remap_resolver.run(queue=self._remapped_queue, event=self._end_event)

    def _page_reader(self) -> None:
        _logger.info("Starting page processing")
        self._end_event.clear()
        paginator = self._s3_client.client_s3.get_paginator("list_objects_v2")
        page_config = {"PageSize": self._page_size}
        pages = paginator.paginate(
            Bucket=self._bucket_name, PaginationConfig=page_config
        )
        for page in pages:
            self._verify_queue.put(page["Contents"])
        _logger.debug("Finished reading pages")
        self._end_event.set()

    def _page_verfication(self, skip_checksum: bool, skip_filesize: bool) -> None:
        """
        Method to verify file by comparing size and checksum
        :returns: None
        """
        _logger.debug("Running page verification")
        self._barrier.wait()
        while True:
            try:
                data = self._verify_queue.get(block=False)
            except Empty:
                if self._end_event.is_set():
                    _logger.debug("Braking from queue")
                    break
                continue
            download_list = []
            for obj in data:
                obj_key = obj.get("Key")
                key = check_object(obj_key)
                if isinstance(key, dict):
                    remapping_update = {"action": "update"}
                    local_path = next(iter(key.values()))
                    remapping_update.update(
                        {"data": {"bucket": self._bucket_name, "remap": key}}
                    )
                    self._remapped_queue.put(remapping_update)
                else:
                    local_path = key
                fp_key = os.path.join(
                    self._backup_root, "active", self._bucket_name, local_path
                )
                ob_key_exists = self.__check_file(path=fp_key)
                if not ob_key_exists:
                    _logger.debug("file doesn't exists: %s", fp_key)
                    download_list.append(key)
                    continue
                if not skip_checksum:
                    obj_sum = obj.get("ETag")
                    obj_sum_matches = self.__check_md5(path=fp_key, md5=obj_sum)
                    if not obj_sum_matches:
                        download_list.append(key)
                        continue
                if not skip_filesize:
                    obj_size = obj.get("Size")
                    obj_size_matches = self.__check_file_size(
                        path=fp_key, size=obj_size
                    )
                    if not obj_size_matches:
                        download_list.append(key)
                        continue
            skipped = len(data) - len(download_list)
            progress_update = {"skipped": skipped}
            self._progress_queue.put(progress_update)
            if len(download_list) > 0:
                self._download_queue.put(download_list)

    def __check_file_size(self, path: str, size: int) -> bool:
        """
        Method to check if file size matches
        :param str path: full path to the file
        :param int size: size of remote object to verify
        :return: True if matches, otherwise False
        """
        _logger.debug("Checking file size %s", path)
        local_size = os.stat(path=path).st_size
        if local_size == size:
            return True
        return False

    def __check_file(self, path: str) -> bool:
        """
        Method to check if local file exists
        :return: True if file exisrts, otherwise false
        """
        _logger.debug("Checking if file exists: %s", path)
        if os.path.isfile(path=path):
            return True
        return False

    def __check_md5(self, path: str, md5: str) -> bool:
        """
        Method to calculate file md5 sum and check if it matches
        :param str path: full path to the local file
        :param str md5: md5 to check against
        :return: True if sum maches, otherwise false
        """
        _logger.debug("Checking md5sum for %s", path)
        with open(path, "rb") as file:
            source_md5 = hashlib.md5()
            while chunk := file.read(8192):
                source_md5.update(chunk)
        calculated_md5 = source_md5.hexdigest()
        if calculated_md5 == md5.replace('"', ""):
            return True
        return False

    def _page_processor(self) -> None:
        proc = multiprocessing.current_process().name
        _logger.debug("Running: %s", proc)
        self._barrier.wait()
        while True:
            try:
                data = self._download_queue.get(block=True, timeout=0.2)
            except Empty:
                if self._end_event.is_set():
                    break
                continue
            else:
                self._s3_client.download_all_objects(self._bucket_name, data)
                progress_update = {"downloaded": len(data)}
                self._progress_queue.put(progress_update)

    def list_buckets(
        self,
        exclude: list,
        show_excludes: bool,
        show_obsolete: bool,
        only_enabled: bool,
        sort: str,
        sort_revers: bool,
    ) -> None:
        results = []
        s3_buckets = self._s3_client.get_admin_buckets()
        s3ben_buckets = os.listdir(os.path.join(self._backup_root, "active"))
        merged_list = list(dict.fromkeys(s3_buckets + s3ben_buckets))
        for bucket in merged_list:
            bucket_excluded = True if bucket in exclude else ""
            enabled = True if bucket in s3ben_buckets else ""
            obsolete = True if bucket not in s3_buckets else ""
            if not show_excludes and bucket_excluded:
                continue
            if not show_obsolete and obsolete:
                continue
            if only_enabled and not enabled:
                continue
            remote_size = 0
            objects = 0
            unit = ""
            if bucket in s3_buckets:
                bucket_info = self._s3_client.get_bucket(bucket=bucket)
                if "rgw.main" in bucket_info["usage"].keys():
                    original_size = bucket_info["usage"]["rgw.main"].get(
                        "size_utilized"
                    )
                    remote_size = convert_to_human_v2(original_size)
                    original_objects = bucket_info["usage"]["rgw.main"].get(
                        "num_objects"
                    )
                    objects, unit = convert_to_human(original_objects)
            remote_format = ">3d" if isinstance(objects, int) else ">5.2f"
            info = {
                "Bucket": bucket,
                "Owner": bucket_info.get("owner"),
            }
            if not only_enabled:
                info["Enabled"] = enabled
            info.update(
                {
                    "size": original_size,
                    "Remote size": remote_size,
                    "objects": original_objects,
                    "Remote objects": f"{objects:{remote_format}}{unit}",
                }
            )
            if show_excludes and not only_enabled:
                info["Exclude"] = bucket_excluded

            if show_obsolete and not only_enabled:
                info["Obsolete"] = obsolete
            results.append(info)
        if sort == "bucket":
            results = sorted(results, key=lambda k: k["Bucket"], reverse=sort_revers)
        if sort == "owner":
            results = sorted(results, key=lambda k: k["Owner"], reverse=sort_revers)
        if sort == "size":
            results = sorted(results, key=lambda k: k["size"], reverse=sort_revers)
        if sort == "objects":
            results = sorted(results, key=lambda k: k["objects"], reverse=sort_revers)
        for r in results:
            r.pop("size")
            r.pop("objects")
        print(tabulate(results, headers="keys"))

    def cleanup_deleted_items(self, days: int) -> None:
        """
        Method to cleanup deleted items
        :param int days: Days to keep deleted items
        :return: None
        """
        deleted_path = os.path.join(self._backup_root, "deleted")
        date_to_remove = datetime.now() - timedelta(days=days)
        _, dirs, _ = next(os.walk(deleted_path))
        for dir in dirs:
            dir_date = datetime.strptime(dir, "%Y-%m-%d")
            if dir_date > date_to_remove:
                continue
            to_remove = os.path.join(deleted_path, dir)
            _logger.debug("Removing %s", to_remove)
            shutil.rmtree(path=to_remove)
        _logger.debug("Cleanup done")
