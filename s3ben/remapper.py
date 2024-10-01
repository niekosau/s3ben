"""
Module to handle object remapping
"""

import json
import multiprocessing
import os
from datetime import datetime
from logging import getLogger
from multiprocessing.synchronize import Event as EventClass
from queue import Empty

from typing_extensions import TypeAlias

_logger = getLogger(__name__)


class ResolveRemmaping:
    """
    Class to resolve remmapped objects
    :param str backup_root: Path to backup root
    """

    Queue: TypeAlias = multiprocessing.Queue
    Event: TypeAlias = EventClass

    def __init__(self, backup_root: str):
        self._queue = None
        self._remapping_db = os.path.join(backup_root, ".remappings")
        self._remappings_deleted = os.path.join(backup_root, ".deleted-remappings")

    def update_remapping(self, bucket: str, remap: dict) -> None:
        """
        Method to update remapping database
        :param str bucket: bucket name for which remap should be added
        :peram dict remap: dictionary containing remapping information
        :return: None
        """
        b_remaps = {}
        if not os.path.exists(self._remapping_db):
            _logger.warning("Remapping db doesn't exists, creating")
            update = {bucket: remap}
            with open(file=self._remapping_db, mode="w", encoding="utf-8") as f:
                json.dump(update, f)
                return
        with open(file=self._remapping_db, mode="r", encoding="utf-8") as f:
            remappings: dict = json.load(f)
        if bucket in remappings.keys():
            b_remaps = remappings.pop(bucket)
        b_remaps.update(remap)
        remappings.update({bucket: b_remaps})
        with open(file=self._remapping_db, mode="w", encoding="utf-8") as f:
            json.dump(obj=remappings, fp=f)

    def move_remapping(self, bucket: str, remap: dict) -> None:
        """
        Method to update remmaping database and remove deleted objects
        """
        if not os.path.exists(self._remapping_db):
            _logger.warning("Remapping database doesn't exists")
            return

        current_date = datetime.now().strftime("%Y-%m-%d")
        remapped_file = next(iter(remap.values()))
        remapped_key = next(iter(remap.keys()))
        _logger.debug("Moving %s to deleted items", remapped_file)

        with open(file=self._remapping_db, mode="r", encoding="utf-8") as f:
            remappings: dict = json.load(f)
        if remapped_key in remappings[bucket].keys():
            remappings[bucket].pop(remapped_key)
        else:
            _logger.warning("%s not found in remapping db", remapped_key)

        with open(file=self._remapping_db, mode="w", encoding="utf-8") as f:
            json.dump(obj=remappings, fp=f)
        if not os.path.exists(self._remappings_deleted):
            moved_dict = {
                current_date: {bucket: remap},
            }
            with open(file=self._remappings_deleted, mode="w", encoding="utf-8") as f:
                json.dump(moved_dict, f)
                return
        with open(file=self._remappings_deleted, mode="r", encoding="utf-8") as f:
            moved_remappings = json.load(f)
        moved_remappings[current_date][bucket].update(remap)
        with open(file=self._remappings_deleted, mode="w", encoding="utf-8") as f:
            json.dump(moved_remappings, f)

    def run(self, queue: Queue, event: Event) -> None:
        """
        Method to launch Resolver as a process
        :param Queue queue: multiprocess.Queue class for receiving data
        :param Event event: multiprocess.Event class for receiving end event
        :return: None

        for updating remap db, dictionary must be added to queue:
        {
          "action": "update",
          "data": {
            "bucket": "bucket_name",
            "remap": {
              "object_key_to_match_local_file": "relative/path_to_key"
            }
          }
        }
        """
        _logger.info("starting remapping resolver")
        while not event.is_set():
            try:
                data: dict = queue.get(timeout=1)
            except Empty:
                if event.is_set():
                    _logger.debug("End event received")
                    break
                continue
            bucket = data["data"].get("bucket")
            remap = data["data"].get("remap")
            if data.get("action") in ["update", "download"]:
                self.update_remapping(bucket=bucket, remap=remap)
            if data.get("action") == "remove":
                remap = data.get("data")
                self.move_remapping(bucket=bucket, remap=remap)
