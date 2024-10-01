"""
Module to handle object remapping
"""

import json
import multiprocessing
import os
from logging import getLogger
from queue import Empty

_logger = getLogger(__name__)


class ResolveRemmaping:
    """
    Class to resolve remmapped objects
    :param str backup_root: Path to backup root
    """

    def __init__(self, backup_root: str):
        self._queue = None
        self._remapping_db = os.path.join(backup_root, ".remappings")

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

    def run(self, queue: multiprocessing.Queue, event: multiprocessing.Event) -> None:
        """
        Method to launch Resolver as a process
        :param multiprocess.Queue queue: multiprocess.Queue class for receiving data
        :param multiprocess.Event event: multiprocess.Event class for receiving end event
        :return: None

        for updating remap db, dictionary must be added to queue:
        {
          "action": "update",
          "data": {
            "bucket": "bucket_name"
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
            if data.get("action") == "update":
                remap = data.get("data")
                self.update_remapping(**remap)
            if data.get("action") == "download":
                remap = data.get("data")
                self.update_remapping(**remap)
