import logging

from src.controller import Controller
from src.workarounds.uniswap_fallback import UniNullUserFallback


class ControllerUniNullUserFallback(Controller):

    def __init__(self):
        super().__init__(UniNullUserFallback())

    def update_snaps(self, query_limit):
        logging.info('FALL BACK SNAP UPDATE INITIATED')
        instance: UniNullUserFallback = self.instance
        snaps = instance.fetch_new_snaps(self.last_update['snaps'], query_limit)
        if snaps:
            self._upload_snaps(snaps)
