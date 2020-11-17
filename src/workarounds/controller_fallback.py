import logging

from src.controller import Controller
from src.workarounds.uniswap_fallback import UniNullUserFallback


class ControllerUniNullUserFallback(Controller):

    def __init__(self, class_=UniNullUserFallback, snap_index=''):
        super().__init__(class_(), snap_index)

    def update_snaps(self, query_limit):
        logging.info('FALL BACK SNAP UPDATE INITIATED')
        snaps = self.instance.fetch_new_snaps(self.last_update[f'snaps{self.snap_index}'], query_limit)
        if snaps:
            self._upload_snaps(snaps)
