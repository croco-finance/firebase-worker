from typing import List

from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap


class Controller:
    def update(self, instance: Dex):
        last_block = 11211034
        prev_lowest, prev_highest = 1000000000, 0
        for snaps in instance.fetch_new_snaps(last_block, query_limit=50):
            assert len(snaps) < 900, 'Reached dangerous amount of snaps in a batch' \
                                     '-> not all snaps might fit into the response for this reason' \
                                     '-> DECREASE QUERY LIMIT!'
            lowest, highest = self._get_lowest_highest_block(snaps)
            print(f'Lowest block: {lowest}, highest block: {highest}')
            assert prev_highest <= lowest, f'Blocks not properly sorted: ' \
                                           f'prev_highest: {prev_highest}, lowest: {lowest}'
            prev_lowest, prev_highest = lowest, highest
            self._upload_snaps(snaps)

    def _upload_snaps(self, snaps: List[ShareSnap]):
        print(f"Uploading {len(snaps)} snaps")

    @staticmethod
    def _get_lowest_highest_block(snaps: List[ShareSnap]):
        lowest_, highest_ = 1000000000, 0
        for snap in snaps:
            if snap.block > highest_:
                highest_ = snap.block
            if snap.block < lowest_:
                lowest_ = snap.block
        return lowest_, highest_
