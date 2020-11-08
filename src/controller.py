import logging
from typing import List

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, YieldReward


class Controller:
    def __init__(self, instance: Dex):
        self.instance = instance
        self.exchange_name = str(instance.exchange.name)
        if not firebase_admin._apps:
            cred = credentials.Certificate('serviceAccountKey.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://croco-finance.firebaseio.com/'
            })
        self.root_ref = db.reference('/')
        self.last_update_ref = self.root_ref.child('lastUpdate').child(self.exchange_name)
        self.last_update = self.last_update_ref.get()

    def update_snaps(self):
        prev_lowest, prev_highest = 1000000000, 0
        for snaps in self.instance.fetch_new_snaps(self.last_update['snaps'], query_limit=50):
            if snaps:
                assert len(snaps) < 900, 'Reached dangerous amount of snaps in a batch' \
                                         '-> not all snaps might fit into the response for this reason' \
                                         '-> DECREASE QUERY LIMIT!'
                lowest, highest = self._get_lowest_highest_block(snaps)
                logging.info(f'Lowest block: {lowest}, highest block: {highest}')
                assert prev_highest <= lowest, f'Blocks not properly sorted: ' \
                                               f'prev_highest: {prev_highest}, lowest: {lowest}'
                prev_lowest, prev_highest = lowest, highest
                self._upload_snaps(snaps)

    def _upload_snaps(self, snaps: List[ShareSnap]):
        logging.info(f"Uploading {len(snaps)} snaps")
        highest_block = self.last_update['snaps']
        for snap in snaps:
            snap_ref = self.root_ref.child(f'users/{snap.user_addr}/{self.exchange_name}'
                                           f'/snaps/{snap.pool_id}/{snap.id}')
            snap_ref.set(snap.to_serializable())
            if snap.block > highest_block:
                highest_block = snap.block
        self.last_update_ref.child('snaps').set(highest_block - 1)
        self.last_update['snaps'] = highest_block - 1

    @staticmethod
    def _get_lowest_highest_block(vals):
        lowest_, highest_ = 1000000000, 0
        for snap in vals:
            if snap.block > highest_:
                highest_ = snap.block
            if snap.block < lowest_:
                lowest_ = snap.block
        return lowest_, highest_

    def update_yields(self):
        prev_lowest, prev_highest = 1000000000, 0
        for rewards in self.instance.fetch_yields(self.last_update['yields'], 50):
            if rewards:
                assert len(rewards) < 900, 'Reached dangerous amount of rewards in a batch' \
                                           '-> not all snaps might fit into the response for this reason' \
                                           '-> DECREASE QUERY LIMIT!'
                lowest, highest = self._get_lowest_highest_block(rewards)
                logging.info(f'Lowest block: {lowest}, highest block: {highest}')
                assert prev_highest <= lowest, f'Blocks not properly sorted: ' \
                                               f'prev_highest: {prev_highest}, lowest: {lowest}'
                prev_lowest, prev_highest = lowest, highest
                self._upload_yields(rewards)

    def _upload_yields(self, yields: List[YieldReward]):
        logging.info(f"Uploading {len(yields)} yields")
        highest_block = self.last_update['yields']
        for yield_ in yields:
            yield_ref = self.root_ref.child(f'users/{yield_.user_addr}/{self.exchange_name}/yields/{yield_.id}')
            yield_ref.set(yield_.to_serializable())
            if yield_.block > highest_block:
                highest_block = yield_.block
        self.last_update_ref.child('yields').set(highest_block - 1)
        self.last_update['yields'] = highest_block - 1
