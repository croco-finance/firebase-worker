from typing import List, Optional

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, YieldReward, Pool, StakingService, PoolDayData


class Controller:
    def __init__(self, instance: Dex, logger, snap_index=''):
        self.instance = instance
        self.logger = logger
        self.snap_index = snap_index
        self.exchange_name = str(instance.exchange.name)
        if not firebase_admin._apps:
            cred = credentials.Certificate('serviceAccountKey.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://croco-finance-a02aa.firebaseio.com/'
                # 'databaseURL': 'https://croco-finance.firebaseio.com/'
            })
        self.root_ref = db.reference('/')
        self.last_update_ref = self.root_ref.child('lastUpdate').child(self.exchange_name)
        self.last_update = self.last_update_ref.get()

    def update_snaps(self, max_objects_in_batch):
        self.logger.info('SNAP UPDATE INITIATED')
        prev_lowest, prev_highest = 1000000000, 0
        for snaps in self.instance.fetch_new_snaps(self.last_update[f'snaps{self.snap_index}'], max_objects_in_batch):
            if snaps:
                lowest, highest = self._get_lowest_highest_block(snaps)
                self.logger.info(f'Lowest block: {lowest}, highest block: {highest}')
                assert prev_highest <= lowest, f'Blocks not properly sorted: ' \
                                               f'prev_highest: {prev_highest}, lowest: {lowest}'
                prev_lowest, prev_highest = lowest, highest
                self._upload_snaps(snaps)

    def update_staked_snaps(self, max_objects_in_batch, staking_service: Optional[StakingService] = None):
        self.logger.info('STAKED SNAP UPDATE INITIATED')
        prev_lowest, prev_highest = 1000000000, 0
        for snaps in self.instance.fetch_new_staked_snaps(self.last_update['stakedSnaps'], max_objects_in_batch,
                                                          staking_service=staking_service):
            if snaps:
                lowest, highest = self._get_lowest_highest_block(snaps)
                self.logger.info(f'Lowest block: {lowest}, highest block: {highest}')
                assert prev_highest <= lowest, f'Blocks not properly sorted: ' \
                                               f'prev_highest: {prev_highest}, lowest: {lowest}'
                prev_lowest, prev_highest = lowest, highest
                self._upload_snaps(snaps, staked=True)

    def _upload_snaps(self, snaps: List[ShareSnap], staked=False):
        snapPath = 'stakedSnaps' if staked else f'snaps{self.snap_index}'
        self.logger.info(f'Uploading {len(snaps)} {"staked " if staked else ""}snaps')
        highest_block = self.last_update[snapPath]
        for snap in snaps:
            snap_ref = self.root_ref.child(f'users/{snap.user_addr}/{self.exchange_name}'
                                           f'/snaps/{snap.pool_id}/{snap.id}')
            snap_ref.set(snap.to_serializable())
            if snap.block > highest_block:
                highest_block = snap.block
        self.last_update_ref.child(snapPath).set(highest_block)
        self.last_update[snapPath] = highest_block
        self.logger.info(f'Updated highest snap firebase block to {highest_block}')

    @staticmethod
    def _get_lowest_highest_block(vals):
        lowest_, highest_ = 1000000000, 0
        for snap in vals:
            if snap.block > highest_:
                highest_ = snap.block
            if snap.block < lowest_:
                lowest_ = snap.block
        return lowest_, highest_

    def update_yields(self, max_objects_in_batch):
        self.logger.info('YIELD UPDATE INITIATED')
        prev_lowest, prev_highest = 1000000000, 0
        for yields in self.instance.fetch_yields(self.last_update['yields'], max_objects_in_batch):
            if yields:
                lowest, highest = self._get_lowest_highest_block(yields)
                self.logger.info(f'Lowest block: {lowest}, highest block: {highest}')
                assert prev_highest <= lowest, f'Blocks not properly sorted: ' \
                                               f'prev_highest: {prev_highest}, lowest: {lowest}'
                prev_lowest, prev_highest = lowest, highest
                self._upload_yields(yields)

    def _upload_yields(self, yields: List[YieldReward]):
        self.logger.info(f"Uploading {len(yields)} yields")
        highest_block = self.last_update['yields']
        for yield_ in yields:
            yield_ref = self.root_ref.child(f'users/{yield_.user_addr}/{self.exchange_name}/yields/{yield_.id}')
            yield_ref.set(yield_.to_serializable())
            if yield_.block > highest_block:
                highest_block = yield_.block
        self.last_update_ref.child('yields').set(highest_block)
        self.last_update['yields'] = highest_block
        self.logger.info(f'Updated highest yields firebase block to {highest_block}')

    def update_pools(self, max_objects_in_batch, min_liquidity=100000):
        self.logger.info('POOL UPDATE INITIATED')
        for pools in self.instance.fetch_pools(max_objects_in_batch, min_liquidity):
            if pools:
                self._upload_pools(pools)

    def _upload_pools(self, pools: List[Pool]):
        self.logger.info(f"Uploading {len(pools)} pools")
        for pool in pools:
            pool_ref = self.root_ref.child(f'pools/{pool.id}')
            pool_ref.set(pool.to_serializable())

    def update_pool_day_data(self, max_objects_in_batch, min_liquidity=100000):
        self.logger.info('POOL DAY DATA UPDATE INITIATED')
        for data in self.instance.get_pool_day_data(max_objects_in_batch, min_liquidity):
            if data:
                self._upload_pool_day_data(data)

    def _upload_pool_day_data(self, data: List[PoolDayData]):
        self.logger.info(f"Uploading {len(data)} pool day data")
        for daily in data:
            pool_ref = self.root_ref.child(f'daily/{daily.pool_id}/{daily.timestamp}')
            pool_ref.set(daily.to_serializable())
