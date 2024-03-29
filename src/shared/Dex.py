import logging
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Iterable, Callable

from src.shared.type_definitions import ShareSnap, Exchange, Pool, YieldReward, StakingService
from src.subgraph import SubgraphReader


class Dex(ABC):
    """
    Base class defining the interface for DEXes.
    """

    def __init__(self, dex_graph_name: str, exchange: Exchange, eth_price_first_block=0):
        self.dex_graph = SubgraphReader(dex_graph_name)
        self.exchange = exchange
        self.eth_price_first_block = eth_price_first_block
        self.block_graph = SubgraphReader('blocklytics/ethereum-blocks')
        self.rewards_graph = SubgraphReader('benesjan/dex-rewards-subgraph')

    @abstractmethod
    def fetch_new_snaps(self, last_block_update: int, max_objects_in_batch: int) -> Iterable[List[ShareSnap]]:
        """
        Returns snapshots of user pool shares. A snapshot is created when
        there is change in the user's position.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch_new_staked_snaps(self, last_block_update: int, max_objects_in_batch: int) -> Iterable[List[ShareSnap]]:
        """
        Returns snapshots of user pool shares. A snapshot is created when
        there is change in the user's position.
        """
        raise NotImplementedError()

    def _populate_eth_prices(self, snaps: List[ShareSnap]):
        snaps = [snap for snap in snaps if snap.block >= self.eth_price_first_block]
        if not snaps:
            return
        blocks = {snap.block for snap in snaps}
        eth_prices = self._get_eth_usd_prices(blocks)
        for snap in snaps:
            snap.eth_price = eth_prices[snap.block]

    def _get_eth_usd_prices(self, blocks: Iterable[int]) -> Dict[int, Decimal]:
        """
        Fetch eth prices in specific block times.
        (used to denominate the returns in ETH)
        """
        query = ''.join(self._get_eth_prices_query_generator()(blocks))
        data = self.dex_graph.query(query, {})
        return {int(block[1:]): Decimal(price['price']) for
                block, price in data['data'].items()}

    @abstractmethod
    def _get_eth_prices_query_generator(self) -> Callable[[Iterable[int]], Iterable[str]]:
        """
        Get a generator which is then used to build the request for eth prices
        from thegraph.com.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch_pools(self, max_objects_in_batch: int, min_liquidity: int, skip: int) -> Iterable[List[Pool]]:
        """
        Returns pools at recent block.
        """
        raise NotImplementedError()

    def fetch_yields(self, last_block_update: int, max_objects_in_batch: int) -> Iterable[List[YieldReward]]:
        """
        Returns Yield rewards for a given exchange.
        """
        query = '''{
            rewards(first: $MAX_OBJECTS, skip: $SKIP, orderBy: blockNumber, orderDirection: asc, where: {blockNumber_gte: $BLOCK, exchange: "$EXCHANGE"}) {
                id
                stakingService
                exchange
                pool
                amount
                user
                transaction
                blockNumber
                blockTimestamp
            }
        }'''
        logging.info(f'{self.exchange}: Last update block: {last_block_update}')
        skip = 0
        while True:
            params = {
                '$MAX_OBJECTS': max_objects_in_batch,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
                '$EXCHANGE': self.exchange.name,
            }
            raw_rewards = self.rewards_graph.query(query, params)['data']['rewards']
            if not raw_rewards:
                break

            yield [self._parse_yield(reward) for reward in raw_rewards]
            skip += max_objects_in_batch

    @staticmethod
    def get_highest_indexed_block(graph: SubgraphReader) -> int:
        query = '''
        {
            _meta {
                block {
                    number
                }
            }
        }
        '''
        resp = graph.query(query, {})
        return int(resp['data']['_meta']['block']['number'])

    def _parse_yield(self, reward: Dict) -> YieldReward:
        return YieldReward(
            reward['id'],
            self.exchange,
            reward['user'],
            reward['pool'],
            Decimal(reward['amount']),
            int(reward['blockNumber']),
            int(reward['blockTimestamp']),
            reward['transaction'],
            StakingService[reward['stakingService']]
        )

    def get_block_seconds_ago(self, seconds=86400):
        query = '''{
            blocks(first: 1, orderBy: timestamp, orderDirection: asc, where: {timestamp_gt: $TIMESTAMP}) {
                number
            }
        }'''
        params = {
            '$TIMESTAMP': int(datetime.now().timestamp() - seconds)
        }
        return int(self.block_graph.query(query, params)['data']['blocks'][0]['number'])
