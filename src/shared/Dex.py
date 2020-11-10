import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import List, Dict, Iterable, Callable

from src.shared.type_definitions import ShareSnap, Exchange, Pool, YieldReward
from src.subgraph import SubgraphReader


class Dex(ABC):
    """
    Base class defining the interface for DEXes.
    """

    def __init__(self, dex_graph_url: str, exchange: Exchange):
        self.dex_graph = SubgraphReader(dex_graph_url)
        self.exchange = exchange
        self.block_graph = SubgraphReader(
            '/subgraphs/name/blocklytics/ethereum-blocks')
        self.rewards_graph = SubgraphReader(
            '/subgraphs/name/benesjan/dex-rewards-subgraph')

    @abstractmethod
    def fetch_new_snaps(self, last_block_update: int, query_limit: int) -> Iterable[List[ShareSnap]]:
        """
        Returns snapshots of user pool shares. A snapshot is created when
        there is change in the user's position.
        """
        raise NotImplementedError()

    def _populate_eth_prices(self, snaps: List[ShareSnap]):
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
    def fetch_pools(self, max_objects_in_batch: int) -> Iterable[List[Pool]]:
        """
        Returns pools at recent block.
        """
        raise NotImplementedError()

    def fetch_yields(self, last_block_update: int, max_objects_in_batch: int) -> Iterable[List[YieldReward]]:
        """
        Returns Yield rewards for a given exchange.
        """
        query = '''{
            rewards(first: $MAX_OBJECTS, skip: $SKIP, orderBy: blockNumber, orderDirection: asc, where: {blockNumber_gt: $BLOCK, exchange: "$EXCHANGE"}) {
                id
                exchange
                pool
                amount
                user
                transaction
                blockNumber
                blockTimestamp
            }
        }'''
        first_block, current_block = last_block_update, self._get_current_block()
        logging.info(f'{self.exchange}: Last update block: {last_block_update}, current block: {current_block}')
        skip = 0
        while first_block < current_block:
            params = {
                '$MAX_OBJECTS': max_objects_in_batch,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
                '$EXCHANGE': self.exchange.name,
            }
            raw_rewards = self.rewards_graph.query(query, params)['data']['rewards']
            yield [self._parse_yield(reward) for reward in raw_rewards]
            skip += max_objects_in_batch

    def _get_current_block(self) -> int:
        query = '''
        {
            currentBlock(id: "CURRENT"){
                number
            }
        }
        '''
        resp = self.rewards_graph.query(query, {})
        # Set current block info on current positions
        return int(resp['data']['currentBlock']['number'])

    def _parse_yield(self, reward: Dict) -> YieldReward:
        return YieldReward(
            reward['id'],
            self.exchange,
            reward['user'],
            reward['pool'],
            Decimal(reward['amount']),
            int(reward['blockNumber']),
            int(reward['blockTimestamp']),
            reward['transaction']
        )
