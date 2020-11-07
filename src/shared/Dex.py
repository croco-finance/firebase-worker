from abc import ABC, abstractmethod
from decimal import Decimal
from typing import List, Dict, Iterable

from src.shared.type_definitions import ShareSnap, Exchange
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
    def _get_eth_prices_query_generator(self):
        """
        Get a generator which is then used to build the request for eth prices
        from thegraph.com.
        """
        raise NotImplementedError()
