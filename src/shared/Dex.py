from abc import ABC, abstractmethod
from typing import List

from src.shared.type_definitions import ShareSnap
from src.subgraph import SubgraphReader


class Dex(ABC):
    """
    Base class defining the interface for DEXes.
    """

    def __init__(self, dex_graph_url: str):
        self.dex_graph = SubgraphReader(dex_graph_url)
        self.block_graph = SubgraphReader(
            '/subgraphs/name/blocklytics/ethereum-blocks')
        self.rewards_graph = SubgraphReader(
            '/subgraphs/name/benesjan/dex-rewards-subgraph')

    @abstractmethod
    def fetch_new_snaps(self, last_block_update: int, current_block: int) -> List[ShareSnap]:
        """
        Returns snapshots of user pool shares. A snapshot is created when
        there is change in the user's position.
        """
        raise NotImplementedError()

    @abstractmethod
    def _set_frontend_rewards(self, address: str, snaps: List[ShareSnap], reward_range=7):
        raise NotImplementedError()