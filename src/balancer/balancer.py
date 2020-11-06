from decimal import Decimal
from typing import List, Dict

from src.balancer.queries import share_query_generator
from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, CurrencyField, PoolToken


class Balancer(Dex):
    """
    A handler for Balancer DEX.
    """

    def __init__(self):
        super().__init__('/subgraphs/name/balancer-labs/balancer')

    def fetch_new_snaps(self, last_block_update: int, max_objects=200) -> List[ShareSnap]:
        reached_joins_last, reached_exits_last = False, False
        skip, snaps = 0, []
        while not reached_joins_last or not reached_exits_last:
            params = {
                '$MAX_OBJECTS': max_objects,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
            }
            txs = self._get_txs(params)
            if not txs:
                return []
            query = ''.join(share_query_generator(txs))
            raw_snaps = self.dex_graph.query(query, {})['data']
            snaps += self._get_snaps_from_txs(raw_snaps)

        return snaps

    def _get_txs(self, params: Dict) -> List[Dict]:
        query = '''
        {
            transactions(first: $MAX_OBJECTS, skip: $SKIP, where: {block_gt: $BLOCK, event_in:["join", "exit"]}) {
                tx
                block
                timestamp
                poolAddress {
                    id
                }
                userAddress {
                    id
                }
                event
            }
        }
        '''
        data = self.dex_graph.query(query, params)['data']
        # There is a bug in the subgraph which returns txs twice
        # I'll filter them in the following for loop
        tx_hashes, filtered_data = set(), []
        for tx in data['transactions']:
            if tx['tx'] not in tx_hashes:
                tx_hashes.add(tx['tx'])
                filtered_data.append(tx)
        return filtered_data

    def _get_snaps_from_txs(self, txs: Dict[str, List[Dict]]) -> List[ShareSnap]:
        snaps = []
        for key, value in txs.items():
            assert len(value) == 1, 'Incorrect number of pool shares in ' \
                                    f'a list: {value}'
            tx_, block_, timestamp_ = key.split('_')
            snaps.append({
                'tx': tx_[1:],
                'tx_type': tx_type,
                'block': int(block_),
                'date': datetime.utcfromtimestamp(int(timestamp_)),
                **self._parse_pool_share(value[0])
            })
        return snaps
