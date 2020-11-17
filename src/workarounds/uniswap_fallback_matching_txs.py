import logging
from typing import List

from src.error_definitions import NonExistentUserException
from src.shared.type_definitions import ShareSnap
from src.workarounds.uniswap_matching_txs import UniMatchingTxs


class UniNullUserFallbackMatchingTxs(UniMatchingTxs):

    def __init__(self):
        super().__init__()

    def fetch_new_snaps(self, last_block_update: int, query_limit: int) -> List[ShareSnap]:
        id_query = '''{
            snaps: liquidityPositionSnapshots(first: 1000, orderBy: block, orderDirection: asc, where: {block_gte: $MIN_BLOCK, block_lt: $MAX_BLOCK}) {
                id
            }
        }'''
        params = {
            '$MIN_BLOCK': last_block_update,
            '$MAX_BLOCK': last_block_update + query_limit,
        }
        raw_snap_ids = self.dex_graph.query(id_query, params)['data']['snaps']

        query = '''{
            liquidityPositionSnapshot(id: "$ID") {
                id
                timestamp
                block
                user {
                    id
                }
                pair {
                    id
                    token0 {
                        id
                        symbol
                        name
                    }
                    token1 {
                        id
                        symbol
                        name
                    }
                }
                reserve0
                reserve1
                reserveUSD
                totalSupply: liquidityTokenTotalSupply
                liquidityTokenBalance
            }
        }'''
        logging.info(f'{self.exchange}: Last update block: {last_block_update}')
        txs, _ = self._get_txs(params)
        snaps = []
        for snap_id in raw_snap_ids:
            try:
                raw_snap = self.dex_graph.query(query, {"$ID": snap_id['id']})['data']['liquidityPositionSnapshot']
                snaps.append(self._process_snap(raw_snap, txs))
            except NonExistentUserException:
                logging.error(f'NonExistentUserException - skipping snap with id: {snap_id}')

            # Get snapshots of staked positions
        snaps += self._get_staked_snaps(params)

        if snaps:
            self._populate_eth_prices(snaps)
            self._populate_uni_prices(snaps)

        return snaps
