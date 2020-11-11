import logging
from typing import List

from src.error_definitions import NonExistentUserException
from src.shared.type_definitions import ShareSnap
from src.uniswap_v2.uniswap import Uniswap


class UniNullUserFallback(Uniswap):

    def __init__(self):
        super().__init__()

    def fetch_new_snaps(self, last_block_update: int, query_limit: int) -> List[ShareSnap]:
        id_query = '''{
            snaps: liquidityPositionSnapshots(first: 1000, orderBy: block, orderDirection: asc, where: {block_gte: $MIN_BLOCK, block_lt: $MAX_BLOCK}) {
                id
            }
        }'''
        first_block, current_block = last_block_update, self._get_current_block()
        last_block = first_block + query_limit
        params = {
            '$MIN_BLOCK': first_block,
            '$MAX_BLOCK': last_block,
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
                tx
                gasUsed
                gasPrice
            }
        }'''
        logging.info(f'{self.exchange}: Last update block: {last_block_update}, current block: {current_block}')
        snaps = []
        for snap_id in raw_snap_ids:
            try:
                raw_snap = self.dex_graph.query(query, {"$ID": snap_id['id']})['data']['liquidityPositionSnapshot']
                snaps.append(self._process_snap(raw_snap))
            except NonExistentUserException:
                logging.error(f'NonExistentUserException - skipping snap with id: {snap_id}')

            # Get snapshots of staked positions
        snaps += self._get_staked_snaps(params)

        merged_snaps = self._merge_corresponding_snaps(snaps)
        if merged_snaps:
            self._populate_eth_prices(merged_snaps)
            self._populate_uni_prices(merged_snaps)

        return merged_snaps
