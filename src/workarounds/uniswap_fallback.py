import logging
from typing import List

from src.error_definitions import NonExistentUserException
from src.shared.type_definitions import ShareSnap
from src.uniswap_v2.uniswap import Uniswap


class UniNullUserFallback(Uniswap):

    def __init__(self, sushi=False):
        super().__init__(sushi=sushi)

    def fetch_new_snaps(self, last_block_update: int, max_objects_in_batch: int) -> List[ShareSnap]:
        id_query = '''{
            snaps: liquidityPositionSnapshots(first: $MAX_OBJECTS, orderBy: block, orderDirection: asc, where: {block_gte: $BLOCK}) {
                id
            }
        }'''
        params = {
            '$MAX_OBJECTS': max_objects_in_batch,
            '$BLOCK': last_block_update,
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
                transaction {
                    id
                    gasUsed
                    gasPrice
                }
            }
        }'''
        logging.info(f'{self.exchange}: Last update block: {last_block_update}')
        snaps = []
        for snap_id in raw_snap_ids:
            try:
                raw_snap = self.dex_graph.query(query, {"$ID": snap_id['id']})['data']['liquidityPositionSnapshot']
                snaps.append(self._process_snap(raw_snap))
            except NonExistentUserException:
                logging.error(f'NonExistentUserException - skipping snap with id: {snap_id}')

        if snaps:
            self._populate_eth_prices(snaps)

        return snaps
