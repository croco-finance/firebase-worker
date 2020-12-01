import logging
from collections import defaultdict
from decimal import Decimal
from typing import List, Iterable, Dict, Tuple

from src.shared.type_definitions import ShareSnap, PoolToken, CurrencyField
from src.subgraph import SubgraphReader
from src.uniswap_v2.uniswap import Uniswap


class UniMatchingTxs(Uniswap):

    def __init__(self):
        super().__init__(dex_subgraph='uniswap/uniswap-v2')
        self.tx_graph = SubgraphReader('benesjan/uni-v2-lp-txs')
        self.uni_price_first_block = 10876348

    def fetch_new_snaps(self, last_block_update: int, max_objects_in_batch: int) -> Iterable[List[ShareSnap]]:
        query = '''{
            snaps: liquidityPositionSnapshots(first: $MAX_OBJECTS, skip: $SKIP, orderBy: block, orderDirection: asc, where: {block_gte: $BLOCK}) {
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
        highest_indexed_block = self.get_highest_indexed_block(self.dex_graph)
        logging.info(f'{self.exchange}: Last update block: {last_block_update}, '
                     f'highest indexed block: {highest_indexed_block}')
        skip = 0
        while True:
            params = {
                '$MAX_OBJECTS': max_objects_in_batch,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
            }
            txs, tx_amount = self._get_txs(params)
            raw_snaps = self.dex_graph.query(query, params)['data']['snaps']
            if not raw_snaps:
                break
            snaps = [self._process_snap(snap, txs) for snap in raw_snaps]

            self._populate_eth_prices(snaps)

            yield snaps
            skip += max_objects_in_batch

    def _get_txs(self, params: Dict) -> Tuple[Dict[str, List[Dict]], int]:
        # When from is 0 address, it's a mint
        # When to is 0 address, it's a burn
        # When none is 0 address, it's transfer of LP tokens
        query = '''{
            transactions(first: 1000, orderBy: blockNumber, orderDirection: asc, where: {blockNumber_gte: $MIN_BLOCK, blockNumber_lt: $MAX_BLOCK}) {
                id
                blockNumber
                timestamp
                from
                to
                gasUsed
                gasPrice
            }
        }'''
        txs = self.tx_graph.query(query, params)['data']['transactions']
        indexed_txs = defaultdict(list)
        for tx in txs:
            indexed_txs[tx['from'] + tx['blockNumber']].append(tx)
            indexed_txs[tx['to'] + tx['blockNumber']].append(tx)
        return indexed_txs, len(txs)

    def _process_snap(self, snap: Dict, txs: Dict[str, List[Dict]]) -> ShareSnap:
        reserves_usd = Decimal(snap['reserveUSD'])
        tokens: List[PoolToken] = []
        for i in range(2):
            tok, res = snap['pair'][f'token{i}'], Decimal(snap[f'reserve{i}'])
            if res:
                price = reserves_usd / (2 * res)
            else:
                price = 0
                logging.warning(f'0 reserves for token {tok["symbol"]} in snap {snap["id"]}. '
                                'Setting token price to 0.')
            tokens.append(PoolToken(CurrencyField(symbol=tok['symbol'],
                                                  name=tok['name'],
                                                  contract_address=tok['id'],
                                                  platform='ethereum'),
                                    Decimal('0.5'),
                                    res,
                                    price
                                    ))

        pool_id, block, user = snap['pair']['id'], snap['block'], snap['user']['id'],
        txs = txs[f'{user}{block}']
        if len(txs) == 1:
            tx = txs[0]
        elif len(txs) > 1:
            tx = txs[0]
            logging.debug('Multiple user txs in a block. Can not distinguish the corresponding tx - choosing the '
                          f'first one. User: {user}, block: {block}.')
        else:
            logging.debug(f'No user txs in a block - creating fake value. User: {user}, block: {block}.')
            tx = {
                'blockNumber': block,
                'id': '',
                'gasUsed': 0,
                'gasPrice': 0
            }

        return ShareSnap(
            snap['id'],
            self.exchange,
            user,
            pool_id,
            Decimal(snap['liquidityTokenBalance']),
            Decimal(snap['totalSupply']),
            tokens,
            int(tx['blockNumber']),
            int(snap['timestamp']),
            tx['id'],
            Decimal(tx['gasUsed']) * Decimal(tx['gasPrice']) * Decimal('1E-18'),
            eth_price=None,
            yield_token_price=None
        )
