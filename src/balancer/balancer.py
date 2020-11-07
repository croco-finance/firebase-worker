import logging
from decimal import Decimal
from typing import List, Dict, Iterable

from src.balancer.queries import share_query_generator
from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, CurrencyField, PoolToken, Exchange


class Balancer(Dex):
    """
    A handler for Balancer DEX.
    """

    def __init__(self):
        super().__init__('/subgraphs/name/balancer-labs/balancer')

    def fetch_new_snaps(self, last_block_update: int, query_limit: int) -> Iterable[List[ShareSnap]]:
        skip = 0
        while True:
            params = {
                '$MAX_OBJECTS': query_limit,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
            }
            txs = self._get_txs(params)
            if not txs:
                break
            query = ''.join(share_query_generator(txs))
            raw_snaps = self.dex_graph.query(query, {})['data']
            yield self._parse_snaps(raw_snaps)
            skip += query_limit

    def _get_txs(self, params: Dict) -> List[Dict]:
        query = '''
        {
            transactions(first: $MAX_OBJECTS, skip: $SKIP, orderBy: block, orderDirection: asc, where: {block_gt: $BLOCK, event_in:["join", "exit"]}) {
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
                gasUsed
                gasPrice
            }
        }
        '''
        data = self.dex_graph.query(query, params)['data']
        # There is a bug in the subgraph which returns txs twice
        # I'll filter them in the following for loop
        tx_hashes, filtered_txs = set(), []
        for tx in data['transactions']:
            if tx['tx'] not in tx_hashes:
                tx_hashes.add(tx['tx'])
                filtered_txs.append(tx)
        return filtered_txs

    @staticmethod
    def _parse_snaps(shares: Dict[str, Dict]) -> List[ShareSnap]:
        snaps = []
        for key, share_list in shares.items():
            if len(share_list) != 1:
                # Occurs for weird unused pools on Balancer - ignoring for now
                logging.warning(f'Incorrect number of pool shares in a list: {share_list}'
                                f'key: {key}')
                continue
            share = share_list[0]
            pool = share['poolId']
            total_weight = Decimal(pool['totalWeight'])
            reserves_usd = Decimal(pool['liquidity'])
            tokens: List[PoolToken] = []
            for token in pool['tokens']:
                token_weight = Decimal(token['denormWeight']) / total_weight
                token_reserve = Decimal(token['balance'])
                price_usd = reserves_usd * token_weight / token_reserve if token_reserve != 0 else 0
                tokens.append(PoolToken(
                    CurrencyField(symbol=token['symbol'],
                                  name=token['name'],
                                  contract_address=token['address'],
                                  platform='ethereum'),
                    token_weight,
                    token_reserve,
                    price_usd,
                ))
            tx_, block_, timestamp_, tx_cost_wei, user_addr = key.split('_')
            snaps.append(ShareSnap(
                tx_[1:],  # This ID might not be unique if user did multiple changes in one call but I don't care
                Exchange.BALANCER,
                user_addr,
                pool['id'],
                Decimal(share['balance']),
                Decimal(pool['totalShares']),
                reserves_usd,
                tokens,
                int(block_),
                int(timestamp_),
                tx_[1:],
                Decimal(tx_cost_wei) * Decimal('1E-18'),
                None
            ))
        return snaps