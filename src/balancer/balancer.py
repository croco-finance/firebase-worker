import logging
from decimal import Decimal
from typing import List, Dict, Iterable, Callable

from src.balancer.queries import share_query_generator, _eth_prices_query_generator, _bal_prices_query_generator
from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, CurrencyField, PoolToken, Exchange, Pool


class Balancer(Dex):
    """
    A handler for Balancer DEX.
    """

    def __init__(self):
        super().__init__('balancer-labs/balancer', Exchange.BALANCER)
        # rewards start at 10322999 but at that point the prices are not yet in the graph
        self.bal_price_first_block = 10323092

    def fetch_new_snaps(self, last_block_update: int, query_limit: int) -> Iterable[List[ShareSnap]]:
        skip = 0
        while True:
            params = {
                '$MAX_OBJECTS': query_limit,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
            }
            # The number of txs will not add up to query limit, due to tx filtering
            txs = self._get_txs(params)
            if not txs:
                break
            query = ''.join(share_query_generator(txs))
            raw_snaps = self.dex_graph.query(query, {})['data']
            snaps = self._parse_snaps(raw_snaps)
            if snaps:
                self._populate_eth_prices(snaps)
                self._populate_bal_prices(snaps)

            yield snaps
            skip += query_limit

    def _get_txs(self, params: Dict) -> List[Dict]:
        query = '''
        {
            transactions(first: $MAX_OBJECTS, skip: $SKIP, orderBy: block, orderDirection: asc, where: {block_gte: $BLOCK, event_in:["join", "exit"]}) {
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
        logging.info(f'Filtered: {len(data["transactions"]) - len(filtered_txs)} txs due to duplicity')
        return filtered_txs

    def _parse_snaps(self, shares: Dict[str, Dict]) -> List[ShareSnap]:
        snaps = []
        for key, share_list in shares.items():
            if len(share_list) != 1:
                # Occurs for weird unused pools on Balancer - ignoring for now
                logging.warning(f'Incorrect number of pool shares in a list: {share_list}, '
                                f'key: {key}')
                continue
            share = share_list[0]
            pool = self._parse_pool(share['poolId'])
            tx_, block_, timestamp_, tx_cost_wei, user_addr = key.split('_')
            snaps.append(ShareSnap(
                tx_[1:],  # This ID might not be unique if user did multiple changes in one call but I don't care
                self.exchange,
                user_addr,
                pool.id,
                Decimal(share['balance']),
                pool.liquidity_token_total_supply,
                pool.tokens,
                int(block_),
                int(timestamp_),
                tx_[1:],
                Decimal(tx_cost_wei) * Decimal('1E-18'),
                None,
                None
            ))
        return snaps

    def _populate_bal_prices(self, snaps: List[ShareSnap]):
        relevant_snaps = [snap for snap in snaps if snap.block >= self.bal_price_first_block]
        if not relevant_snaps:
            return
        blocks = {snap.block for snap in relevant_snaps}
        bal_prices = self._get_yield_token_prices(blocks)
        for snap in relevant_snaps:
            snap.yield_token_price = bal_prices[snap.block]

    def _get_yield_token_prices(self, blocks: Iterable[int]) -> Dict[int, Decimal]:
        """
        Fetch eth prices in specific block times.
        (used to denominate the returns in ETH)
        """
        query = ''.join(_bal_prices_query_generator(blocks))
        data = self.dex_graph.query(query, {})
        return {int(block[1:]): Decimal(price['price']) for
                block, price in data['data'].items()}

    def _get_eth_prices_query_generator(self) -> Callable[[Iterable[int]], Iterable[str]]:
        return _eth_prices_query_generator

    def fetch_pools(self, max_objects_in_batch: int) -> Iterable[List[Pool]]:
        query = '''{
            pools(first: $MAX_OBJECTS, skip: $SKIP, orderBy: liquidity, orderDirection: desc) {
                id
                symbol
                totalWeight
                totalShares
                liquidity
                tokens {
                    symbol
                    name
                    address
                    denormWeight
                    balance
                }
            }
        }'''
        skip, highest_indexed_block = 0, self.get_highest_indexed_block(self.dex_graph)
        eth_price = self._get_eth_usd_prices([highest_indexed_block])[highest_indexed_block]
        yield_token_price = self._get_yield_token_prices([highest_indexed_block])[highest_indexed_block]
        while True:
            params = {
                '$MAX_OBJECTS': max_objects_in_batch,
                '$SKIP': skip,
            }
            raw_pools = self.dex_graph.query(query, params)['data']['pools']
            if not raw_pools:
                break

            yield [self._parse_pool(pool, highest_indexed_block, eth_price, yield_token_price) for pool in raw_pools]
            skip += max_objects_in_batch

    def _parse_pool(self, raw_pool: Dict, block: int, eth_price: Decimal, yield_token_price: Decimal) -> Pool:
        total_weight = Decimal(raw_pool['totalWeight'])
        reserves_usd = Decimal(raw_pool['liquidity'])
        tokens: List[PoolToken] = []
        for token in raw_pool['tokens']:
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
        return Pool(
            raw_pool['id'],
            self.exchange,
            Decimal(raw_pool['totalShares']),
            tokens,
            block,
            eth_price,
            yield_token_price
        )
