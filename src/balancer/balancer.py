from decimal import Decimal
from typing import List, Dict, Iterable, Callable

from src.balancer.queries import _eth_prices_query_generator, _bal_prices_query_generator
from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, CurrencyField, PoolToken, Exchange, Pool, StakingService, PoolDayData


class Balancer(Dex):
    """
    A handler for Balancer DEX.
    """

    def __init__(self):
        super().__init__('benesjan/balancer-with-snapshots', Exchange.BALANCER, eth_price_first_block=9783867)
        # rewards start at 10322999 but at that point the prices are not yet in the graph
        self.bal_price_first_block = 10323092

    def fetch_new_snaps(self, last_block_update: int, max_objects_in_batch: int) -> Iterable[List[ShareSnap]]:
        query = '''{
            snaps: poolShareSnapshots(first: $MAX_OBJECTS, skip: $SKIP, orderBy: block, orderDirection: asc, where: {block_gte: $BLOCK}) {
                id
                userAddress {
                    id
                }
                balance
                tokenSnapshots {
                    balance
                    token {
                        poolId {
                            id
                            totalWeight
                        }
                        symbol
                        name
                        address
                        balance
                        denormWeight
                    }
                }
                liquidity
                totalShares
                txHash
                block
                timestamp
                gasUsed
                gasPrice
            }
        }'''
        skip = 0
        while True:
            params = {
                '$MAX_OBJECTS': max_objects_in_batch,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
            }
            # The number of txs will not add up to query limit, due to tx filtering
            raw_snaps = self.dex_graph.query(query, params)['data']['snaps']
            if not raw_snaps:
                break
            snaps = [self._parse_snap(snap) for snap in raw_snaps]
            if snaps:
                self._populate_eth_prices(snaps)
                self._populate_bal_prices(snaps)

            yield snaps
            skip += max_objects_in_batch

    def _parse_snap(self, snap: Dict) -> ShareSnap:
        # TODO: simplify the following once the subgrpah is resynced
        pool = snap['tokenSnapshots'][0]['token']['poolId']
        total_weight = Decimal(pool['totalWeight'])
        reserves_usd = Decimal(snap['liquidity'])
        tokens: List[PoolToken] = []
        for tokenSnap in snap['tokenSnapshots']:
            token = tokenSnap['token']
            # Replace token reserves with the one from snap
            token['balance'] = snap['balance']
            tokens.append(self._parse_token(token, total_weight, reserves_usd))
        return ShareSnap(
            snap['id'],
            self.exchange,
            snap['userAddress']['id'],
            pool['id'],
            snap['balance'],
            snap['totalShares'],
            tokens,
            snap['block'],
            snap['timestamp'],
            snap['txHash'],
            Decimal(snap['gasPrice']) * Decimal(snap['gasUsed']) * Decimal('1E-18')
        )

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

    def fetch_pools(self, max_objects_in_batch: int, min_liquidity: int) -> Iterable[List[Pool]]:
        query = '''{
            pools(first: $MAX_OBJECTS, skip: $SKIP, orderBy: liquidity, orderDirection: desc, where: {liquidity_gte: $MIN_LIQUIDITY}) {
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
                '$MIN_LIQUIDITY': min_liquidity,
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
            tokens.append(self._parse_token(token, total_weight, reserves_usd))
        return Pool(
            raw_pool['id'],
            self.exchange,
            Decimal(raw_pool['totalShares']),
            tokens,
            block,
            eth_price,
            {StakingService.BALANCER: yield_token_price}
        )

    def _parse_token(self, token: Dict, total_weight: Decimal, reserves_usd: Decimal) -> PoolToken:
        token_weight = Decimal(token['denormWeight']) / total_weight
        token_reserve = Decimal(token['balance'])
        price_usd = reserves_usd * token_weight / token_reserve if token_reserve != 0 else 0
        return PoolToken(
            CurrencyField(symbol=token['symbol'],
                          name=token['name'],
                          contract_address=token['address'],
                          platform='ethereum'),
            token_weight,
            token_reserve,
            price_usd
        )

    def fetch_new_staked_snaps(self, last_block_update: int, max_objects_in_batch: int) -> Iterable[List[ShareSnap]]:
        raise NotImplementedError

    def get_pool_day_data(self, max_objects_in_batch: int, min_liquidity: int) -> Iterable[List[PoolDayData]]:
        raise NotImplementedError
