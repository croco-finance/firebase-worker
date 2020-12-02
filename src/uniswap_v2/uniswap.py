import logging
from decimal import Decimal
from typing import List, Dict, Iterable, Callable, Tuple

from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, PoolToken, CurrencyField, Exchange, Pool
from src.uniswap_v2.queries import _staked_query_generator, _eth_prices_query_generator, _yield_reserves_query_generator


class Uniswap(Dex):
    """
    A handler for Uniswap v2 DEX.
    """

    PRICE_OVERRIDES = {
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': Decimal('1'),  # USDC
        '0x6b175474e89094c44da98b954eedeac495271d0f': Decimal('1'),  # DAI
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': Decimal('203')  # WETH
    }

    # Used to override prices at the beginning of Uni v2
    # - taken from uniswap.info source code
    PRICE_DISCOVERY_START_TIMESTAMP = 1589747086

    def __init__(
            self,
            exchange=Exchange.UNI_V2,
            dex_subgraph='uniswap/uniswap-v2',
            # dex_subgraph='benesjan/uniswap-v2',
            yield_price_pair='0xd3d2e2692501a5c9ca623199d38826e513033a17',
            yield_price_first_block=10876348
    ):
        super().__init__(dex_subgraph, exchange)
        self.yield_price_pair = yield_price_pair
        self.yield_price_first_block = yield_price_first_block

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
                transaction {
                    id
                    gasUsed
                    gasPrice
                }
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
            raw_snaps = self.dex_graph.query(query, params)['data']['snaps']
            if not raw_snaps:
                break
            snaps = [self._process_snap(snap) for snap in raw_snaps]
            self._populate_eth_prices(snaps)

            yield snaps
            skip += max_objects_in_batch

    def _process_snap(self, snap: Dict) -> ShareSnap:
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

        pool_id, block = snap['pair']['id'], snap['block']
        return ShareSnap(
            snap['id'],
            self.exchange,
            snap['user']['id'],
            pool_id,
            Decimal(snap['liquidityTokenBalance']),
            Decimal(snap['totalSupply']),
            tokens,
            snap['block'],
            snap['timestamp'],
            snap['transaction']['id'],
            Decimal(snap['transaction']['gasUsed']) * Decimal(snap['transaction']['gasPrice']) * Decimal('1E-18'),
            eth_price=None,
            yield_token_price=None
        )

    def fetch_new_staked_snaps(self, last_block_update: int, max_objects_in_batch: int) -> Iterable[List[ShareSnap]]:
        highest_indexed_block = self.get_highest_indexed_block(self.rewards_graph)
        logging.info(f'{self.exchange}: Last update block: {last_block_update}, '
                     f'highest indexed block: {highest_indexed_block}')
        skip, snaps = 0, range(max_objects_in_batch)
        while True:
            assert len(snaps) == max_objects_in_batch, 'Incorrect number of snaps in a batch:' \
                                                       f'{len(snaps)} instead of {max_objects_in_batch}'
            params = {
                '$MAX_OBJECTS': max_objects_in_batch,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
                '$EXCHANGE': self.exchange.name,
            }
            snaps = self._get_staked_snaps(params)
            skip += max_objects_in_batch
            if len(snaps) == 0:
                break
            self._populate_eth_prices(snaps)
            self._populate_yield_prices(snaps)

            yield snaps

    def _get_staked_snaps(self, params: Dict) -> List[ShareSnap]:
        query = '''
        {
            stakePositionSnapshots(first: $MAX_OBJECTS, skip: $SKIP, orderBy: blockNumber, orderDirection: asc, where: {blockNumber_gte: $BLOCK, , exchange: "$EXCHANGE"}) {
                id
                user
                pool
                liquidityTokenBalance
                blockNumber
                blockTimestamp
                txHash
                txGasUsed
                txGasPrice
            }
        }
        '''
        # 1. Get the positions and snapshots
        staked = self.rewards_graph.query(query, params)['data']['stakePositionSnapshots']
        staked_dict = {f'b{stake["blockNumber"]}_{stake["pool"]}-{stake["id"]}': stake for stake in staked}

        if not staked:
            return []

        # 2. get the pool shares at the time of those snapshots
        query = ''.join(_staked_query_generator(staked))
        data = self.dex_graph.query(query, {})
        # buikd the snap list and return
        snaps = []
        for key, staked in staked_dict.items():
            pool_key = key.split("-")[0]
            pool = data['data'][pool_key]
            snaps.append(self._build_share_snap(staked, pool, staked=True))
        return snaps

    def _build_share_snap(self, stake: Dict, pool: Dict, staked=False) -> ShareSnap:
        reserves_usd = Decimal(pool['reserveUSD'])
        tokens = []
        for i in range(2):
            tok, res = pool[f'token{i}'], Decimal(pool[f'reserve{i}'])

            if int(stake['blockTimestamp']) < self.PRICE_DISCOVERY_START_TIMESTAMP and \
                    tok['id'] in self.PRICE_OVERRIDES:
                price_usd = self.PRICE_OVERRIDES[tok['id']]
            else:
                # In the graph Pair object the price is stored relatively
                # between the 2 tokens. To compute the USD price I used
                # the following equation transformation:
                # r0 * t0Relative + r1 * t1Relative = reserveUSD
                # t1Relative = r1/r0*t0Relative
                # ==> t0Dollars = reserveUSD/(2*r0)
                # ==> t1Dollars = reserveUSD/(2*r1)
                price_usd = 0 if res == 0 else reserves_usd / (2 * res)

            token_type = CurrencyField(symbol=tok['symbol'],
                                       name=tok['name'],
                                       contract_address=tok['id'],
                                       platform='ethereum')
            tokens.append(PoolToken(token_type,
                                    Decimal('0.5'),
                                    res,
                                    price_usd))
        return ShareSnap(
            stake['id'],
            self.exchange,
            stake['user'],
            pool['id'],
            Decimal(stake['liquidityTokenBalance']),
            Decimal(pool['totalSupply']),
            tokens,
            int(stake['blockNumber']),
            int(stake['blockTimestamp']),
            stake['txHash'],
            Decimal(stake['txGasUsed']) * Decimal(stake['txGasPrice']) * Decimal('1E-18'),
            None,
            None,
            staked
        )

    def _get_eth_prices_query_generator(self) -> Callable[[Iterable[int]], Iterable[str]]:
        return _eth_prices_query_generator

    def _populate_yield_prices(self, snaps: List[ShareSnap]):
        relevant_snaps = [snap for snap in snaps if snap.block >= self.yield_price_first_block]
        if not relevant_snaps:
            return
        blocks = {snap.block for snap in relevant_snaps}
        prices = self._get_yield_token_prices(blocks)
        for snap in relevant_snaps:
            snap.yield_token_price = prices[snap.block]

    def _get_yield_token_prices(self, blocks: Iterable[int]) -> Dict[int, Decimal]:
        """
        Fetch eth prices in specific block times.
        (used to denominate the returns in ETH)
        """
        query = ''.join(_yield_reserves_query_generator(blocks, self.yield_price_pair))
        data = self.dex_graph.query(query, {})
        return {int(block[1:]): Decimal(val['reserveUSD']) / (2 * Decimal(val['reserve0'])) for
                block, val in data['data'].items()}

    def fetch_pools(self, max_objects_in_batch: int, min_liquidity: int) -> Iterable[List[Pool]]:
        query = '''{
            pairs(first: $MAX_OBJECTS, skip: $SKIP, orderBy: reserveUSD, orderDirection: desc, where: {reserveUSD_gte: $MIN_LIQUIDITY}) {
                id
                reserveUSD
                reserve0
                reserve1
                totalSupply
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
            raw_pools = self.dex_graph.query(query, params)['data']['pairs']
            if not raw_pools:
                break

            yield [self._parse_pool(pool, highest_indexed_block, eth_price, yield_token_price) for pool in raw_pools]
            skip += max_objects_in_batch

    def _parse_pool(self, raw_pool: Dict, block: int, eth_price: Decimal, yield_token_price: Decimal) -> Pool:
        reserves_usd = Decimal(raw_pool['reserveUSD'])
        tokens: List[PoolToken] = []
        for i in range(2):
            tok, res = raw_pool[f'token{i}'], Decimal(raw_pool[f'reserve{i}'])
            price_usd = reserves_usd / (2 * res) if res else 0
            tokens.append(PoolToken(CurrencyField(symbol=tok['symbol'],
                                                  name=tok['name'],
                                                  contract_address=tok['id'],
                                                  platform='ethereum'),
                                    Decimal('0.5'),
                                    res,
                                    price_usd
                                    ))
        return Pool(
            raw_pool['id'],
            self.exchange,
            Decimal(raw_pool['totalSupply']),
            tokens,
            block,
            eth_price,
            yield_token_price
        )
