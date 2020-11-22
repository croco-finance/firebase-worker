import logging
from decimal import Decimal
from typing import List, Dict, Iterable, Callable

from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, PoolToken, CurrencyField, Exchange, Pool
from src.uniswap_v2.queries import _staked_query_generator, _eth_prices_query_generator, _uni_reserves_query_generator


class Uniswap(Dex):
    """
    A handler for Uniswap v2 DEX.
    """

    # Used to override prices at the beginning of Uni v2
    # - taken from uniswap.info source code
    PRICE_OVERRIDES = {
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': Decimal('1'),  # USDC
        '0x6b175474e89094c44da98b954eedeac495271d0f': Decimal('1'),  # DAI
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': Decimal('203')  # WETH
    }

    PRICE_DISCOVERY_START_TIMESTAMP = 1589747086

    # Key is pool id and value is the corresponding staking contract
    # Used for yield farming calculations
    # The staking contracts were deployed at block 10875196
    POOLS_STAKING_MAP = {
        '0xbb2b8038a1640196fbe3e38816f3e67cba72d940': '0xca35e32e7926b96a9988f61d510e038108d8068e',
        '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc': '0x7fba4b8dc5e7616e59622806932dbea72537a56b',
        '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852': '0x6c3e4cb2e96b01f4b866965a91ed4437839a121a',
        '0xa478c2975ab1ea89e8196811f51a7b7ade33eb11': '0xa1484c3aa22a66c62b77e0ae78e15258bd0cb711',
    }

    def __init__(self, subgraph_name='benesjan/uniswap-v2'):
        super().__init__(subgraph_name, Exchange.UNI_V2)
        self.uni_price_first_block = 10876348

    def fetch_new_snaps(self, last_block_update: int, query_limit: int) -> Iterable[List[ShareSnap]]:
        query = '''{
            snaps: liquidityPositionSnapshots(first: 1000, orderBy: block, orderDirection: asc, where: {block_gte: $MIN_BLOCK, block_lt: $MAX_BLOCK}) {
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
        first_block, highest_indexed_block = last_block_update, self.get_highest_indexed_block(self.dex_graph)
        logging.info(f'{self.exchange}: Last update block: {last_block_update}, '
                     f'highest indexed block: {highest_indexed_block}')
        while first_block < highest_indexed_block:
            last_block = first_block + query_limit
            if last_block > highest_indexed_block:
                last_block = highest_indexed_block
            params = {
                '$MIN_BLOCK': first_block,
                '$MAX_BLOCK': last_block,
            }
            raw_snaps = self.dex_graph.query(query, params)['data']['snaps']
            snaps = [self._process_snap(snap) for snap in raw_snaps]

            # Get snapshots of staked positions
            snaps += self._get_staked_snaps(params)

            if snaps:
                self._populate_eth_prices(snaps)
                self._populate_uni_prices(snaps)

            yield snaps
            first_block = last_block
            # Feedback regulating query limit in order to not get near the 1000 entities/request limit
            if len(snaps) > 300 and query_limit > 20:
                query_limit -= 10
                logging.info(f'Decreased query limit to: {query_limit}')
            elif len(snaps) < 100:
                query_limit += 10
                logging.info(f'Increased query limit to: {query_limit}')

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
            snap['tx'],
            Decimal(snap['gasUsed']) * Decimal(snap['gasPrice']) * Decimal('1E-18'),
            eth_price=None,
            yield_token_price=None
        )

    def _get_staked_snaps(self, params: Dict) -> List[ShareSnap]:
        query = '''
        {
            stakePositionSnapshots(first: 1000, orderBy: blockNumber, orderDirection: asc, where: {blockNumber_gte: $MIN_BLOCK, blockNumber_lt: $MAX_BLOCK, exchange: "UNI_V2"}) {
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
        staked_dict = {f'b{stake["blockNumber"]}_{stake["pool"]}': stake for stake in staked}

        if not staked:
            return []

        # 2. get the pool shares at the time of those snapshots
        query = ''.join(_staked_query_generator(staked))
        data = self.dex_graph.query(query, {})
        snaps = []
        for key, pool in data['data'].items():
            new_snap = self._build_share_snap(staked_dict[key], pool, staked=True)
            snaps.append(new_snap)

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
                price_usd = reserves_usd / (2 * res)

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

    def _populate_uni_prices(self, snaps: List[ShareSnap]):
        relevant_snaps = [snap for snap in snaps if
                          snap.block >= self.uni_price_first_block and snap.pool_id in self.POOLS_STAKING_MAP.keys()]
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
        query = ''.join(_uni_reserves_query_generator(blocks))
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
