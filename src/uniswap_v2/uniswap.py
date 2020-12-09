import logging
from collections import defaultdict
from decimal import Decimal
from typing import List, Dict, Iterable, Callable, Optional

from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, PoolToken, CurrencyField, Pool, StakingService, PoolDayData
from src.subgraph import SubgraphReader
from src.uniswap_v2.queries import _staked_query_generator, _eth_prices_query_generator, yield_reserves_query_generator, \
    pool_day_data_query_generator
from src.uniswap_v2.yield_pools import yield_pools


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
            snap['liquidityTokenBalance'],
            snap['totalSupply'],
            tokens,
            snap['block'],
            snap['timestamp'],
            snap['transaction']['id'],
            Decimal(snap['transaction']['gasUsed']) * Decimal(snap['transaction']['gasPrice']) * Decimal('1E-18'),
        )

    def fetch_new_staked_snaps(self, last_block_update: int, max_objects_in_batch: int,
                               staking_service: Optional[StakingService] = None) -> Iterable[List[ShareSnap]]:
        highest_indexed_block = self.get_highest_indexed_block(self.rewards_graph)
        logging.info(f'{self.exchange}: Last update block: {last_block_update}, '
                     f'highest indexed block: {highest_indexed_block}')
        skip = 0
        while True:
            params = {
                '$MAX_OBJECTS': max_objects_in_batch,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
                '$EXCHANGE': self.exchange.name,
                '$STAKING_SERVICE_FILTER': f', stakingService: {staking_service.name}' if staking_service else ''
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
            stakePositionSnapshots(first: $MAX_OBJECTS, skip: $SKIP, orderBy: blockNumber, orderDirection: asc, where: {blockNumber_gte: $BLOCK, exchange: $EXCHANGE$STAKING_SERVICE_FILTER}) {
                id
                stakingService
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
        data = self.dex_graph.query(query, {})['data']
        # build the snap list and return
        snaps = []
        for key, staked in staked_dict.items():
            pool_key = key.split("-")[0]
            pool = data[pool_key]
            snaps.append(self._build_share_snap(staked, pool))
        return snaps

    def _build_share_snap(self, stake: Dict, pool: Dict) -> ShareSnap:
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
        staking_service = StakingService[stake['stakingService']] if 'stakingService' in stake else None
        return ShareSnap(
            stake['id'],
            self.exchange,
            stake['user'],
            pool['id'],
            stake['liquidityTokenBalance'],
            pool['totalSupply'],
            tokens,
            stake['blockNumber'],
            stake['blockTimestamp'],
            stake['txHash'],
            Decimal(stake['txGasUsed']) * Decimal(stake['txGasPrice']) * Decimal('1E-18'),
            staking_service=staking_service
        )

    def _get_eth_prices_query_generator(self) -> Callable[[Iterable[int]], Iterable[str]]:
        return _eth_prices_query_generator

    def _populate_yield_prices(self, snaps: List[ShareSnap]):
        yield_grouped_block_filtered_snaps = defaultdict(list)
        for snap in snaps:
            yield_pool = yield_pools[snap.staking_service]
            if snap.block >= yield_pool.firs_block:
                yield_grouped_block_filtered_snaps[snap.staking_service].append(snap)
        if not yield_grouped_block_filtered_snaps:
            return

        for staking_service_name, snap_list in yield_grouped_block_filtered_snaps.items():
            blocks = {snap.block for snap in snap_list}
            yield_pool = yield_pools[staking_service_name]
            query = ''.join(yield_reserves_query_generator(blocks, yield_pools[staking_service_name].pool_id))
            data = SubgraphReader(yield_pool.subgraph_name).query(query)
            prices = {int(block[1:]): Decimal(val['reserveUSD']) / (2 * Decimal(val['reserve0'])) for
                      block, val in data['data'].items()}
            for snap in snap_list:
                snap.yield_token_price = prices[snap.block]

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
        yield_token_prices = self._get_yield_token_prices()
        while True:
            params = {
                '$MAX_OBJECTS': max_objects_in_batch,
                '$SKIP': skip,
                '$MIN_LIQUIDITY': min_liquidity,
            }
            raw_pools = self.dex_graph.query(query, params)['data']['pairs']
            if not raw_pools:
                break

            yield [self._parse_pool(pool, highest_indexed_block, eth_price, yield_token_prices) for pool in raw_pools]
            skip += max_objects_in_batch

    def _get_yield_token_prices(self) -> Dict[StakingService, Decimal]:
        prices = {}
        for staking_service, yield_pool in yield_pools.items():
            if staking_service is StakingService.UNI_V2:
                # We are no longer supporting UNI rewards as liquidity mining ended
                continue
            subgraph = SubgraphReader(yield_pool.subgraph_name)
            highest_indexed_block = self.get_highest_indexed_block(subgraph)
            query = ''.join(yield_reserves_query_generator([highest_indexed_block], yield_pool.pool_id))
            data = SubgraphReader(yield_pool.subgraph_name).query(query)
            val = list(data['data'].values())[0]
            prices[staking_service] = Decimal(val['reserveUSD']) / (2 * Decimal(val['reserve0']))
        return prices

    def _parse_pool(self, raw_pool: Dict, block: int, eth_price: Decimal,
                    yield_token_prices: Dict[StakingService, Decimal]) -> Pool:
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
            yield_token_prices
        )

    def get_pool_day_data(self, max_objects_in_batch: int, min_liquidity: int) -> Iterable[List[PoolDayData]]:
        id_query = '''{
            pairs(first: $MAX_OBJECTS, skip: $SKIP, orderBy: reserveUSD, orderDirection: desc, where: {reserveUSD_gte: $MIN_LIQUIDITY}) {
                id
            }
        }'''
        skip, highest_indexed_block = 0, self.get_highest_indexed_block(self.dex_graph)
        while True:
            params = {
                '$MAX_OBJECTS': max_objects_in_batch,
                '$SKIP': skip,
                '$MIN_LIQUIDITY': min_liquidity,
            }
            pool_ids = self.dex_graph.query(id_query, params)['data']['pairs']
            if not pool_ids:
                break

            query = ''.join(pool_day_data_query_generator(pool_ids))
            data = self.dex_graph.query(query)['data']
            yield [self._parse_pool_day_data(pool[0]) for pool in data.values()]
            skip += max_objects_in_batch

    def _parse_pool_day_data(self, pool: Dict) -> PoolDayData:
        return PoolDayData(pool_id=pool['pairAddress'],
                           timestamp=int(pool['date']),
                           liquidity_token_total_supply=pool['totalSupply'],
                           usd_volume=Decimal(pool['dailyVolumeUSD']),
                           token_volume=[Decimal(pool['dailyVolumeToken0']), Decimal(pool['dailyVolumeToken1'])])
