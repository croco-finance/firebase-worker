from collections import defaultdict
from decimal import Decimal
from typing import List, Dict, Iterable

from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, PoolToken, CurrencyField, Exchange
from src.uniswap_v2.queries import _staked_query_generator, _eth_prices_query_generator
from src.utils import get_eth_client


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
    POOLS_STAKING_MAP = {
        '0xbb2b8038a1640196fbe3e38816f3e67cba72d940': '0xca35e32e7926b96a9988f61d510e038108d8068e',
        '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc': '0x7fba4b8dc5e7616e59622806932dbea72537a56b',
        '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852': '0x6c3e4cb2e96b01f4b866965a91ed4437839a121a',
        '0xa478c2975ab1ea89e8196811f51a7b7ade33eb11': '0xa1484c3aa22a66c62b77e0ae78e15258bd0cb711',
    }

    def __init__(self):
        self.w3 = get_eth_client()
        # super().__init__('/subgraphs/name/benesjan/uniswap-v2')
        super().__init__('/subgraphs/name/uniswap/uniswap-v2')

    def fetch_new_snaps(self, last_block_update: int, query_limit: int) -> Iterable[List[ShareSnap]]:
        query = '''
                {
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
                    }
                }
                '''
        reached_last, reached_staked_last, first_block = False, False, last_block_update
        while not reached_last or not reached_staked_last:
            last_block = first_block + query_limit
            params = {
                '$MIN_BLOCK': first_block,
                '$MAX_BLOCK': last_block,
            }
            snaps = []
            if not reached_last:
                data = self.dex_graph.query(query, params)['data']
                for snap in data['snaps']:
                    snaps.append(self._process_snap(snap))
                reached_last = not bool(data['snaps'])

            if not reached_staked_last:
                # Get snapshots of staked positions
                new_staked_snaps = self._get_staked_snaps(params)
                snaps += new_staked_snaps
                reached_staked_last = not bool(new_staked_snaps)

            merged_snaps = self._merge_corresponding_snaps(snaps)
            if merged_snaps:
                self._populate_eth_prices(merged_snaps)

            yield merged_snaps
            first_block = last_block

    @staticmethod
    def _process_snap(snap: Dict) -> ShareSnap:
        reserves_usd = Decimal(snap['reserveUSD'])
        tokens: List[PoolToken] = []
        for i in range(2):
            tok, res = snap['pair'][f'token{i}'], Decimal(snap[f'reserve{i}'])
            tokens.append(PoolToken(CurrencyField(symbol=tok['symbol'],
                                                  name=tok['name'],
                                                  contract_address=tok['id'],
                                                  platform='ethereum'),
                                    Decimal('0.5'),
                                    res,
                                    reserves_usd / (2 * res)
                                    ))

        pool_id, block = snap['pair']['id'], snap['block']
        return ShareSnap(
            snap['id'],
            Exchange.UNI_V2,
            snap['user']['id'],
            pool_id,
            Decimal(snap['liquidityTokenBalance']),
            Decimal(snap['totalSupply']),
            reserves_usd,
            tokens,
            snap['block'],
            snap['timestamp'],
            tx_hash=None,  # TODO: fill in the values once the graph is synced
            tx_cost_eth=None,  # TODO: fill in the values once the graph is synced
            eth_price=None  # TODO: fill in the values once the graph is synced
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
        # Set current block info on current positions

        if not staked:
            return []

        # 2. get the pool shares at the time of those snapshots
        query = ''.join(_staked_query_generator(staked))
        data = self.dex_graph.query(query, {})
        snaps = []
        for key, pool in data['data'].items():
            new_snap = self._build_share_snap(staked_dict[key], pool)
            snaps.append(new_snap)

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
            Exchange.UNI_V2,
            stake['user'],
            pool['id'],
            Decimal(stake['liquidityTokenBalance']),
            Decimal(pool['totalSupply']),
            reserves_usd,
            tokens,
            int(stake['blockNumber']),
            int(stake['blockTimestamp']),
            stake['txHash'],
            Decimal(stake['txGasUsed']) * Decimal(stake['txGasPrice']) * Decimal('1E-18'),
            None
        )

    def _merge_corresponding_snaps(self, raw_snaps) -> List[ShareSnap]:
        user_snaps_dict = defaultdict(list)
        for snap in raw_snaps:
            user_snaps_dict[snap.user_addr].append(snap)

        snaps = []
        for user_snaps in user_snaps_dict.values():
            snaps += self._merge_user_snaps(user_snaps)
        return snaps

    @staticmethod
    def _merge_user_snaps(snaps: List[ShareSnap]) -> List[ShareSnap]:
        # Every staked snap has the corresponding liquidity position snap
        # (increase in staked LP balance always results in the equal decrease
        # in the normal snap) - sum LP balances of snaps at the same block
        # Group them by pool id
        pool_dict: Dict[str, Dict[int, ShareSnap]] = {}
        for snap in snaps:
            block, pool_id = snap.block, snap.pool_id

            if pool_id not in pool_dict:
                pool_dict[pool_id] = {}

            if block in pool_dict[pool_id]:
                if pool_dict[pool_id][block].liquidity_token_balance == 0 \
                        or snap.liquidity_token_balance == 0:
                    # When 1 of the 2 snapshots in the same block have 0
                    # balance it means that the snapshots were created because
                    # user deposited all his LP tokens into the staking
                    # contract. Such event is useless for UI and hence I can
                    # delete the snapshots
                    del pool_dict[pool_id][block]
                else:
                    pool_dict[pool_id][block].liquidity_token_balance \
                        += snap.liquidity_token_balance
            else:
                pool_dict[pool_id][block] = snap

        merged_snaps = []
        for summed_snaps in pool_dict.values():
            for snap in summed_snaps.values():
                merged_snaps.append(snap)
        return merged_snaps

    def _get_eth_prices_query_generator(self):
        return _eth_prices_query_generator
