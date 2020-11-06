from decimal import Decimal
from typing import List, Dict

from src.shared.Dex import Dex
from src.shared.type_definitions import ShareSnap, PoolToken, CurrencyField, Exchange
from src.uniswap_v2.queries import _staked_query_generator
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

    def fetch_new_snaps(self, last_block_update: int, current_block: int, max_objects=200) -> List[ShareSnap]:
        query = '''
                {
                    snaps: liquidityPositionSnapshots(first: $MAX_OBJECTS, skip: $SKIP, where: {block_gt: $BLOCK}) {
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
        skip, snaps, staked_snaps = 0, [], []
        while last_block_update + skip < current_block:
            params = {
                '$MAX_OBJECTS': max_objects,
                '$SKIP': skip,
                '$BLOCK': last_block_update,
            }
            data = self.dex_graph.query(query, params)['data']
            for snap in data['snaps']:
                snaps.append(self._process_snap(snap))

            # Get snapshots of staked positions
            staked_snaps += self._get_staked_snaps(last_block_update, skip)
            skip += max_objects

        return self._merge_snaps_and_staked(snaps, staked_snaps)

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
            int(snap['timestamp']),
            tx_cost_eth=None,
            yield_reward=None,
            eth_price=None
        )

    def _get_staked_snaps(self, last_block_update: int, skip: int) -> List[ShareSnap]:
        """
        :param address: user ethereum address
        :param block_delay: a parameter which sets how many blocks back
            should be considered as the last block - necessary in order
            to account for the slowness of thegraph.com indexer
        """
        query = '''
        {
            stakePositionSnapshots(where: {block_gt: $BLOCK, exchange: "UNI_V2"}) {
                pool
                liquidityTokenBalance
                blockNumber
                blockTimestamp
            }
        }
        '''
        params = {
            '$BLOCK': last_block_update,
        }
        # 1. Get the positions and snapshots
        positions = self.rewards_graph.query(query, params)['data']['stakePositionSnapshots']
        # Set current block info on current positions

        if not positions:
            return []

        # 2. get the pool shares at the time of those snapshots
        query = ''.join(_staked_query_generator(positions))
        data = self.dex_graph.query(query, {})
        snaps = []
        for key, pool in data['data'].items():
            # Key consists of t{TIMESTAMP}_{lp_token_balance}
            timestamp_, balance_, block_ = key.split('_')
            timestamp, block = int(timestamp_[1:]), int(block_)
            lp_token_balance = Decimal(balance_.replace('dot', '.'))

            new_snap = self._build_share_snap(pool, timestamp,
                                              lp_token_balance,
                                              address, block)
            snaps.append(new_snap)

        return snaps

    def _build_share_snap(self, pool, timestamp, lp_token_balance,
                          address, block) -> PoolShareSnapshot:
        reserves_usd = Decimal(pool['reserveUSD'])
        tokens = []
        for i in range(2):
            tok, res = pool[f'token{i}'], Decimal(pool[f'reserve{i}'])

            if timestamp < self.PRICE_DISCOVERY_START_TIMESTAMP and \
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

            tokens.append({
                'token': CurrencyField(symbol=tok['symbol'],
                                       name=tok['name'],
                                       contract_address=tok['id'],
                                       platform='ethereum'),
                'weight': Decimal('0.5'),
                'reserve': res,
                'price_usd': price_usd
            })

        return {
            'exchange': Exchange.UNI_V2,
            'user_addr': address,
            'pool_id': pool['id'],
            'liquidity_token_balance': lp_token_balance,
            'liquidity_token_total_supply': Decimal(pool['totalSupply']),
            'reserves_usd': reserves_usd,
            'tokens': tokens,
            'tx': None,
            'tx_type': None,
            'block': block,
            'date': datetime.utcfromtimestamp(timestamp)
        }

    @staticmethod
    def _merge_snaps_and_staked(snaps, staked) -> List[PoolShareSnapshot]:
        # Every staked snap has the corresponding liquidity position snap
        # (increase in staked LP balance always results in the equal decrease
        # in the normal snap) - sum LP balances of snaps at the same block
        raw_merged = snaps + staked

        # Group them by pool id
        pool_dict = {}
        for snap in raw_merged:
            block, pool_id = snap['block'], snap['pool_id']

            if pool_id not in pool_dict:
                pool_dict[pool_id] = {}

            if block in pool_dict[pool_id]:
                if pool_dict[pool_id][block]['liquidity_token_balance'] == 0 \
                        or snap['liquidity_token_balance'] == 0:
                    # When 1 of the 2 snapshots in the same block have 0
                    # balance it means that the snapshots were created because
                    # user deposited all his LP tokens into the staking
                    # contract. Such event is useless for UI and hence I can
                    # delete the snapshots
                    del pool_dict[pool_id][block]
                else:
                    pool_dict[pool_id][block]['liquidity_token_balance'] \
                        += snap['liquidity_token_balance']
            else:
                pool_dict[pool_id][block] = snap

        merged_snaps = []
        for summed_snaps in pool_dict.values():
            for snap in summed_snaps.values():
                merged_snaps.append(snap)
        return merged_snaps
