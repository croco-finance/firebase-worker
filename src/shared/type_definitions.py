from decimal import Decimal
from enum import Enum
from typing import List, Optional

import attr


@attr.s(auto_attribs=True)
class CurrencyField(object):
    symbol: str
    name: Optional[str]  # None in Balancer swaps
    contract_address: str
    platform: str


@attr.s(auto_attribs=True)
class PoolToken(object):
    token: CurrencyField
    weight: Decimal  # weights are normalized to (0,1)
    reserve: Decimal
    price_usd: Decimal


class Exchange(Enum):
    UNI_V2 = 0
    BALANCER = 1


@attr.s(auto_attribs=True)
class YieldReward(object):
    token: CurrencyField
    price: Decimal
    amount: Decimal


@attr.s(auto_attribs=True)
class ShareSnap(object):
    id: str
    exchange: Exchange
    user_addr: str
    pool_id: str
    liquidity_token_balance: Decimal
    liquidity_token_total_supply: Decimal
    reserves_usd: Decimal
    tokens: List[PoolToken]
    block: int
    timestamp: int
    # TODO: fill in the values once the graph is synced
    tx_hash: Optional[str]
    # TODO: fill in the values once the graph is synced
    tx_cost_eth: Optional[Decimal]
    # TODO: fill in the values once the graph is synced
    eth_price: Optional[Decimal]
