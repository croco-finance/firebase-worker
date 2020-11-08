from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict

import attr


@attr.s(auto_attribs=True)
class CurrencyField(object):
    symbol: str
    name: str
    contract_address: str
    platform: str

    def to_serializable(self) -> Dict:
        return {
            'symbol': self.symbol,
            'name': self.name,
            'contractAddress': self.contract_address,
            'platform': self.platform,
        }


@attr.s(auto_attribs=True)
class PoolToken(object):
    token: CurrencyField
    weight: Decimal  # weights are normalized to (0,1)
    reserve: Decimal
    price_usd: Decimal

    def to_serializable(self) -> Dict:
        return {
            'token': self.token.to_serializable(),
            'weight': str(self.weight),
            'reserve': str(self.reserve),
            'priceUsd': str(self.price_usd)
        }


class Exchange(Enum):
    UNI_V2 = 0
    BALANCER = 1


@attr.s(auto_attribs=True)
class Pool(object):
    id: str
    exchange: Exchange
    liquidity_token_total_supply: Decimal
    tokens: List[PoolToken]

    def to_serializable(self) -> Dict:
        return {
            'exchange': str(self.exchange.name),
            'liquidityTokenTotalSupply': str(self.liquidity_token_total_supply),
            'tokens': [token.to_serializable() for token in self.tokens],
        }


@attr.s(auto_attribs=True)
class ShareSnap(object):
    id: str
    exchange: Exchange
    user_addr: str
    pool_id: str
    liquidity_token_balance: Decimal
    liquidity_token_total_supply: Decimal
    tokens: List[PoolToken]
    block: int
    timestamp: int
    tx_hash: str
    tx_cost_eth: Decimal
    # Optional because it's more efficient to populate the prices after having the instance
    eth_price: Optional[Decimal]
    # Set for snaps which were at the time eligible for yield reward if the price was available in the graph
    yield_token_price: Optional[Decimal]

    def to_serializable(self) -> Dict:
        serializable = {
            'id': self.id,
            'exchange': str(self.exchange.name),
            'liquidityTokenBalance': str(self.liquidity_token_balance),
            'liquidityTokenTotalSupply': str(self.liquidity_token_total_supply),
            'tokens': [token.to_serializable() for token in self.tokens],
            'block': self.block,
            'timestamp': self.timestamp,
            'txHash': self.tx_hash,
            'txCostEth': str(self.tx_cost_eth),
            'ethPrice': str(self.eth_price)
        }
        if self.yield_token_price:
            serializable['yieldTokenPrice'] = str(self.yield_token_price)
        return serializable


@attr.s(auto_attribs=True)
class YieldReward(object):
    id: str
    user_addr: str
    token: CurrencyField
    amount: Decimal
