from decimal import Decimal
from typing import Iterable, List


def _qet_pair_query(timestamp: int, pair_id: str, balance: Decimal,
                    blocks: dict) -> str:
    timestamp_key = f't{timestamp}'
    balance_ = f'{balance:f}'.replace('.', 'dot')
    block = blocks[timestamp_key]
    return f'''{timestamp_key}_{balance_}_{block}: pair(id:"{pair_id}", block: {{ number: {block} }}) {{
        id
        totalSupply
        reserve0
        reserve1
        reserveUSD
        token0 {{
            id
            symbol
            name
        }}
        token1 {{
            id
            symbol
            name
        }}
    }}
    '''


def _eth_prices_query_generator(blocks: Iterable[int]) -> str:
    """
    Example return value:
    {
        t10925018: bundle(id: "1", block: { number: 10925018 }) {
            price: ethPrice
        }
        t11113275: bundle(id: "1", block: { number: 11113275 }) {
            price: ethPrice
        }
    }
    """
    yield '{'
    for block in blocks:
        if block is None:
            # When the block is None, it means the snapshot is from the present
            # moment --> I'll fetch the most recent price
            yield '''
            tNone: bundle(id: "1") {
                price: ethPrice
            }'''
        else:
            yield f'''
            t{block}: bundle(id: "1", block: {{ number: {block} }}) {{
                price: ethPrice
            }}
            '''
    yield '}'


def _staked_query_generator(positions: List) -> str:
    """
    Example return value:
    {
        t1603466256_0dot000039532619811031_11113293: pair(id:"0xbb2b8038a1640196fbe3e38816f3e67cba72d940", block: { number: 11113293 }) {
            id
            totalSupply
            reserve0
            reserve1
            reserveUSD
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
       ...
    }
    """
    yield '{\n'
    for position in positions:
        timestamp = int(position["blockTimestamp"])
        pair_id = position["pool"]
        balance = Decimal(position["liquidityTokenBalance"])
        timestamp_key = f't{position["blockTimestamp"]}'
        blocks = {timestamp_key: position['blockNumber']}
        yield _qet_pair_query(timestamp, pair_id, balance, blocks)
    yield '}'
