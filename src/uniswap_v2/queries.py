from typing import Iterable, List


def _eth_prices_query_generator(block_heights: Iterable[int]) -> Iterable[str]:
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
    for block_height in block_heights:
        yield f'''
            t{block_height}: bundle(id: "1", block: {{ number: {block_height} }}) {{
                price: ethPrice
            }}
            '''
    yield '}'


def _staked_query_generator(staked: List) -> Iterable[str]:
    """
    Example return value:
    {
        b11113293_0xbb2b8038a1640196fbe3e38816f3e67cba72d940: pair(id:"0xbb2b8038a1640196fbe3e38816f3e67cba72d940", block: { number: 11113293 }) {
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
    for position in staked:
        pool_id, block = position["pool"], position['blockNumber']
        yield f'''b{block}_{pool_id}: pair(id:"{pool_id}", block: {{ number: {block} }}) {{
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
    yield '}'


def yield_reserves_query_generator(block_heights: Iterable[int], pair_id) -> Iterable[str]:
    """
    Used to compute the value of UNI.

    Example return value:
    {
        t10692365: pair(block: { number: 10692365 }, id: "0xd3d2e2692501a5c9ca623199d38826e513033a17") {
            reserve0
            reserveUSD
        }
        t10880437: pair(block: { number: 10880437 }, id: "0xd3d2e2692501a5c9ca623199d38826e513033a17") {
            reserve0
            reserveUSD
        }
    }
    """
    yield '{'
    for block_height in block_heights:
        yield f'''
            t{block_height}: pair(block: {{ number: {block_height} }}, id: "{pair_id}") {{
                reserve0
                reserveUSD
            }}
            '''
    yield '}'
