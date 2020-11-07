from typing import Iterable, List


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
        yield f'''
            t{block}: bundle(id: "1", block: {{ number: {block} }}) {{
                price: ethPrice
            }}
            '''
    yield '}'


def _staked_query_generator(staked: List) -> str:
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
