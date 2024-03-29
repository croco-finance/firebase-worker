from typing import Iterable


def share_query_generator(txs: list) -> Iterable[str]:
    yield "{"
    for tx in txs:
        user_address = tx['userAddress']['id']
        tx_cost = int(tx['gasUsed']) * int(tx['gasPrice'])
        yield f'''
        k{tx['tx']}_{tx['block']}_{tx['timestamp']}_{tx_cost}_{user_address}: poolShares(first: 1000, where: {{userAddress: "{user_address}", poolId: "{tx['poolAddress']['id']}"}}, block: {{ number: {tx['block']} }}) {{
            userAddress {{
                id
            }}
            balance
            poolId {{
                id
                symbol
                totalWeight
                totalShares
                liquidity
                tokens {{
                    symbol
                    name
                    address
                    denormWeight
                    balance
                }}
            }}
        }}'''
    yield "\n}"


def _qet_pool_query(timestamp: int, pool_id: str, balance: str,
                    blocks: dict) -> str:
    timestamp_key = f't{timestamp}'
    balance_ = f'{balance:f}'.replace('.', 'dot')
    block = blocks[timestamp_key]
    return f'''{timestamp_key}_{balance_}_{block}: pool(id:"{pool_id}", block: {{ number: {block} }}) {{
    id
    totalWeight
    totalShares
    liquidity
    tokens {{
        symbol
        name
        address
        denormWeight
        balance
        }}
    }}'''


def _eth_prices_query_generator(block_heights: Iterable[int]) -> Iterable[str]:
    """
    Example return value:
    {
        b10692365: tokenPrice(block: { number: 10692365 }, id: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2") {
            price
        }
        b10880437: tokenPrice(block: { number: 10880437 }, id: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2") {
            price
        }
    }
    """
    yield '{'
    for block_height in block_heights:
        yield f'''
            b{block_height}: tokenPrice(block: {{ number: {block_height} }}, id: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2") {{
                price
            }}
            '''
    yield '}'


def _bal_prices_query_generator(block_heights: Iterable[int]) -> Iterable[str]:
    """
    Example return value:
    {
        t10692365: tokenPrice(block: { number: 10692365 }, id: "0xba100000625a3754423978a60c9317c58a424e3d") {
            price
        }
        t10880437: tokenPrice(block: { number: 10880437 }, id: "0xba100000625a3754423978a60c9317c58a424e3d") {
            price
        }
    }
    """
    yield '{'
    for block_height in block_heights:
        yield f'''
            t{block_height}: tokenPrice(block: {{ number: {block_height} }}, id: "0xba100000625a3754423978a60c9317c58a424e3d") {{
                price
            }}
            '''
    yield '}'
