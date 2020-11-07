from typing import Iterable


def share_query_generator(txs: list) -> str:
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


def _eth_prices_query_generator(blocks: Iterable[int]) -> str:
    """
    Example return value:
    {
        t10692365: tokenPrice(block: { number: 10692365 }, id: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2") {
            price
        }
        t10880437: tokenPrice(block: { number: 10880437 }, id: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2") {
            price
        }
    }
    """
    yield '{'
    for block in blocks:
        yield f'''
            t{block}: tokenPrice(block: {{ number: {block} }}, id: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2") {{
                price
            }}
            '''
    yield '}'
