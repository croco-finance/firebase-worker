from typing import Dict

from src.shared.type_definitions import YieldPool, Exchange

yield_pools = {
    'UNI_V2': YieldPool(
        pool_id='0xd3d2e2692501a5c9ca623199d38826e513033a17',
        subgraph_name='uniswap/uniswap-v2',
        firs_block=10876348
    ),
    'SUSHI': YieldPool(
        pool_id='0x795065dcc9f64b5614c407a6efdc400da6221fb0',
        subgraph_name='benesjan/sushi-swap',
        firs_block=10829340
    ),
    # 'SUSHI': YieldPool(
    #     pool_id='0xce84867c3c02b05dc570d0135103d3fb9cc19433',
    #     subgraph_name='uniswap/uniswap-v2',
    #     firs_block=10736320
    # ),
    'INDEX': YieldPool(
        pool_id='0x4d5ef58aac27d99935e5b6b4a6778ff292059991',
        subgraph_name='uniswap/uniswap-v2',
        firs_block=10876348
    )
}
