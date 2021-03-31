from typing import Dict

from src.shared.type_definitions import StakingService
from src.uniswap_v2.type_definitions import YieldPool

yield_pools: Dict[StakingService, YieldPool] = {
    StakingService.UNI_V2: YieldPool(
        pool_id='0xd3d2e2692501a5c9ca623199d38826e513033a17',
        subgraph_name='benesjan/uniswap-v2',
        firs_block=10876348
    ),
    StakingService.SUSHI: YieldPool(
        pool_id='0x795065dcc9f64b5614c407a6efdc400da6221fb0',
        subgraph_name='benesjan/sushi-swap',
        firs_block=10829340
    ),
    # StakingService.SUSHI: YieldPool(
    #     pool_id='0xce84867c3c02b05dc570d0135103d3fb9cc19433',
    #     subgraph_name='uniswap/uniswap-v2',
    #     firs_block=10736320
    # ),
    StakingService.INDEX: YieldPool(
        pool_id='0x3452a7f30a712e415a0674c0341d44ee9d9786f9',
        subgraph_name='benesjan/uniswap-v2',
        firs_block=11004221
    ),
    StakingService.PANCAKE: YieldPool(
        pool_id='0xa527a61703d82139f8a06bc30097cc9caa2df5a6',
        subgraph_name='benesjan/pancakeswap',
        firs_block=695058  # block of the deployment of the pancake pool
    )
}
