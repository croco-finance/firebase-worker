import attr


@attr.s(auto_attribs=True, slots=True)
class YieldPool(object):
    """
    An object which contains information about pair, from which the price data can be fetched
    """
    pool_id: str  # Address of the pool, from which the price will be obtained
    subgraph_name: str
    firs_block: int  # First block, in which the price is available
