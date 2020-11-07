from typing import List

from src.balancer.balancer import Balancer
from src.shared.type_definitions import ShareSnap
from src.uniswap_v2.uniswap import Uniswap


def upload_snaps(snaps: List[ShareSnap]):
    print(f"Uploading {len(snaps)} snaps")


def get_lowest_highest_block(snaps: List[ShareSnap]):
    lowest_, highest_ = 1000000000, 0
    for snap in snaps:
        if snap.block > highest_:
            highest_ = snap.block
        if snap.block < lowest_:
            lowest_ = snap.block
    return lowest_, highest_


if __name__ == '__main__':
    last_block = 11204701
    # instance = Uniswap()
    instance = Balancer()
    prev_lowest, prev_highest = 1000000000, 0
    for snaps in instance.fetch_new_snaps(last_block, query_limit=50):
        assert len(snaps) < 900, 'Reached dangerous amount of snaps in a batch' \
                                 '-> not all snaps might fit into the response for this reason' \
                                 '-> DECREASE QUERY LIMIT!'
        lowest, highest = get_lowest_highest_block(snaps)
        print(f'Lowest block: {lowest}, highest block: {highest}')
        assert prev_highest <= lowest, f'Blocks not properly sorted: ' \
                                       f'prev_highest: {prev_highest}, lowest: {lowest}'
        prev_lowest, prev_highest = lowest, highest
        upload_snaps(snaps)
