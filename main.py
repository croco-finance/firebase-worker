from typing import List

from src.balancer.balancer import Balancer
from src.shared.type_definitions import ShareSnap
from src.uniswap_v2.uniswap import Uniswap


def upload_snaps(snaps: List[ShareSnap]):
    print(f"Uploading snaps, num snaps: {len(snaps)}")


def get_smallest_largest_block(snaps: List[ShareSnap]):
    smallest_, largest_ = 1000000000, 0
    for snap in snaps:
        if snap.block > largest_:
            largest_ = snap.block
        if snap.block < smallest_:
            smallest_ = snap.block
    return smallest_, largest_


if __name__ == '__main__':
    last_block = 11204701
    # instance = Uniswap()
    instance = Balancer()
    prev_smallest, prev_largest = 1000000000, 0
    for snaps in instance.fetch_new_snaps(last_block, query_limit=100):
        smallest, largest = get_smallest_largest_block(snaps)
        assert prev_largest <= smallest, f'Blocks not properly sorted: ' \
                                        f'prev_largest: {prev_largest}, smallest: {smallest}'
        prev_smallest, prev_largest = smallest, largest
        upload_snaps(snaps)
