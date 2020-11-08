import logging
import time

from src.balancer.balancer import Balancer
from src.controller import Controller
from src.uniswap_v2.uniswap import Uniswap

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    for class_ in [Uniswap, Balancer]:
    # for class_ in [Balancer]:
        controller = Controller(class_())
        while True:
            try:
                # controller.update_snaps()
                # controller.update_yields()
                controller.update_pools()
                break
            except Exception as e:
                print(f'chyba: {e}')
                time.sleep(300)
