import logging

from src.balancer.balancer import Balancer
from src.controller import Controller
from src.uniswap_v2.uniswap import Uniswap

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    for class_ in [Uniswap, Balancer]:
        controller = Controller(class_())
        controller.update_snaps()
