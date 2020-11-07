from src.balancer.balancer import Balancer
from src.controller import Controller
from src.uniswap_v2.uniswap import Uniswap

if __name__ == '__main__':
    controller = Controller()
    for class_ in [Uniswap, Balancer]:
        controller.update(class_())
