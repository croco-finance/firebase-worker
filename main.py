import logging
import time

from src.balancer.balancer import Balancer
from src.controller import Controller

if __name__ == '__main__':
    logging.basicConfig(filename='balancer.log',
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)
    controller = Controller(Balancer())
    while True:
        try:
            controller.update_snaps(query_limit=100)
            controller.update_yields(query_limit=30)
            # controller.update_pools()
            break
        except Exception as e:
            logging.error(f'CONTROL LOOP EXCEPTION OCCURRED: {e}')
            time.sleep(150)
