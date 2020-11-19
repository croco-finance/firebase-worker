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

    # Save the log output to console as well:
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger().addHandler(console)

    controller = Controller(Balancer())
    while True:
        try:
            controller.update_snaps(query_limit=100)
            controller.update_yields(max_objects_in_batch=100)
            # controller.update_pools(max_objects_in_batch=100)
            logging.info(f'FINISHED SYNC')
            break
        except Exception as e:
            logging.error(f'CONTROL LOOP EXCEPTION OCCURRED: {e}')
            time.sleep(150)
