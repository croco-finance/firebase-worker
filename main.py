# [START gae_python38_app]
from flask import Flask

from src.balancer.balancer import Balancer
from src.controller import Controller
from src.shared.type_definitions import Exchange
from src.uniswap_v2.uniswap import Uniswap
from src.workarounds.uniswap_matching_txs import UniMatchingTxs

app = Flask(__name__)


@app.route('/update/<string:exchange>/<string:entity_type>/')
@app.route('/update/<string:exchange>/<string:entity_type>/<int:min_liquidity>/')
def update(exchange, entity_type, min_liquidity=None):
    if exchange == 'UNI_V2':
        # TODO: switch to Uniswap() once the graph is synced
        controller = Controller(UniMatchingTxs(), snap_index=6)
        # controller = Controller(Uniswap())
    elif exchange == 'BALANCER':
        controller = Controller(Balancer())
    elif exchange == 'SUSHI':
        controller = Controller(Uniswap(dex_graph_name='benesjan/sushi-swap', exchange=Exchange.SUSHI))
    else:
        return '{"success": false, "exception": "Unknown exhchange type."}'
    try:
        if entity_type == 'snaps':
            controller.update_snaps(max_objects_in_batch=100)
        elif entity_type == 'staked_snaps':
            controller.update_staked_snaps(max_objects_in_batch=100)
        elif entity_type == 'pools':
            if min_liquidity is None:
                return '{"success": false, "exception": "None min_liquidity URL parameter in update of pools."}'
            controller.update_pools(max_objects_in_batch=100, min_liquidity=min_liquidity)
        elif entity_type == 'yields':
            controller.update_yields(max_objects_in_batch=100)
        else:
            return '{"success": false, "exception": "Unknown entity type."}'
    except Exception as e:
        return f'{"success": false, "exception": {e}}'
    return '{"success": true}'


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
# [END gae_python38_app]
