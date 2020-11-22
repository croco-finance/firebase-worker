from flask import Flask
from flask_cors import CORS

from src.balancer.balancer import Balancer
from src.controller import Controller
from src.workarounds.uniswap_matching_txs import UniMatchingTxs

app = Flask(__name__)
CORS(app)


@app.route('/update/<string:exchange>/<string:entity_type>/')
@app.route('/update/<string:exchange>/<string:entity_type>/<int:min_liquidity>/')
def update(exchange, entity_type, min_liquidity=None):
    if exchange == 'UNI_V2':
        controller = Controller(UniMatchingTxs(), snap_index=6)
        # controller = Controller(Uniswap())
    elif exchange == 'BALANCER':
        controller = Controller(Balancer())
    else:
        return '{"success": false, "exception": "Unknown exhchange type."}'
    try:
        if entity_type == 'snaps':
            controller.update_snaps(query_limit=100)
        elif entity_type == 'pools':
            if min_liquidity==None:
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
