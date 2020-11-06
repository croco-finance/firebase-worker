from web3 import Web3

from src.subgraph import SubgraphReader


def get_eth_client() -> Web3:
    return Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/'
                                  '5ea9c140dbf043a28849c180793486d7'))


def get_current_block() -> int:
    query = '''
    {
        currentBlock(id: "CURRENT"){
            number
        }
    }
    '''
    rewards_graph = SubgraphReader(
        '/subgraphs/name/benesjan/dex-rewards-subgraph')
    # 1. Get the positions and snapshots
    data = rewards_graph.query(query, {})['data']
    # Set current block info on current positions
    return int(data['currentBlock']['number'])
