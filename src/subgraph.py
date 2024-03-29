import logging
from urllib.parse import urljoin

import requests

from src.error_definitions import NonExistentUserException, NotIndexedBlockException


class SubgraphReader:
    """
    General read handler of subgraph's data.
    """

    def __init__(self, subgraph):
        if subgraph.startswith('http'):
            self.url = subgraph
        else:
            provider = 'https://api.thegraph.com/subgraphs/name/'
            # provider = 'http://graph.marlin.pro/subgraphs/name/'
            self.url = urljoin(provider, subgraph)

    def query(self, query, params=None):
        """
        Execute query, with optional parameters.
        """
        if params:
            query = self._pass_params(query, params)
        result = requests.post(self.url, json={'query': query}).json()
        if result and 'data' not in result:
            for error in result['errors']:
                if error['message'] == 'Null value resolved for non-null field `user`':
                    raise NonExistentUserException()
                elif 'Failed to decode `block.number`' in error['message']:
                    raise NotIndexedBlockException(f'Subgraph: {self.url}, message: {error["message"]}')
            logging.error(f'Request fetching failed. Result: {result},\nquery: {query}, subgraph: {self.url}')
        return result

    @staticmethod
    def _pass_params(query, params):
        """
        Pass params into query.
        """
        for param, value in params.items():
            query = query.replace(param, str(value))
        return query
