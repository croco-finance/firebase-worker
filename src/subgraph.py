import logging
from urllib.parse import urljoin

import requests


class SubgraphReader:
    """
    General read handler of subgraph's data.
    """

    def __init__(self, subgraph_url):
        # provider = 'https://api.thegraph.com//'
        provider = 'http://graph.marlin.pro/'
        self.url = urljoin(provider, subgraph_url)

    def query(self, query, params=None):
        """
        Execute query, with optional parameters.
        """
        if params:
            query = self._pass_params(query, params)
        result = None
        for i in range(10):
            result = requests.post(self.url, json={'query': query}).json()
            if result and 'data' in result:
                break
            else:
                logging.warning(f'Request fetching failed. Result: {result}')
        return result

    @staticmethod
    def _pass_params(query, params):
        """
        Pass params into query.
        """
        for param, value in params.items():
            query = query.replace(param, value)
        return query
