import logging
import requests

import nsq.config.lookup

_logger = logging.getLogger(__name__)


class Lookup(object):
    def __init__(self, lookup_host_prefixes):
        if not lookup_host_prefixes:
            raise ValueError("At least one lookup-host must be given.")

        self.__lookup_host_prefixes = lookup_host_prefixes

    def get_servers(self, topic):
        servers = set()
        for lookup_host_prefix in self.__lookup_host_prefixes:
            replacements = {
                'lookup_host_prefix': lookup_host_prefix,
                'topic': topic,
            }
            
            url = nsq.config.lookup.LOOKUP_SERVER_URL_TEMPLATE % replacements

            _logger.debug("Polling lookup server: [%s]", url)

            r = requests.get(url)
            r.raise_for_status()

            data = r.json()

            for producer_info in data['data']['producers']:
                servers.add(
                    (producer_info['broadcast_address'].lower(), 
                     int(producer_info['tcp_port'])))

        return list(servers)
