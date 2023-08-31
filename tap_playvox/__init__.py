#!/usr/bin/env python3

import sys
import json
import singer

from tap_playvox.client import PlayvoxClient
from tap_playvox.endpoints import ENDPOINTS_CONFIG
from tap_playvox.discover import discover
from tap_playvox.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [ 
    "client_id",
    "client_secret",
    "subdomain",
    "start_date",
    "access_token",
    "token_expiry"
]

def do_discover(client):
    LOGGER.info('Testing authentication')
    try:
        client.get(ENDPOINTS_CONFIG.get('users').get('path'), endpoint='users')
    except:
        raise Exception('Error could not authenticate with Playvox')

    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')

@singer.utils.handle_top_exception(LOGGER)
def main():
    
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    
    with PlayvoxClient(parsed_args.config, parsed_args.config_path) as client:
        if parsed_args.discover:
            do_discover(client)
        else:
            state = parsed_args.state if parsed_args.state else {"bookmarks": {}}
            sync(client,
                 parsed_args.catalog,
                 state)
