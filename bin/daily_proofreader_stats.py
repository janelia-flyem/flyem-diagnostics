"""
daily_proofreader_stats.py
Retrieve user/operation proofreading stats for the last 24 hours from
Elasticsearch and publish to a Kafka topic. By default, this only looks
at stats for emdata4. This can be changed by specyfying an index to read from.
This program is meant to run once per day (preferably just before midnight).
"""
import argparse
import datetime
import json
import sys
import time
import colorlog
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# General
OPERATIONS = ['cleave', 'merge', 'split-supervoxel']


def call_responder(server, endpoint, payload=''):
    """
    Call a responder
    Keyword arguments:
        server: server
        endpoint: REST endpoint
        psyload: POST payload
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        if payload:
            headers = {"Content-type": "application/json",
                       "Authorization": "Bearer " + CONFIG[server]['bearer']}
            req = requests.post(url, headers=headers, json=payload)
        else:
            req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code not in [200, 404]:
        LOGGER.critical('Status: %s', str(req.status_code))
        sys.exit(-1)
    else:
        return req.json()


def fetch_counts(datestruct):
    """
    Fetch operation counts by user
    Keyword arguments:
        datestruct: structure for a single timestamp containing users/operation
    """
    response = call_responder('elasticsearch', 'query/daily_proofreader_hits')
    for rec in response['result']['hits']['hits']:
        data = rec['_source']
        if data['user'] not in datestruct:
            datestruct[data['user']] = {"cleave": 0, "merge": 0,
                                        "split-supervoxel": 0}
        if '/cleave/' in data['uri']:
            datestruct[data['user']]['cleave'] += 1
        elif '/merge' in data['uri']:
            datestruct[data['user']]['merge'] += 1
        elif '/split-supervoxel' in data['uri']:
            datestruct[data['user']]['split-supervoxel'] += 1


def process_data():
    """
    Load information from last 24 hours, filter, and publish to Kafka
    """
    if ARG.WRITE:
        producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                 key_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                 bootstrap_servers=BROKERS)
    datestruct = dict()
    fetch_counts(datestruct)
    uts = dict()
    for sub in CAT_SUBGROUPS:
        for user in CAT_SUBGROUPS[sub]:
            uts[user] = sub
    epoch_seconds = time.time()
    for user in sorted(datestruct):
        payload = {'time': epoch_seconds}
        wuser = user.split('@')[0]
        workday = call_responder('config', 'config/workday/' + wuser)
        payload['user'] = wuser
        if 'config' in workday:
            payload['organization'] = workday['config']['organization']
            if payload['organization'] == 'Connectome Annotation Team':
                payload['subgroup'] = uts[wuser] if wuser in uts else ''
        else:
            LOGGER.warning("Could not find user %s", wuser)
            payload['organization'] = 'unknown'
        for key in OPERATIONS:
            payload['operation'] = key
            payload['count'] = datestruct[user][key]
            if ARG.WRITE:
                LOGGER.debug(json.dumps(payload))
                future = producer.send(ARG.TOPIC, payload, str(datetime.datetime.now()))
                try:
                    future.get(timeout=10)
                except KafkaError:
                    LOGGER.critical("Failed publishing to %s", ARG.TOPIC)
            else:
                LOGGER.info(json.dumps(payload))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Write proofreader daily stats to Kafka topic")
    PARSER.add_argument('--index', dest='INDEX', action='store',
                        default='emdata4_*dvid_activity-*',
                        help='ES index to read from [emdata4_*dvid_activity-*]')
    PARSER.add_argument('--topic', dest='TOPIC', action='store',
                        default='proofreader_daily_stats',
                        help='Kafka topic to publish to [nptest]')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write record to config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    # Initialize
    CONFIG = call_responder('config', 'config/rest_services')['config']
    BROKERS = call_responder('config', 'config/servers')['config']['Kafka']['broker_list']
    CAT_SUBGROUPS = call_responder('config', 'config/cat_subgroups')['config']
    # Process messages for last 24 hours
    process_data()
    # Done
    sys.exit(0)
