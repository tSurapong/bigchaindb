"""Peer connections management module.

"""

import logging
import threading

from pymongo import MongoClient

from bigchaindb import Bigchain
from bigchaindb.models import Transaction


logger = logging.getLogger(__name__)



def connect_to_peers():
    bigchain = Bigchain()
    connections = [MongoClient(peer) for peer in bigchain.peers]
    for c in connections:
        t = threading.Thread(target=listen_for_transactions, args=(c,))
        t.daemon = True
        t.start()


def connect():
    logger.info(100*'*')
    logger.info('connect to peers')
    connect_to_peers()


import time

import pymongo

def listen_for_transactions(db_client):
    oplog = db_client.local.oplog.rs
    first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(-1).next()
    logger.info(first)
    print(first)
    ts = first['ts']

    bigchain = Bigchain()
    while True:
        # For a regular capped collection CursorType.TAILABLE_AWAIT is the
        # only option required to create a tailable cursor. When querying the
        # oplog the oplog_replay option enables an optimization to quickly
        # find the 'ts' value we're looking for. The oplog_replay option
        # can only be used when querying the oplog.
        cursor = oplog.find(
            {'ts': {'$gt': ts}, 'ns': 'bigchain.backlog', 'op': 'i'},
            cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
            oplog_replay=True,
        )
        while cursor.alive:
            logger.info(100*'*')
            logger.info('cursor is alive')
            for doc in cursor:
                ts = doc['ts']
                logger.info(doc)
                logger.info('\n')

                # WRITE to backlog OR put into a queue/pool of txs
                try:
                    del doc['o']['_id']
                    del doc['o']['assignee']
                    del doc['o']['assignement_timestamp']
                except KeyError:
                    pass

                # basic validation (schema)
                try:
                    Transaction.from_dict(doc['o'])
                except Exception:
                    logger.error('tx basic validation failed')
                else:
                    logger.info('.............. tx is valid .................')
                    bigchain.write_transaction(doc['o'])

            # We end up here if the find() returned no documents or if the
            # tailable cursor timed out (no new documents were added to the
            # collection for more than 1 second).
            time.sleep(1)
        logger.info('cursor is gone .........................................')
