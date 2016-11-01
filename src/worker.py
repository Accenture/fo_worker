#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from multiprocessing import Pool, ProcessError
from time import time
from os import getpid
from bson.objectid import ObjectId
from pymongo import MongoClient
from pika import BlockingConnection, ConnectionParameters
from runner import run
import config as env
import datetime as dt
import socket
import logging
from logging.config import fileConfig

#
# ENV VARS
#

RMQ_HOST = env.RMQ_HOST
RMQ_PORT = env.RMQ_PORT
MONGO_HOST = env.MONGO_HOST
MONGO_PORT = env.MONGO_PORT
MONGO_NAME = env.MONGO_NAME

#
# MODULE CONSTANTS
#

RMQ_QUEUE_SOURCE = 'task_queue'
RMQ_QUEUE_SINK = 'notify_queue'
RMQ_SLEEP = 10
TASK_TIMEOUT = 3600 * 48

#
# LOGGING
#

fileConfig('logging_config.ini')

logger = logging.getLogger(__name__)

def main():

    def process_job(func, *args):
        pool = Pool(processes=1, maxtasksperchild=1)
        result = pool.apply_async(func, args=args)
        deadline = time() + TASK_TIMEOUT
        while True:  # Need this sleep loop since pika is not thread safe

            if deadline - time() <= 0:
                pool.terminate()
                pool.join()
                raise TimeoutError

            if not result.ready():
                logging.info('Sleeping...')
                mq_conn.sleep(RMQ_SLEEP)
                continue

            pool.terminate()
            pool.join()

            if result.successful():
                break
            else:
                raise ProcessError

    def reconcile_db(db, id):
        db.jobs.update_one(
            {'_id': ObjectId(id)},
            {
                "$set": {
                    "status": "failed"
                }
            }
        )
        print('RECONCILE DB')

    def updateEnd(db, id):
        end_time = dt.datetime.utcnow()
        db.jobs.update_one(
            {'_id': ObjectId(id)},
            {
                "$set": {
                    'optimization_end_time': end_time
                }
            }
        )
        print('update failed time')
    def divByZero(db, id):
        db.jobs.update_one(
            {'_id': ObjectId(id)},
            {
                "$set": {
                    "status": "Unbounded"
                }
            }
        )
        print('RECONCILE DB')

    def send_notification(ch, id, status):
        res = dict(user_id=id, status=status)
        ch.basic_publish(exchange='',
                 routing_key=RMQ_QUEUE_SINK,
                 body=json.dumps(res))

    # logging.basicConfig(level=logging.INFO,
    #                     format=LOG_FORMAT,
    #                     filename=LOG_FILE)
    #
    # LOGGER.info('main thread pid: %s', getpid())

    db_conn = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = db_conn[MONGO_NAME]
    mq_conn = BlockingConnection(ConnectionParameters(host=RMQ_HOST,
                                                      port=RMQ_PORT))
    ch = mq_conn.channel()
    ch.queue_declare(queue=RMQ_QUEUE_SOURCE, durable=True)
    ch.queue_declare(queue=RMQ_QUEUE_SINK, durable=False)
    ch.basic_qos(prefetch_count=1)
   
    messages = ch.consume(queue=RMQ_QUEUE_SOURCE, arguments={'server': socket.gethostname(), 'pid': getpid()})

    try:
        for method, properties, body in messages:
            try:
                print("Running new job")
                process_job(run, body)
                userId = json.loads(body.decode('utf-8'))['userId']
                send_notification(ch, userId, 'done')
            except (ZeroDivisionError):
                logging.error('Division by Zero Error')
                ch.basic_reject(delivery_tag=method.delivery_tag,
                                requeue=False)
                _id = json.loads(body.decode('utf-8'))['_id']
                userId = json.loads(body.decode('utf-8'))['userId']
                divByZero(db, _id)
                send_notification(ch, userId, 'failed')
            except (TimeoutError, ProcessError):
                print("Job has failed")
                logging.error('Process exited unexpectedly')
                ch.basic_reject(delivery_tag=method.delivery_tag,
                                requeue=False)
                _id = json.loads(body.decode('utf-8'))['_id']
                userId = json.loads(body.decode('utf-8'))['userId']
                updateEnd(db, _id)
                reconcile_db(db, _id)
                send_notification(ch, userId, 'failed')
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)
    except KeyboardInterrupt:
        ch.cancel()
        ch.close()

    mq_conn.close()
    db_conn.close()

if __name__ == '__main__':
    main()
