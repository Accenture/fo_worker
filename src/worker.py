#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json
import pika
import time
from pymongo import MongoClient
import gridfs
from bson.objectid import ObjectId
import optimizer


def main():

    db = MongoClient()['app']
    fs = gridfs.GridFS(db)

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='notify_queue', durable=False)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        # consume
        print(" [x] Received %r" % body)

        msg = json.loads(body.decode('utf-8'))
        # job = db.jobs.find_one({'_id': ObjectId(body.decode('utf-8'))})
        job = db.jobs.find_one({'_id': ObjectId(msg['_id'])})

        # set status to working
        db.jobs.update_one(
            # {'_id': ObjectId(body.decode('utf-8'))},
            {'_id': job['_id']},
            {
                "$set": {
                    "status": "working"
                }
            }
        )

        # retrieve context
        # job

        # retrieve artifacts


        # do work
        time.sleep(2)
        # optimizer.optimize()

        # set status to done
        db.jobs.update_one(
            {'_id': job['_id']},
            {
                "$set": {
                    "status": "done"
                }
            }
        )

        res = dict(
            job_id=msg['_id'],
            user_id=msg['userId'],
            outcome='Success'
        )

        # send notification
        ch.basic_ack(delivery_tag=method.delivery_tag)
        channel.basic_publish(exchange='',
                              routing_key='notify_queue',
                              # body='Job: 123 requested by userId: 456 is done!',
                              body=json.dumps(res),
                              properties=pika.BasicProperties(
                                  # delivery_mode=2,  # make message persistent
                              ))

        print(" [x] Done")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue='task_queue')

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()


if __name__ == '__main__':
    main()
