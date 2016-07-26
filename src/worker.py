#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json
import pika
import time
from pymongo import MongoClient
import gridfs
from bson.objectid import ObjectId
import pandas as pd
from brandExitConversion import brandExitMung
from preoptimizer import preoptimize
from optimizer import optimize


def main():
    def make_serializable(db_object):
        if '_id' in db_object:
            db_object['_id'] = str(db_object['_id'])
        if 'uploadDate' in db_object:
            db_object['uploadDate'] = db_object['uploadDate'].isoformat()
        return db_object

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
        # job = db.jobs.find_one({'_id': ObjectId(msg['_id'])})
        # print(job)
        # set status to working
        db.jobs.find_one_and_update(
            # {'_id': ObjectId(body.decode('utf-8'))},
            {'_id': msg['_id']},
            {
                "$set": {
                    "status": "working"
                }
            }
        )

        # print("################################################################\n"+"################################################################\n"+"################################################################\n")
        # print(type(msg))
        # print(type(msg["optimizedMetrics"]["salesPenetration"]))
        # print("################################################################\n"+"################################################################\n"+"################################################################\n")
        # retrieve context
        # Hardik Code to parse out information
        def fetch_artifact(artifact_id):
            file = fs.get(ObjectId(artifact_id))
            file = pd.DataFrame(list(file))
            print("**************************************************************\n"+"**************************************************************\n"+"**************************************************************\n")
            print (file.loc(0))
            print("**************************************************************\n"+"**************************************************************\n"+"**************************************************************\n")
            # file.set_index([0])
            # file.columns=file.iloc[0]
            # file.drop([0])
            return file

        
        #What are we passing through the optimize params? is there anything?
        #Probably need to call the preoptimize function right here...
        #Then call optimize? or does optimize from preop call optimize...
        # brandExitArtifact=fetch_artifact(job["artifacts"]["brandExitArtifactId"])
        # print('!!!!!')
        # print(msg)
        # print(msg["optimizedMetrics"])
        fixtureArtifact=fetch_artifact(msg["artifacts"]["salesArtifactId"])
        # print("\n\n\n"+str(fixtureArtifact.columns)+"\n\n\n")
        transactionArtifact=fetch_artifact(msg["artifacts"]["spaceArtifactId"])
        #brandExitArtifact=brandExitMung(fetch_artifact(msg["artifacts"]["brandExitArtifactId"]))
        opt_amt = preoptimize(fixtureArtifact,transactionArtifact,msg["metricAdjustment"],msg["salesPenetrationThreshold"],msg["optimizedMetrics"],msg["increment"])
        optmizedArtifactId = optimize(opt_amt,msg["tierCounts"],msg["spaceBounds"],100)
        
        #######################################################################################
        #Write optimized results back to database
        # def writeArtifact(artifact):
            # fs.put(artifact)
            # return
        #######################################################################################
        
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
