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
# from TierKey import tierKeyCreate
# from TierOptim import tierDef


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
        # print(" [x] Received %r" % body)

        msg = json.loads(body.decode('utf-8'))
        # Find job initially
        job = db.jobs.find_one({'_id': ObjectId(msg['_id'])})
        job_id = job['_id']
        job_status = job['status']
        # print(job_status)

        '''
        Fetch (twice) and write once to the document by updating status 
        '''
        def status_update(status):
            # TODO: can find_one_and_update return json? minimize read/write to DB
            db.jobs.find_one_and_update(
                {'_id': job_id},
                {
                    "$set": {
                        "status": status
                    }
                }
            )
            # Doing double the work... has to be a better way!
            job = db.jobs.find_one({'_id': ObjectId(msg['_id'])})
            job_status = job['status']
            return job_status # pending, opitimizing, ...

        def write_res(art_name,res_id):
             db.jobs.find_one_and_update(
                {'_id': job_id},
                {
                    "$set": {
                        "artifactResults": {
                            art_name:res_id
                        }
                    }
                }
            )
        
        def create_new_res_artifact():
            artifact_id = fs.put(file.read(),
                                userId=current_user['id'],
                                filename=filename,
                                # shape=df.shape,
                                # factors=list(df.columns.levels[0]),
                                description=request.form.get('description', None),
                                # spaceUnit=inferred_space_unit,
                                **inspection
                                )

            new_artifact = db.fs.files.find_one({'_id': artifact_id})
                
        # Hardik Code to parse out information
        def fetch_artifact(artifact_id):
            file = fs.get(ObjectId(artifact_id))
            file = pd.read_csv(file,header=0)
            file=file.dropna()
            return file

        # TODO: turn all these if statements into a consolodated func
        if job_status == 'pending' or job_status == 'preoptimizing':
            status_update('preoptimizing')
            '''
            on failure we must make sure we log the fail and send a msg back to mq
            '''
            # raise Exception('FAIL!') 
            # fixtureArtifact=fetch_artifact(msg["artifacts"]["spaceArtifactId"]).set_index("Store")
            # transactionArtifact=fetch_artifact(msg["artifacts"]["salesArtifactId"]).set_index("Store #")
            # opt_amt = preoptimize(fixtureArtifact,transactionArtifact,float(msg["metricAdjustment"]),float(msg["salesPenetrationThreshold"]),msg["optimizedMetrics"],msg["increment"])
            write_res('masterResId',51535125)
            '''
            Write res to database for next step in optimize to use...
            '''
        
        job_status = status_update('optimizing') 
        
        if job_status == 'optimizing':
            '''
            on failure we must make sure we log the fail and send a msg back to mq
            '''
            print('For the sake of python...')
            # raise Exception('FAIL!')
            # optimize(opt_amt,msg["tierCounts"],msg["spaceBounds"],msg["increment"])
            '''
            Write res to database for next step in optimize to use...
            '''

        status_update('done')
       
        # fixtureArtifact=fetch_artifact(msg["artifacts"]["spaceArtifactId"]).set_index("Store")
        # print(fixtureArtifact.columns)
        # transactionArtifact=fetch_artifact(msg["artifacts"]["salesArtifactId"]).set_index("Store #")
        # print(transactionArtifact.columns)
        # opt_amt = preoptimize(fixtureArtifact,transactionArtifact,float(msg["metricAdjustment"]),float(msg["salesPenetrationThreshold"]),msg["optimizedMetrics"],msg["increment"])
        # optimize(opt_amt,msg["tierCounts"],msg["spaceBounds"],msg["increment"])
        
        
        
        
        # set status to done
        # db.jobs.update_one(
        #     {'_id': job['_id']},
        #     {
        #         "$set": {
        #             "status": "done"
        #         }
        #     }
        # )

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
