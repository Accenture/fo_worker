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

    # my_test_file = fs.get(ObjectId("577eabb51d41c808371a6092")).read()
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='notify_queue', durable=False)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        # print(" [x] Received %r" % body)

        msg = json.loads(body.decode('utf-8'))
        # Find job to check status of job
        job = db.jobs.find_one({'_id': ObjectId(msg['_id'])})
        current_user = job['userId']
        job_id = job['_id']
        job_status = job['status']
        # print(job_status)
        isPreop = False
        isOptimize = False
        # if either flag is True then that optimization piece will be where we start! otherwise if pending we will run the whole thang...
        if job_status == 'pending':
            isPreop = True
        elif job_status == 'optimizing':
            isOptimize = True
        else:
            isPreop = True

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

        '''
        Create new artifact that contains optimize result and append it to document model
        '''
        def create_new_res_artifact(artifact_name,file,filename):
            # TODO: is there a better way to show relationship between gridfs files and document models?
            artifact_id = fs.put(file,
                                userId=current_user,
                                filename='res-' + filename,
                                description=''
                                )
            new_artifact = db.fs.files.find_one({'_id': artifact_id})
            db.jobs.find_one_and_update(
                {'_id': job_id},
                {
                    "$set": {
                        "artifactResults": {
                            artifact_name:new_artifact['_id']
                        }
                    }
                }
            )
                
        # Hardik Code to parse out information
        def fetch_artifact(artifact_id):    
            file = fs.get(ObjectId(artifact_id))
            file = pd.read_csv(file,header=0)
            file=file.dropna()
            return file

        # if isPreop:
        #     status_update('preoptimizing')
        #     '''
        #     on failure we must make sure we log the fail and send a msg back to mq
        #     '''
             
        #     # fixtureArtifact=fetch_artifact(msg["artifacts"]["spaceArtifactId"]).set_index("Store")
        #     # transactionArtifact=fetch_artifact(msg["artifacts"]["salesArtifactId"]).set_index("Store")
        #     # opt_amt = preoptimize(fixtureArtifact,transactionArtifact,float(msg["metricAdjustment"]),float(msg["salesPenetrationThreshold"]),msg["optimizedMetrics"],msg["increment"])
        #     # create_new_res_artifact('spaceResId',my_test_file,'spaceArtifact')
        #     job_status = status_update('optimizing')
        #     isOptimize = True
        #     # raise Exception('FAIL!')
        #     '''
        #     Write res to database for next step in optimize to use...
        #     '''
        # if isOptimize:
        #     if not isPreop and isOptimize:
        #         print('Rabbit must have failed, but no worries we will start here!')
        #         # pull artifact from gridfs so optimzation can use it    
        #     else:
        #         print('Rabbit must be working smoothly for once...')
        #         # create_new_res_artifact('salesResId',my_test_file,'salesArtifact')
        #         # use what we have in the one worker life span
        #     # RUN OPTIMIZATION PIECE...
        
        # # TODO: (FIX) for some stupid reason create_new_res_artifact is overwritting on each time it runs...
        # # create_new_res_artifact('masterOpResId',my_test_file,'optimizedArtifact')
        # status_update('done')
        
        # TODO: turn all these if statements into a consolodated func
        # if job_status == 'pending' or job_status == 'preoptimizing':
        #     status_update('preoptimizing')
        #     '''
        #     on failure we must make sure we log the fail and send a msg back to mq
        #     '''
        #     # raise Exception('FAIL!') 
        #     # fixtureArtifact=fetch_artifact(msg["artifacts"]["spaceArtifactId"]).set_index("Store")
        #     # transactionArtifact=fetch_artifact(msg["artifacts"]["salesArtifactId"]).set_index("Store #")
        #     # opt_amt = preoptimize(fixtureArtifact,transactionArtifact,float(msg["metricAdjustment"]),float(msg["salesPenetrationThreshold"]),msg["optimizedMetrics"],msg["increment"])
        #     create_new_res_artifact('masterSpaceResId',my_test_file,'salesArtifact')
        #     '''
        #     Write res to database for next step in optimize to use...
        #     '''
        
        # job_status = status_update('optimizing') 
        
        # if job_status == 'optimizing':
        #     '''
        #     on failure we must make sure we log the fail and send a msg back to mq
        #     we also need to pull from mongo the document so we can continue optimzation
        #     DON'T PULL FROM MONGO UNLESS IT DIES AND PICKS UP FROM WHERE WE LEFT OFF.
        #     '''
        #     print('For the sake of python...')
        #     # raise Exception('FAIL!')
        #     # optimize(opt_amt,msg["tierCounts"],msg["spaceBounds"],msg["increment"])
        #     '''
        #     Write res to database for next step in optimize to use...
        #     '''

        # status_update('done')
        #What are we passing through the optimize params? is there anything?
        #Probably need to call the preoptimize function right here...
        #Then call optimize? or does optimize from preop call optimize...
        # if isinstance(msg["artifacts"]["futureSpaceId"],str):
        #     futureSpace=fetch_artifact(msg["artifacts"]["futureSpaceId"]).set_index("Store")
        # if isinstance(msg["artifacts"]["futureSpaceId"],str):
        #     brandExitArtifact=fetch_artifact(msg["artifacts"]["brandExitArtifactId"])
        fixtureArtifact=fetch_artifact(msg["artifacts"]["spaceArtifactId"]).set_index("Store")
        transactionArtifact=fetch_artifact(msg["artifacts"]["salesArtifactId"]).set_index("Store")
        if (msg["jobType"] == 'Traditional'):
            opt_amt = preoptimize(fixtureArtifact,transactionArtifact,futureSpace,brandExitArtifact,float(msg["metricAdjustment"]),float(msg["salesPenetrationThreshold"]),msg["optimizedMetrics"],msg["increment"])
            optimize(opt_amt,msg["tierCounts"],msg["spaceBounds"],msg["increment"])
        if (msg["jobType"] == 'Enhanced'):
            print("WIP")
            ### R Stuff
        # if (msg["optimizationType"] == 'Enhanced'):      
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
