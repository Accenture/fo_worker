#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json

import gridfs
import pandas as pd
import pika
from bson.objectid import ObjectId
from pymongo import MongoClient

import config
from FixtureOptimization.CurveFitting import curveFittingBS
from FixtureOptimization.ksMerging import ksMerge
from FixtureOptimization.optimizerR5 import optimize
from FixtureOptimization.outputFunctions import createLong, createWide, createDrillDownSummary, createTieredSummary
from FixtureOptimization.preoptimizerEnh import preoptimizeEnh

# from TierKey import tierKeyCreate
# from TierOptim import tierDef

JOB_STATUS = {
    'QUEUED':'QUEUED',
    'RUNNING':'RUNNING',
    'DONE':'DONE'
}

def main():
    def make_serializable(db_object):
        if '_id' in db_object:
            db_object['_id'] = str(db_object['_id'])
        if 'uploadDate' in db_object:
            db_object['uploadDate'] = db_object['uploadDate'].isoformat()
        return db_object

    db = MongoClient(config.MONGO_CON)['app']
    fs = gridfs.GridFS(db)

    # my_test_file = fs.get(ObjectId("577eabb51d41c808371a6092")).read()
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=config.RABBIT_URL))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='notify_queue', durable=False)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        # print(" [x] Received %r" % body)

        msg = json.loads(body.decode('utf-8'))
        # Find job to check status of job
        job = db.jobs.find_one({'_id': ObjectId(msg['_id'])})
        try:
            job_id = job['_id']
        except TypeError as e:
            print('Job Not Found')
            return False

        # current_user = job['userId']
        # job_status = job['status']

        db.jobs.update_one(
            {'_id': job['_id']},
            {
                "$set": {
                    "status": "running"
                }
            }
        )

        def fetchTransactions(artifact_id):
            file = fs.get(ObjectId(artifact_id))
            file = pd.read_csv(file,header=None)
            return file

        def fetchSpace(artifact_id):
            file = fs.get(ObjectId(artifact_id))
            file = pd.read_csv(file,header=0,dtype={'Store': object},skiprows=[1])
            return file

        def fetchExit(artifact_id):
            file = fs.get(ObjectId(artifact_id))
            file = pd.read_csv(file,header=0,skiprows=[1])
            return file

        Stores=msg['salesStores']
        Categories=msg['salesCategories']

        try:
            # futureSpace=primaryMung(fetch_artifact(msg["artifacts"]["futureSpaceId"]))
            futureSpace=fetchSpace(msg["artifacts"]["futureSpaceId"])
            print("Future Space was Uploaded")
        except:
            futureSpace=None
            print("Future Space was not Uploaded")
        try:
            brandExitArtifact=fetchExit(msg["artifacts"]["brandExitArtifactId"])
            print("Brand Exit was Uploaded")
        except:
            print("Brand Exit was not Uploaded")
            brandExitArtifact=None

        msg["optimizationType"]='traditional'
        if (str(msg["optimizationType"]) == 'traditional'):
            dataMerged = ksMerge(msg['jobType'], fetchTransactions(msg["artifacts"]["salesArtifactId"]),
                                 fetchSpace(msg["artifacts"]["spaceArtifactId"]),
                                 brandExitArtifact, futureSpace)
            cfbsArtifact = curveFittingBS(dataMerged[0], msg['spaceBounds'], msg['increment'],
                                          msg['storeCategoryBounds'],
                                          float(msg["salesPenetrationThreshold"]), msg['jobType'],
                                          msg['optimizationType'])
            preOpt = preoptimizeEnh(dataMunged=dataMerged[1], mAdjustment=float(msg["metricAdjustment"]),
                                 salesPenThreshold = float(msg["salesPenetrationThreshold"]),
                                 optimizedMetrics = msg["optimizedMetrics"], increment=msg["increment"])
            # mPreOptCFBS = mergePreOptCF(cfbsArtifact, preOpt[['Store','Category','Penetration','Optimal Space']])
            # mPreOptCFBS = pd.merge(cfbsArtifact, preOpt[['Store','Category','Penetration','Optimal Space']],on=['Store','Category'])
            optimRes = optimize(job_id, msg['meta']['name'], Stores, Categories, msg["tierCounts"],
                                msg["spaceBounds"], msg["increment"], preOpt)
            # optimRes = optimize(msg['optimizationType'], msg['meta']['name'], Stores, Categories, msg['tierCounts'],
            #                     msg['increment'], msg['optimizedMetrics'], mPreOptCFBS)
            # except:
                # print(TypeError)
                # print("Traditional Optimization has Failed")
        if (msg["optimizationType"] == 'enhanced'):
        #     try:
            #     masterData=dataMerging(msg)
                # cfbsArtifact=curveFittingBS(masterData,spaceBounds,increment,optimizedMetrics['sales'],optimizedMetrics['profits'],optimizedMetrics['units'],msg['storeCategoryBounds'],msg['optimizationType'])
                # cfbsDict=cfbsArtifact.set_index(["Store","Product"])
                # preOpt = preoptimize(Stores=Stores,Categories=Categories,spaceData=fixtureArtifact,data=transactionArtifact,metricAdjustment=float(msg["metricAdjustment"]),salesPenetrationThreshold=float(msg["salesPenetrationThreshold"]),optimizedMetrics=msg["optimizedMetrics"],increment=msg
                # optimize(job_id,preOpt,msg["tierCounts"],msg["increment"],cfbsArtifact)
            # except:
            print("Ken hasn't finished development for that yet")
        # Call functions to create output information
        longOutput = createLong(cfbsArtifact[0], optimRes[1])
        wideOutput = createWide(longOutput, msg['jobType'], msg['optimizationType'])
        if msg['jobType'] == "tiered":
            summaryReturned = createTieredSummary(longOutput)
        else:  # since type == "Drill Down"
            summaryReturned = createDrillDownSummary(longOutput)



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
