#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json

import gridfs
import pandas as pd
import pika
from bson.objectid import ObjectId
from pymongo import MongoClient

import config
from CurveFitting import curveFittingBS
from FixtureOptimization.ksMerging import ksMerge
from FixtureOptimization.mungingFunctions import mergePreOptCF
from FixtureOptimization.preoptimizerEnh import preoptimizeEnh
from optimizerR4 import optimize
from FixtureOptimization.outputFunctions import createLong, createWide, createDrillDownSummary, createTieredSummary

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

        def fetch_artifact(artifact_id):
            file = fs.get(ObjectId(artifact_id))
            file = pd.read_csv(file,header=None)
            return file

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

        #Test Files
        # rawDM = pd.read_csv('TEST_Data_Merging_Output_Adj.csv', header=0)
        # bounds = pd.read_csv('TEST_Bound_Input.csv',header=0)
        # cfbsArtifact = curveFittingBS(rawDM, bounds, msg['increment'], msg['storeCategoryBounds'],
        #                               float(msg["salesPenetrationThreshold"]), msg['jobType'], msg['optimizationType'])
        # masterData = dataMerged[0]
        # print(pd.unique(masterData.columns==rawDM.columns))
        # input()
        # print(pd.unique(masterData.index==rawDM.index))
        # input()
        # print('masterData types')
        # print(masterData.dtypes)
        # print('rawDM types')
        # print(rawDM.dtypes)
        # input()
        # for col in masterData.columns:
        #     print(col)
        #     print(pd.unique(masterData[col]==rawDM[col]))
        #     # if False in pd.unique(masterData[col]==rawDM[col]):
        #     #     print(dataMerged[col].dtypes)
        #     #     print(rawDM[col].dtypes)
        #     input()
        # dataMerged[0].to_csv('ksMergedRes.csv',sep=',',index=False)

        def primaryMung(df):
            df['Store'].astype(int)
            df.set_index('Store',drop=True,inplace=True)
            return df

        fixtureArtifact=fetch_artifact(msg["artifacts"]["spaceArtifactId"])
        transactionArtifact=fetch_artifact(msg["artifacts"]["salesArtifactId"])
        # Stores=.to_numeric
        Stores=msg['salesStores']
        # Stores=fixtureArtifact[0].reindex(fixtureArtifact.index.drop([0,1])).reset_index(drop=True).rename('Stores').astype(int).values
        Categories=msg['salesCategories']
        # Categories=fixtureArtifact.loc[0][3::,].reset_index(drop=True).rename('Categories')

        try:
            # futureSpace=primaryMung(fetch_artifact(msg["artifacts"]["futureSpaceId"]))
            futureSpace=primaryMung(fetchSpace(msg["artifacts"]["futureSpaceId"]))
            print("Future Space was Uploaded")
        except:
            futureSpace=None
            print("Future Space was not Uploaded")
        try:
            # brandExitArtifact=(fetch_artifact(msg["artifacts"]["brandExitArtifactId"]))
            brandExitArtifact=fetchExit(msg["artifacts"]["brandExitArtifactId"])
            print("Brand Exit was Uploaded")
            # brandExitArtifact=brandExitMung(brandExitArtifact,Stores,Categories)
            print("Brand Exit Munged")
        except:
            print("Brand Exit was not Uploaded")
            brandExitArtifact=None
        # transactionArtifact = primaryMung(fetchTransactions(msg["artifacts"]["salesArtifactId"]))
        # fixtureArtifact = primaryMung(fetchSpace(msg["artifacts"]["spaceArtifactId"]))

        msg["optimizationType"]='traditional'
        if (str(msg["optimizationType"]) == 'traditional'):
            dataMerged = ksMerge(msg['jobType'], fetchTransactions(msg["artifacts"]["salesArtifactId"]),
                                 fetchSpace(msg["artifacts"]["spaceArtifactId"]),
                                 fetchExit(msg["artifacts"]["brandExitArtifactId"]),
                                 fetchSpace(msg["artifacts"]["futureSpaceId"]))
            cfbsArtifact = curveFittingBS(dataMerged[0], msg['spaceBounds'], msg['increment'],
                                          msg['storeCategoryBounds'],
                                          float(msg["salesPenetrationThreshold"]), msg['jobType'],
                                          msg['optimizationType'])
            # preOpt = preoptimize(Stores=Stores, Categories=Categories, spaceData=fixtureArtifact,
            #                      data=transactionArtifact, mAdjustment=float(msg["metricAdjustment"]),
            #                      salesPenThreshold=float(msg["salesPenetrationThreshold"]),
            #                      optimizedMetrics=msg["optimizedMetrics"], increment=msg["increment"],
            #                      brandExitArtifact=brandExitArtifact, newSpace=futureSpace)
            preOpt = preoptimizeEnh(dataMunged=dataMerged[1], mAdjustment=float(msg["metricAdjustment"]),
                                 salesPenThreshold = float(msg["salesPenetrationThreshold"]),
                                 optimizedMetrics = msg["optimizedMetrics"], increment=msg["increment"])
            mPreOptCFBS = mergePreOptCF(cfbsArtifact, preOpt)
            optimRes = optimize(job_id, msg['meta']['name'], Stores, Categories, preOpt, msg["tierCounts"],
                                msg["spaceBounds"], msg["increment"], fixtureArtifact, brandExitArtifact)
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
        longOutput = createLong(mPreOptCFBS, optimRes)
        wideOutput = createWide(longOutput, msg['jobType'], msg['optimizationType'])

        if msg['optimizationType'] == "tiered":
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
