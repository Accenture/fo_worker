#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pymongo import MongoClient
import gridfs
from bson.objectid import ObjectId
import pandas as pd
import json
import datetime as dt
from brandExitConversion import brandExitMung
from preoptimizerR4 import preoptimize
from optimizerR4 import optimize
import config as env

#
# ENV VARS
#

MONGO_HOST = env.MONGO_HOST
MONGO_PORT = env.MONGO_PORT
MONGO_NAME = env.MONGO_NAME

#
# LOGGING
#

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOG_FILE = None
LOGGER = logging.getLogger(__name__)


def run(body):

    db = MongoClient(host=MONGO_HOST,
                     port=MONGO_PORT)[MONGO_NAME]
    fs = gridfs.GridFS(db)

    def fetch_artifact(artifact_id):
        file = fs.get(ObjectId(artifact_id))
        file = pd.read_csv(file, header=0)
        return file

    msg = json.loads(body.decode('utf-8'))
    # Find job to check status of job
    job = db.jobs.find_one({'_id': ObjectId(msg['_id'])})
    try:
        job_id = job['_id']
    except TypeError:
        print('Job Not Found')
        return False

    # current_user = job['userId']
    # job_status = job['status']
    worker_start_time = dt.datetime.today()
    db.jobs.update_one(
        {'_id': job['_id']},
        {
            "$set": {
                "status": "running",
                'worker_start_time': worker_start_time
            }
        }
    )

    def fetch_artifact(artifact_id):
        file = fs.get(ObjectId(artifact_id))
        file = pd.read_csv(file, header=None)
        return file

    def fetchTransactions(artifact_id):
        file = fs.get(ObjectId(artifact_id))
        file = pd.read_csv(file, header=None)
        return file

    def fetchSpace(artifact_id):
        file = fs.get(ObjectId(artifact_id))
        file = pd.read_csv(file, header=0, dtype={'Store': object}, skiprows=[1])
        return file

    def fetchExit(artifact_id):
        file = fs.get(ObjectId(artifact_id))
        file = pd.read_csv(file, header=0, skiprows=[1])
        return file

    dataMerged = ksMerge(msg['jobType'], fetchTransactions(msg["artifacts"]["salesArtifactId"]),
                         fetchSpace(msg["artifacts"]["spaceArtifactId"]),
                         fetchExit(msg["artifacts"]["brandExitArtifactId"]),
                         fetchSpace(msg["artifacts"]["futureSpaceId"]))

    # print(dataMerged.head())
    def primaryMung(df):
        df.columns = df.iloc[0].values
        df.drop(df.index[[0, 1]], axis=0, inplace=True)
        df.Store.astype(int)
        df.set_index('Store', drop=True, inplace=True)
        # df.set_index(df.Store.values.astype(int),drop=True,inplace=True)
        return df

    fixtureArtifact = fetch_artifact(msg["artifacts"]["spaceArtifactId"])
    transactionArtifact = fetch_artifact(msg["artifacts"]["salesArtifactId"])
    Stores = fixtureArtifact[0].reindex(fixtureArtifact.index.drop([0, 1])).reset_index(drop=True).rename(
        'Stores').astype(int).values
    print(type(Stores[0]))
    Categories = fixtureArtifact.loc[0][3::, ].reset_index(drop=True).rename('Categories')

    try:
        futureSpace = primaryMung(fetch_artifact(msg["artifacts"]["futureSpaceId"]))
        print("Future Space was Uploaded")
    except:
        futureSpace = None
        print("Future Space was not Uploaded")
    try:
        brandExitArtifact = (fetch_artifact(msg["artifacts"]["brandExitArtifactId"]))
        print("Brand Exit was Uploaded")
        brandExitArtifact = brandExitMung(brandExitArtifact, Stores, Categories)
        print("Brand Exit Munged")
    except:
        print("Brand Exit was not Uploaded")
        brandExitArtifact = None
    # mData=kbMerge(msg['jobType'], transactionArtifact, fixtureArtifact, futureSpace, brandExitArtifact)
    # masterData = dataMerging(msg["jobType"],transactionArtifact, fixtureArtifact, futureSpace, brandExitArtifact)
    transactionArtifact = primaryMung(transactionArtifact)
    fixtureArtifact = primaryMung(fixtureArtifact)
    # print(fixtureArtifact.head())
    # print(transactionArtifact.head())
    masterData = pd.read_csv('childrenOutput.csv', header=0)
    # bounds=pd.read_csv('src/bounds.csv',header=0)
    msg["optimizationType"] = 'traditional'
    if (str(msg["optimizationType"]) == 'traditional'):
        cfbsArtifact = curveFittingBS(masterData, msg['spaceBounds'], msg['increment'], msg['storeCategoryBounds'],
                                      float(msg["salesPenetrationThreshold"]), msg['jobType'], msg['optimizationType'])
        preOpt = preoptimize(Stores=Stores, Categories=Categories, spaceData=fixtureArtifact, data=transactionArtifact,
                             mAdjustment=float(msg["metricAdjustment"]),
                             salesPenThreshold=float(msg["salesPenetrationThreshold"]),
                             optimizedMetrics=msg["optimizedMetrics"], increment=msg["increment"],
                             brandExitArtifact=brandExitArtifact, newSpace=futureSpace)
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
    longOutput = createLong(mergedPreOptCFReturned, optimResult)
    wideOutput = createWide(Stores, Categories, optimResult, optReturned, penReturned, fixtureArtifact)

    if optimType == "Tiered":
        summaryReturned = createTieredSummary(longReturned)
    else:  # since type == "Drill Down"
        summaryReturned = createDrillDownSummary(longReturned)
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

    db.client.close()

    return json.dumps(res)


if __name__ == '__main__':
    LOGGER.debug('hello from {}'.format(__name__))
