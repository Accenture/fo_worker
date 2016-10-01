#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime as dt
import logging
from pulp import *
import config as env
import json
import gridfs
import pandas as pd
import pika
from bson.objectid import ObjectId
from pymongo import MongoClient
import config
from FixtureOptimization.CurveFitting import curveFittingBS
from FixtureOptimization.ksMerging import ksMerge
from FixtureOptimization.preoptimizerEnh import preoptimizeEnh
from FixtureOptimization.optimizerR5 import optimize
from FixtureOptimization.outputFunctions import createLong, createWide, createDrillDownSummary, createTieredSummary

# from TierKey import tierKeyCreate
# from TierOptim import tierDef

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

    Stores = msg['salesStores']
    Categories = msg['salesCategories']

    try:
        # futureSpace=primaryMung(fetch_artifact(msg["artifacts"]["futureSpaceId"]))
        futureSpace = fetchSpace(msg["artifacts"]["futureSpaceId"])
        print("Future Space was Uploaded")
    except:
        futureSpace = None
        print("Future Space was not Uploaded")
    try:
        brandExitArtifact = fetchExit(msg["artifacts"]["brandExitArtifactId"])
        print("Brand Exit was Uploaded")
    except:
        print("Brand Exit was not Uploaded")
        brandExitArtifact = None

    # msg["optimizationType"] = 'traditional'
    if (str(msg["optimizationType"]) == 'traditional'):
        dataMerged = ksMerge(msg['jobType'], fetchTransactions(msg["artifacts"]["salesArtifactId"]),
                             fetchSpace(msg["artifacts"]["spaceArtifactId"]),
                             brandExitArtifact, futureSpace)
        cfbsArtifact = curveFittingBS(dataMerged[0], msg['spaceBounds'], msg['increment'],
                                      msg['storeCategoryBounds'],
                                      float(msg["salesPenetrationThreshold"]), msg['jobType'],
                                      msg['optimizationType'])
        preOpt = preoptimizeEnh(dataMunged=dataMerged[1], mAdjustment=float(msg["metricAdjustment"]),
                                salesPenThreshold=float(msg["salesPenetrationThreshold"]),
                                optimizedMetrics=msg["optimizedMetrics"], increment=msg["increment"])
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
    longOutput = createLong(cfbsArtifact, optimRes[1])
    wideOutput = createWide(longOutput, msg['jobType'], msg['optimizationType'])
    if msg['jobType'] == "tiered":
        summaryReturned = createTieredSummary(longOutput)
    else:  # since type == "Drill Down"
        summaryReturned = createDrillDownSummary(longOutput)

    end_time = dt.datetime.utcnow()
    db.jobs.find_one_and_update(
        {'_id': job_id},
        {
            "$set": {
                'optimization_end_time': end_time,
                'optimzation_total_time': total_time,
                "artifactResults": {
                    'long_table':long_id,
                    'wide_table':wide_id,
                    'summary_report': summary_id
                }
            }
        }
    )

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
