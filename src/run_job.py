#!/usr/bin/env python
# -*- coding: utf-8 -*-


import datetime as dt
from pulp import *
import config as env
import json
import gridfs
import pandas as pd
import pika
from bson.objectid import ObjectId
from pymongo import MongoClient
from FixtureOptimization.CurveFitting import curveFittingBS
from FixtureOptimization.dataMerging import dataMerge
from FixtureOptimization.preoptimizer import preoptimize
from FixtureOptimization.optimizerTrad import optimizeTrad
from FixtureOptimization.optimizerEnh import optimizeEnh
from FixtureOptimization.outputFunctions import createLong, createWide, createDrillDownSummary, createTieredSummary, outputValidation
from pika import BlockingConnection, ConnectionParameters
from FixtureOptimization.SingleStoreOptimization import optimizeSingleStore
import logging
import traceback
from distutils.util import strtobool

import helper

import sys
sys.path.append('/vagrant/fo_api/src/')
from artifacts import ArtifactBuilder

from optparse import OptionParser

#
# ENV VARS
#

MONGO_HOST = env.MONGO_HOST
MONGO_PORT = env.MONGO_PORT
MONGO_NAME = env.MONGO_NAME
MONGO_USERNAME = env.MONGO_USERNAME
MONGO_PASSWORD = env.MONGO_PASSWORD
IS_AUTH_MONGO = env.IS_AUTH_MONGO

#
# MODULE CONSTANTS
#

RMQ_QUEUE_SINK = 'notify_queue'

#
# LOGGING
#

# file_dir = '/vagrant/fo_worker/test/test_optimizer_files/test/'


def run(body):

    msg = body

    worker_start_time = dt.datetime.utcnow()


    print("#####################################################################")
    print('beginning of ' + msg['meta']['name'] + ' date of ' + dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("#####################################################################")


    try:
        futureSpace = helper.fetchSpace(file_dir + msg["artifacts"]["futureSpaceId"])
        print("Future Space was Uploaded")
    except:
        futureSpace = None
        print("Future Space was not Uploaded")
    try:
        brandExitArtifact = helper.fetchExit(file_dir + msg["artifacts"]["brandExitArtifactId"])
        print("Brand Exit was Uploaded")
    except:
        print("Brand Exit was not Uploaded")
        brandExitArtifact = None
    if msg['jobType'] == 'unconstrained':
        msg['tierCounts'] = None

    dataMerged = dataMerge(jobName=msg['meta']['name'],jobType=msg['jobType'],optimizationType=msg['optimizationType'],transactions=helper.fetchTransactions(file_dir + msg["artifacts"]["salesArtifactId"]),
                            space=helper.fetchSpace(file_dir + msg["artifacts"]["spaceArtifactId"]),
                            brandExit=brandExitArtifact, futureSpace=futureSpace)
    print('finished data merging')
    preOpt = preoptimize(jobType=msg['jobType'], optimizationType=msg['optimizationType'], dataMunged=dataMerged[1],
                            mAdjustment=float(msg["metricAdjustment"]),
                            salesPenThreshold=float(msg["salesPenetrationThreshold"]),
                            optimizedMetrics=msg["optimizedMetrics"], increment=msg["increment"])
    print('finished preoptimize')
    if msg['optimizationType'] == 'traditional':
        print('going to the optimization')
        optimRes = optimizeTrad(jobName=msg['meta']['name'], Stores=msg['salesStores'],
                                Categories=msg['salesCategories'],
                                spaceBound=msg['spaceBounds'], increment=msg['increment'], dataMunged=preOpt,
                                salesPen=msg['salesPenetrationThreshold'], tierCounts = msg['tierCounts'])
        cfbsArtifact=[None,None]
        scaledAnalyticsID = None
    else:
        cfbsArtifact = curveFittingBS(dataMerged[0], msg['spaceBounds'], msg['increment'],
                                    msg['storeCategoryBounds'],
                                    float(msg["salesPenetrationThreshold"]), msg['jobType'],
                                    msg['optimizationType'])
        print('finished curve fitting')
        cfbsOptimal = optimizeSingleStore(cfbsArtifact[0].set_index(['Store', 'Category']), msg['increment'],
                                        msg['optimizedMetrics'])
        print('finished single store')
        optimRes = optimizeEnh(methodology=msg['optimizationType'], jobType=msg['jobType'], jobName=msg['meta']['name'],
                             Stores=msg['salesStores'], Categories=msg['salesCategories'],
                             increment=msg['increment'], weights=msg['optimizedMetrics'], cfbsOutput=cfbsOptimal[1],
                             preOpt=preOpt, salesPen=msg['salesPenetrationThreshold'], tierCounts=msg['tierCounts'])
    print('we just did the optimization')
    statusID = optimRes[0]
    print(statusID)
    print('Set the Status')
    if statusID == 'Optimal' or 'Infeasible':
        # Call functions to create output information
        print('Out of the optimization')
        longOutput = createLong(msg['jobType'],msg['optimizationType'], optimRes[1])
        print('Created Long Output')


        end_time = dt.datetime.utcnow()
        print('created the end time')

        print("Adding end time and output ids")
        print ([
            {
                "$set": {
                    'optimization_end_time': end_time,
                    "status": statusID,
                    "objectiveValue": optimRes[2],
                }
            }
        ])
    else:
        end_time = dt.datetime.utcnow()
        print('created the end time')

        print("Adding end time and output is")
        print([
            {
                "$set": {
                    'optimization_end_time': end_time,
                    "objectiveValue": optimRes[2]
                }
            }
        ])

    # logging.info('end of ' + msg['meta']['name'])

    print("#####################################################################")
    print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("#####################################################################")

    print("Job complete")

if __name__ == '__main__':
    # LOGGER.debug('hello from {}'.format(__name__))
    # logger.debug('hello from {}'.format(__name__))

    # Getting input json name from command line
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="input_json")
    parser.add_option("-d", "--dir", dest="input_dir")
    (options, args) = parser.parse_args()

    # Modify json structures by replacing filename hash locations
    # from MongoDB to local filename.
    file_dir = options.input_dir
    jsonFilename = file_dir + options.input_json 
    body = helper.loadJson(jsonFilename)
    # print (body['filenames'])
    # print (body['artifacts'])
    # helper.modifyJson(body)
    # print (body['filenames'])
    # print (body['artifacts'])

    meta = ArtifactBuilder.create(open(file_dir + 'Space_data.csv', 'r'), 'space')
    body['salesStores'] = meta['stores']
    body['salesCategories'] = meta['categories']
    body['spaceBounds'] = meta['extrema']

    # Trigger the new runner without logging any ID info.
    run(body)
