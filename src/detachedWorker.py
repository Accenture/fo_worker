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
from helper import fetch_artifact as fetch_artifact
from helper import fetchTransactions as fetchTransactions
from helper import fetchSpace as fetchSpace
from helper import fetchLong as fetchLong
from helper import fetchExit as fetchExit

import sys
sys.path.append('/vagrant/fo_api/src/')
from artifacts import ArtifactBuilder

from optparse import OptionParser

import time

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
    # print ("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    # print ("file_dir: ", file_dir)

    worker_start_time = dt.datetime.utcnow()


    print("#####################################################################")
    print('beginning of ' + msg['meta']['name'] + ' date of ' + dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("#####################################################################")

    print ('before csv files loading in run line 74-115: ', time.time())
    try:
        sales = helper.fetchTransactions(file_dir + msg["artifacts"]["salesArtifactId"])
    except:
        logging.info('attempting drill down')
        sales = helper.fetchTransactions(file_dir + msg["artifacts"]["categorySalesArtifactId"])
        logging.info(msg["artifacts"]["categorySalesArtifactId"])
    # except:
    #     logger.exception("Did not find categorySalesArtifactId")
    logging.info('uploaded sales')

    try:
        space = helper.fetchSpace(file_dir + msg["artifacts"]["spaceArtifactId"])
        # print (space.columns.values.tolist())
    except:
        space = helper.fetchLong(
            msg['artifacts']['tieredResultArtifactId'],filtCategory)
    logging.info('uploaded space')


    try:
        futureSpace = helper.fetchSpace(file_dir + msg["artifacts"]["futureSpaceId"])
        # print (futureSpace.index.values.tolist())
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

    print ('After csv files loading in run line 74-115: ', time.time())

    if msg['jobType'] == 'unconstrained' or msg['jobType'] == 'drilldown':
        msg['tierCounts'] = None
    if msg['jobType'] == 'drilldown':
        msg['optimizedMetrics'] = \
            {
                "spread": 100,
                "salesPenetration": 0,
                "salesPerSpaceUnit": 0,
                "inventoryTurns": 0,
                "grossMargin": 0
            }
        # msg['increment'] = 1
        # msg['salesStores'] = space['Store'].unique()
        # msg['salesCategories'] = space['Category'].unique()
        # msg['metricAdjustment'] = 0
        # msg["salesPenetrationThreshold"] = .02

    print ('before dataMerge in line 127: ', time.time())
    dataMerged = dataMerge(jobName=msg['meta']['name'], jobType=msg['jobType'],
                           optimizationType=msg['optimizationType'], transactions=sales, space=space,
                           brandExit=brandExitArtifact, futureSpace=futureSpace)
    logging.info('finished data merging')
    print ('after dataMerge in line 127: ', time.time())

    print ('before preoptimize in line 135: ', time.time())
    try:
        preOpt = preoptimize(jobType=msg['jobType'], optimizationType=msg['optimizationType'], dataMunged=dataMerged[1],
                             mAdjustment=float(msg["metricAdjustment"]),
                             salesPenThreshold=float(msg["salesPenetrationThreshold"]),
                             optimizedMetrics=msg["optimizedMetrics"], increment=msg["increment"])
    except:
        logging.exception('A thing')
        traceback.print_exception()
    logging.info('finished preoptimize')
    print ('after preoptimize in line 135: ', time.time())
    if msg['optimizationType'] == 'traditional':
        if msg['jobType'] == 'unconstrained' or msg['jobType'] == 'tiered':
            logging.info('going to the optimization')
            print('before optimizeTrad in line 148: ', time.time())
            optimRes = optimizeTrad(jobName=msg['meta']['name'], Stores=msg['salesStores'],
                                    Categories=msg['salesCategories'],
                                    spaceBound=msg['spaceBounds'], increment=msg['increment'], dataMunged=preOpt,
                                    salesPen=msg['salesPenetrationThreshold'], tierCounts = msg['tierCounts'])
            print('after optimizeTrad in line 148: ', time.time())
        else:
            print ('before optimizeDD in line 156: ', time.time())
            try:
                optimRes = optimizeDD(jobName=msg['meta']['name'], increment=msg["increment"], dataMunged=preOpt,
                                  salesPen=msg['salesPenetrationThreshold'])
            except:
                logging.exception('A thing')
                traceback.print_exception()
            print ('after optimizeDD in line 156: ', time.time())
        cfbsArtifact=[None,None]
        scaledAnalyticsID = None
    else:
        print ('before curveFittingBS in line 166: ', time.time())
        cfbsArtifact = curveFittingBS(dataMerged[0], msg['spaceBounds'], msg['increment'],
                                    msg['storeCategoryBounds'],
                                    float(msg["salesPenetrationThreshold"]), msg['jobType'],
                                    msg['optimizationType'])
        logging.info('finished curve fitting')
        print ('after curveFittingBS in line 166: ', time.time())

        print ('before single store in line 174: ', time.time())
        cfbsOptimal = optimizeSingleStore(cfbsArtifact[0].set_index(['Store', 'Category']), msg['increment'],
                                        msg['optimizedMetrics'])
        logging.info('finished single store')
        print ('after single store in line 174: ', time.time())
        print ('before optimizeEnh in line 179: ', time.time())
        optimRes = optimizeEnh(methodology=msg['optimizationType'], jobType=msg['jobType'], jobName=msg['meta']['name'],
                             Stores=msg['salesStores'], Categories=msg['salesCategories'],
                             increment=msg['increment'], weights=msg['optimizedMetrics'], cfbsOutput=cfbsOptimal[1],
                             preOpt=preOpt, salesPen=msg['salesPenetrationThreshold'], tierCounts=msg['tierCounts'])
        print ('after optimizeEnh in line 179: ', time.time())
    logging.info('we just did the optimization')
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
    parser.add_option("-c", "--conf", dest="input_json")
    parser.add_option("-sa", "--sales", dest="sales_data")
    parser.add_option("-sp", "--space", dest="space_data")
    parser.add_option("-b", "--brandExit", dest="brand_exit")
    parser.add_option("-fs", "--futureSpace", dest="future_space")
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
