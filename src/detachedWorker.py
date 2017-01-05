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
#from FixtureOptimization.CurveFitting import curveFittingBS
from FixtureOptimization.dataMerging import read_sales_data, read_space_data, read_future_space_data, read_brand_exit_data, \
    merge_space_data, merge_space_and_sales_data, prepare_bounds

from FixtureOptimization.preoptimizer import validate_space_data, validate_sales_data, prepare_data, bcolors
from FixtureOptimization.optimizerTrad import optimizeTrad
from FixtureOptimization.optimizerEnh import optimizeEnh
from FixtureOptimization.outputFunctions import createLong, createWide, createDrillDownSummary, createTieredSummary, outputValidation
#from FixtureOptimization.optimizerDD import optimizeDD
#from FixtureOptimization.optimizerDDEnh import optimizeEnhDD
from pika import BlockingConnection, ConnectionParameters
from FixtureOptimization.SingleStoreOptimization import optimizeSingleStore
import logging
import traceback
from distutils.util import strtobool

import helper
from helper import fetch_artifact as fetch_artifact
from helper import fetch_sales_data as fetch_sales_data
from helper import fetch_space_data as fetch_space_data
from helper import fetchLong as fetchLong
from helper import fetch_brand_exit_data as fetch_brand_exit_data
from helper import toCsv as toCsv
# imports from refactored code
from codeoptimization.traditionalOptimizer import TraditionalOptimizer 
from codeoptimization.dataMerger import DataMerger

import sys
sys.path.append('/vagrant/fo_api/src/')
#from artifacts import ArtifactBuilder

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

def print_verbose(title, data):
    print('######################################################################################################')
    print(' ')
    print(bcolors.BOLD + bcolors.UNDERLINE + title + bcolors.ENDC)
    print(' ')
    print(data)
    print(' ')


######################################################################################################
def run(msg):
    
    #Creating a data handler object to read, and merge data sources
    
    data_merger = DataMerger()

	# ONLY FOR TESTING!!
    filtCategory = 'MISSESJRS'
    csvfiles = []
    ############################# File Uploading #################################

    # sets dataframe display for floats to 2 digits post decimal point
    pd.options.display.float_format = '{:10,.2f}'.format

    # allows 30 columns printed for dataframes
    pd.set_option('display.max_columns', 30)

    # does not allow line breaks for column printout
    pd.set_option('expand_frame_repr', False)

    #############################
	# 1. Sales data
    try:
        #sales = fetch_sales_data(msg["artifacts"]["salesArtifactId"])
        sales = data_merger.read_sales_data(filename=msg["artifacts"]["salesArtifactId"])
        csvfiles.append('First level sales data')
        print_verbose('Sales data:', sales)
    except:
        sales = fetch_sales_data(msg["artifacts"]["categorySalesArtifactId"])
        csvfiles.append('Drilldown sales data')
        print_verbose('Sales data:', sales)

    #############################
    #  2. Space data
    try:
        space = data_merger.read_space_data(filename=msg["artifacts"]["spaceArtifactId"])
        csvfiles.append('First level space data')
        print_verbose('Space data:', space)
    except:
        space = fetchLong(msg['artifacts']['tieredResultArtifactId'], filtCategory)
        csvfiles.append('Drilldown space')
        print_verbose('Space data:', space)

    #############################
    # 3. Future space data
    try:
        future_space = data_merger.read_future_space_data(jobType=msg['jobType'], filename=msg["artifacts"]["futureSpaceId"])
        csvfiles.append("Future space data")
        print_verbose('Future space data:', future_space)
    except:
        print('No Future space data provided!')
        future_space = None

    #############################
    # 4. Brand exit data
    try:
        brand_exit = data_merger.read_brand_exit_data(filename=msg["artifacts"]["brandExitArtifactId"])
        csvfiles.append("Brand exit data")
        print_verbose('Brand exit data:', brand_exit)
    except:
        print('No Brand exit data provided!')
        brand_exit = None

    ##############################################################################

    ############################## Prepare Bounds ################################
    print("=> Prepare bounds data")

#     category_bounds = prepare_bounds(space_bound = msg['spaceBounds'],
#                                      increment   = msg['increment'],
#                                      tier_bound  = msg['tierCounts'])
    
    
    category_bounds = data_merger.prepare_bounds(msg['spaceBounds'],
                                                 msg['increment'],
                                                 msg['tierCounts'])

    print ("<= Prepare bounds data")
    ##############################################################################

    ######################## Modify json according to jobType ####################

    if msg['jobType'] == 'unconstrained' or msg['jobType'] == 'drilldown':
        msg['tierCounts'] = None
    if msg['jobType'] == 'drilldown': # Why?
        msg['optimizedMetrics'] = \
            {
                "spread": 0,
                "salesPenetration": 100,
                "salesPerSpaceUnit": 0,
                "inventoryTurns": 0,
                "grossMargin": 0
            }
        msg['increment'] = 1
        msg['salesStores'] = space['Store'].unique()
        msg['metricAdjustment'] = 0
        msg["salesPenetrationThreshold"] = .02

    print_verbose('JSON file:', msg)

    ###############################################################################

    # Validates sales data
    sales, idx_sales_invalid = validate_sales_data(sales)

    # Validates space data
    #space, idx_space_invalid = validate_space_data(space, category_bounds)

    # extracts the categories from the sales data
    sales_categories = sales['Category'].unique()
    # extracts the categories from the space data
    space_categories = space['Category'].unique()

    print_verbose('Categories (in Sales data):', sales_categories)
    print_verbose('Categories (in Space data):', space_categories)

    ####################################### Data Merging #################################################
    print ("\n=> merge data")

    # merges the space data with requirements for future space and brand exits by store and category
    space = data_merger.merge_space_data(space, future_space, brand_exit)

    # merges the space data with the sales data by store and category
    sales_space_data = data_merger.merge_space_and_sales_data(sales, space)

    print ("\n<= merge data")
    ######################################################################################################

    print_verbose('Full raw dataset:', sales_space_data)

    ###################################### Validate ###################################################
    # print("=> Validate data")
    #
    # sales_space_data, idx_invalid = validate_data(jobType	= msg['jobType'],
    #                                             optimizationType	= msg['optimizationType'],
    #                                             data				= sales_space_data,
    #                                             category_bounds     = category_bounds,
    #                                             metricAdjustment	= float(msg["metricAdjustment"]),
    #                                             salesPenThreshold	= float(msg["salesPenetrationThreshold"]),
    #                                             bizmetrics		    = msg["optimizedMetrics"])
    # print ("<= Validate data")
    ######################################################################################################

    ###################################### Prepare ###################################################
    print ("=> prepare data")

    prepped_data = prepare_data(jobType			= msg['jobType'],
                            optimizationType	= msg['optimizationType'],
							data				= sales_space_data,
                            metricAdjustment	= float(msg["metricAdjustment"]),
                            salesPenThreshold	= float(msg["salesPenetrationThreshold"]),
                            bizmetrics		    = msg["optimizedMetrics"])
    print ("<= prepare data")
    ######################################################################################################

    ###################################### Optimize ######################################################
    print ("=> optimize()")

    if msg['optimizationType'] == 'traditional':

        # NOTE: Although it is not actually required to have stores and categories as parameters
        # for this function, since they can be derived from the actual data via
        # stores = data['Store'].unique()
        # categories = data['Category'].unique()
        # we may want to keep those in here in case we want to allow files with more stores and categories
        # data specified than what we want to optimize for at a time. Then we could simply here specify
        # for which of the categories and store we actually want an optimization run.
        # However, if this is not going to be a feature we should take it out of the signature

        # Unconstrained or Tierd Optimization
        if msg['jobType'] == 'unconstrained' or msg['jobType'] == 'tiered':
#             optimRes = optimizeTrad(job_name	= msg['meta']['name'],
#                                     job_type    = msg['jobType'],
#                                     stores		= msg['salesStores'],
#                                     categories	= msg['salesCategories'],
#                                     category_bounds = category_bounds,
#                                     increment	= msg['increment'],
#                                     data		= prepped_data,
#                                     sales_penetration_threshold = msg['salesPenetrationThreshold'])
        
              optimizer= TraditionalOptimizer( msg['meta']['name'],
                                             msg['jobType'],
                                             msg['salesStores'],
                                             msg['salesCategories'],
                                             category_bounds,
                                             msg['increment'],
                                             prepped_data,
                                             msg['salesPenetrationThreshold'])
              optimRes = optimizer.optimize()
        # Drill Down Optimization
        elif msg['jobType'] == 'drilldown':
            msg['salesCategories'] = prepped_data['Category'].unique()

            optimRes = optimizeDD(jobName		= msg['meta']['name'],
                                    increment	= msg["increment"],
                                    data		= prepped_data,
                                    salesPen	= msg['salesPenetrationThreshold'])
        cfbsArtifact=[None,None]
        scaledAnalyticsID = None
    else:
		# sales_space_data[0] is data in prcess_sales_and_space_data in dataMerging.py
        cfbsArtifact = curveFittingBS(sales_space_data[0],
                                    msg['spaceBounds'],
                                    msg['increment'],
                                    msg['storeCategoryBounds'],
                                    float(msg["salesPenetrationThreshold"]),
                                    msg['jobType'],
                                    msg['optimizationType'])
        logging.info('finished curve fitting')

        cfbsOptimal = optimizeSingleStore(cfbsArtifact[0].set_index(['Store', 'Category']),
										  msg['increment'],
										  msg['optimizedMetrics'])
        logging.info('finished single store')

        optimRes = optimizeEnh(methodology=msg['optimizationType'],
							   jobType=msg['jobType'],
							   jobName=msg['meta']['name'],
                             Stores=msg['salesStores'],
							   Categories=msg['salesCategories'],
                             increment=msg['increment'],
							   weights=msg['optimizedMetrics'],
							   cfbsOutput=cfbsOptimal[1],
                             prepped_data=prepped_data,
							   salesPen=msg['salesPenetrationThreshold'],
							   tierCounts=msg['tierCounts'])
    print ("<= optimize()")
    ######################################################################################################

    ###################################### Output ########################################################
    statusID = optimRes[0]
    print ("The solution is " + statusID)

    if statusID == 'Optimal':
        # Call functions to create output information
        longOutput = createLong(msg['jobType'], msg['optimizationType'], optimRes[1])
        wideOutput = createWide(longOutput[0], msg['jobType'], msg['optimizationType'])

        longOutput[0].to_csv(filename='longOutput')
        wideOutput.to_csv(filename='wideOutput')

        # print ("longOutput[0]")
        # print (longOutput[0].columns.values.tolist())
        # print ("#####################################")
        # print ("longOutput[1]")
        # print (longOutput[1].columns.values.tolist())
        # print ("#####################################")
        # print ("wideOutput")
        # print (wideOutput.columns.values.tolist())

    # Why?
    return (sales_space_data, prepped_data, optimRes[len(optimRes)-1],longOutput[0], wideOutput)

######################################################################################################

######################################################################################################
if __name__ == '__main__':
    # LOGGER.debug('hello from {}'.format(__name__))
    # logger.debug('hello from {}'.format(__name__))

    # Getting input json name from command line
    parser = OptionParser()
    parser.add_option("-c", "--conf", dest="conf")
    parser.add_option("-s", "--sales", dest="sales_data")
    parser.add_option("-p", "--space", dest="space_data")
    parser.add_option("-b", "--brandExit", dest="brand_exit")
    parser.add_option("-f", "--futureSpace", dest="future_space")
    parser.add_option("-d", "--dir", dest="input_dir")
    parser.add_option("-r", "--drillDown", dest="drill_down")

    (options, args) = parser.parse_args()

    # Modify json structures by replacing filename hash locations
    # from MongoDB to local filename.
    # jsonFilename = file_dir + options.conf
    body = helper.jsonFormulator(options)

	###################################### Sales and Space Input Validation #####################################
    # meta_sales = ArtifactBuilder.create(open(body['artifacts']['salesArtifactId'], 'r'), 'sales')
    # meta_space = ArtifactBuilder.create(open(body['artifacts']['spaceArtifactId'], 'r'), 'space')

    # # Checking if stores match between sales_data and space_data.
    # stores_diff = [set(meta_space['stores']) - set(meta_sales['stores']), 'space'] \
    #               if len(set(meta_space['stores'])) >= len(set(meta_sales['stores'])) \
    #               else [set(meta_sales['stores']) - set(meta_space['stores']), 'sales']
    # if len(stores_diff[0]) != 0:
    #   print ("stores on sales data and space data don't match!!!!! Here's the what the stores missing from " + stores_diff[1] + ":")
    #   print (list(stores_diff[0]))
    #   sys.exit(1)

    # # Checkign if categories matche between sales_data and space_data.
    # categories_diff = [set(meta_space['categories']) - set(meta_sales['categories']), 'space'] \
    #               if len(set(meta_space['categories'])) >= len(set(meta_sales['categories'])) \
    #               else [set(meta_sales['categories']) - set(meta_space['categories']), 'sales']
    # if len(categories_diff[0]) != 0:
    #   print ("stores on sales data and space data don't match!!!!! Here's the what the stores missing from " + categories_diff[1] + ":")
    #   print (list(categories_diff[0]))
    #   sys.exit(1)

	##########################################################################################################

    # print (list(map(int,meta_space['stores'])))
    # print (meta_space['extrema'])
    # print (body['spaceBounds'])

    # sys.exit(0)

    # body['salesStores'] = [7, 8, 9, 10, 11, 14, 17, 18]
    # body['salesStores'] = list(map(int, meta_space['stores']))
    # body['salesCategories'] = meta_space['categories']
    # body['spaceBounds'] = meta_space['extrema']

    # Trigger the new runner without logging any ID info.
    dataMerged, preOpt, problem, longOutput, wideOutput = run(body)

    _dir = 'test/temp/'

    if LpStatus[problem.status].lower() == 'optimal':
        toCsv(dataMerged, _dir + 'intermediate/dataMerged.csv')
        toCsv(preOpt, _dir + 'intermediate/preOpt.csv')
        problem.writeLP(_dir + 'lp_problem/' + 'optimalSolution' + '.lp')
        toCsv(longOutput, _dir + 'output/longOutput.csv')
        toCsv(wideOutput, _dir + 'output/wideOutput.csv')
    else:
        print ("The problem is ", LpStatus[problem.status])
        problem.writeLP(_dir + 'lp_problem/' + 'infeasibleSolution' + '.lp')
