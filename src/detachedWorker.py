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
from FixtureOptimization.optimizerDD import optimizeDD
from FixtureOptimization.optimizerDDEnh import optimizeEnhDD
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

	filtCategory = 'MISSESJRS'

	try:
		sales = fetchTransactions(msg["artifacts"]["salesArtifactId"])
		# print (sales)
	except:
		logging.info('attempting drill down')
		sales = fetchTransactions(msg["artifacts"]["categorySalesArtifactId"])
		logging.info(msg["artifacts"]["categorySalesArtifactId"])
	# except:
	#     logger.exception("Did not find categorySalesArtifactId")
	logging.info('uploaded sales')

	try:
		space = fetchSpace(msg["artifacts"]["spaceArtifactId"])
		# print (space)
	except:
		space = fetchLong(
			msg['artifacts']['tieredResultArtifactId'],filtCategory)
	logging.info('uploaded space')

	try:
		futureSpace = fetchSpace(msg["artifacts"]["futureSpaceId"])
		logging.info("Future Space was Uploaded")
	except:
		futureSpace = None
		logging.info("Future Space was not Uploaded")
	try:
		brandExitArtifact = fetchExit(msg["artifacts"]["brandExitArtifactId"])
		logging.info("Brand Exit was Uploaded")
	except:
		logging.info("Brand Exit was not Uploaded")
		brandExitArtifact = None
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
		msg['increment'] = 1
		msg['salesStores'] = space['Store'].unique()
		msg['metricAdjustment'] = 0
		msg["salesPenetrationThreshold"] = .02

	dataMerged = dataMerge(jobName=msg['meta']['name'], jobType=msg['jobType'],
						   optimizationType=msg['optimizationType'], transactions=sales, space=space,
						   brandExit=brandExitArtifact, futureSpace=futureSpace)
	logging.info('finished data merging')
	try:
		preOpt = preoptimize(jobType=msg['jobType'], optimizationType=msg['optimizationType'], dataMunged=dataMerged[1],
							 mAdjustment=float(msg["metricAdjustment"]),
							 salesPenThreshold=float(msg["salesPenetrationThreshold"]),
							 optimizedMetrics=msg["optimizedMetrics"], increment=msg["increment"])
	except:
		logging.exception('A thing')
		traceback.print_exception()
	logging.info('finished preoptimize')
	if msg['optimizationType'] == 'traditional':
		if msg['jobType'] == 'unconstrained' or msg['jobType'] == 'tiered':
			logging.info('going to the optimization')
			optimRes = optimizeTrad(jobName=msg['meta']['name'], Stores=msg['salesStores'],
									Categories=msg['salesCategories'],
									spaceBound=msg['spaceBounds'], increment=msg['increment'], dataMunged=preOpt,
									salesPen=msg['salesPenetrationThreshold'], tierCounts = msg['tierCounts'])
		else:
			try:
				print(preOpt['Category'])
				msg['salesCategories'] = preOpt['Category'].unique()
				optimRes = optimizeDD(jobName=msg['meta']['name'], increment=msg["increment"], dataMunged=preOpt,
								  salesPen=msg['salesPenetrationThreshold'])
				# optimRes = optimizeEnhDD(methodology=msg['optimizationType'], jobType=msg['jobType'], jobName=msg['meta']['name'],
				#               Stores=msg['salesStores'], Categories=msg['salesCategories'], increment=msg['increment'],
				#               weights=msg['optimizedMetrics'], preOpt=preOpt, salesPen=msg['salesPenetrationThreshold'])
			except Exception:
				logging.exception('A thing')
				traceback.print_exception()
		cfbsArtifact=[None,None]
		scaledAnalyticsID = None
	else:
		cfbsArtifact = curveFittingBS(dataMerged[0], msg['spaceBounds'], msg['increment'],
									msg['storeCategoryBounds'],
									float(msg["salesPenetrationThreshold"]), msg['jobType'],
									msg['optimizationType'])
		logging.info('finished curve fitting')
		cfbsOptimal = optimizeSingleStore(cfbsArtifact[0].set_index(['Store', 'Category']), msg['increment'],
										msg['optimizedMetrics'])
		logging.info('finished single store')
		optimRes = optimizeEnh(methodology=msg['optimizationType'], jobType=msg['jobType'], jobName=msg['meta']['name'],
							 Stores=msg['salesStores'], Categories=msg['salesCategories'],
							 increment=msg['increment'], weights=msg['optimizedMetrics'], cfbsOutput=cfbsOptimal[1],
							 preOpt=preOpt, salesPen=msg['salesPenetrationThreshold'], tierCounts=msg['tierCounts'])
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
	run(body)
