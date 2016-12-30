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
from FixtureOptimization.dataMerging import dataMerge
from FixtureOptimization.preoptimizer import preoptimize
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
from helper import fetchTransactions as fetchTransactions
from helper import fetchSpace as fetchSpace
from helper import fetchLong as fetchLong
from helper import fetchExit as fetchExit

#import sys
#sys.path.append('/vagrant/fo_api/src/')
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


def run(msg):

	filtCategory = 'MISSESJRS'
	csvfiles = []
	############################# File Uploading #################################
	try:
		sales = fetchTransactions(msg["artifacts"]["salesArtifactId"])
		csvfiles.append('First level sales data')
	except:
		sales = fetchTransactions(msg["artifacts"]["categorySalesArtifactId"])
		csvfiles.append('Drill Down sales data')

	try:
		space = fetchSpace(msg["artifacts"]["spaceArtifactId"])
		csvfiles.append('First level sales data')
	except:
		space = fetchLong(msg['artifacts']['tieredResultArtifactId'],filtCategory)
		csvfiles.append('Drill Down sales')

	try:
		futureSpace = fetchSpace(msg["artifacts"]["futureSpaceId"])
		csvfiles.append("future space data")
	except:
		futureSpace = None
	try:
		brandExitArtifact = fetchExit(msg["artifacts"]["brandExitArtifactId"])
		csvfiles.append("brand exit data")
	except:
		brandExitArtifact = None
	print ("Finished uploading " + ', '.join(csvfiles))
	##############################################################################

	################################### Modify json according to jobType #################################

	if msg['jobType'] == 'unconstrained' or msg['jobType'] == 'drilldown':
		msg['tierCounts'] = None
	if msg['jobType'] == 'drilldown': # Why?
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

	######################################################################################################

	####################################### Data Merging #################################################
	print ("=> dataMerge()")
	dataMerged = dataMerge(jobName=msg['meta']['name'], 
							jobType=msg['jobType'],
							optimizationType=msg['optimizationType'],
							transactions=sales, 
							space=space,
							brandExit=brandExitArtifact,
							futureSpace=futureSpace)
	print ("<= dataMerge()")
	######################################################################################################

	###################################### Preoptimize ###################################################
	print ("=> preoptimize()")
	preOpt = preoptimize(jobType=msg['jobType'],
							optimizationType=msg['optimizationType'],
						 	dataMunged=dataMerged[1],
							mAdjustment=float(msg["metricAdjustment"]),
							salesPenThreshold=float(msg["salesPenetrationThreshold"]),
							optimizedMetrics=msg["optimizedMetrics"],
							increment=msg["increment"])
	print ("<= preoptimize()")
	######################################################################################################

	###################################### Optimize ######################################################
	print ("=> optimize()")
	if msg['optimizationType'] == 'traditional':
		if msg['jobType'] == 'unconstrained' or msg['jobType'] == 'tiered':
			optimRes = optimizeTrad(jobName=msg['meta']['name'],
									Stores=msg['salesStores'],
									Categories=msg['salesCategories'],
									spaceBound=msg['spaceBounds'],
									increment=msg['increment'],
									dataMunged=preOpt,
									salesPen=msg['salesPenetrationThreshold'],
									tierCounts = msg['tierCounts'])
		else: # Drill Down
			msg['salesCategories'] = preOpt['Category'].unique()
			optimRes = optimizeDD(jobName=msg['meta']['name'],
									increment=msg["increment"],
									data=preOpt,
									salesPen=msg['salesPenetrationThreshold'])
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
	print ("<= optimize()")
	######################################################################################################

	###################################### Output ########################################################
	statusID = optimRes[0]
	print ("The solution is " + statusID)
	if statusID == 'Optimal' or 'Infeasible':
		# Call functions to create output information
		longOutput = createLong(msg['jobType'],msg['optimizationType'], optimRes[1])
		wideOutput = createWide(longOutput[0], msg['jobType'], msg['optimizationType'])

		print ("longOutput[0]")
		print (longOutput[0].columns.values.tolist())
		print ("#####################################")
		print ("longOutput[1]")
		print (longOutput[1].columns.values.tolist())
		print ("#####################################")
		print ("wideOutput")
		print (wideOutput.columns.values.tolist())

		# if cfbsArtifact[1] is not None:
		# 	longID = str(create_output_artifact_from_dataframe(longOutput[0]))
		# 	analyticsID = str(create_output_artifact_from_dataframe(cfbsArtifact[1]))
		# 	scaledAnalyticsID = str(create_output_artifact_from_dataframe(longOutput[1]))
		# 	logging.info('Created analytics ID')
		# else:
		# 	longID = str(create_output_artifact_from_dataframe(
		# 		longOutput[0][
		# 			['Store', 'Category', 'Climate', 'VSG', 'Sales Penetration', 'Result Space', 'Current Space',
		# 			 'Optimal Space']]))
		# 	analyticsID=None
		# 	logging.info('Set analytics ID to None')

		# if msg['jobType'] == "tiered" or 'unconstrained':
		# 	summaryID = str(create_output_artifact_from_dataframe(createTieredSummary(longOutput[int(0)])))
		# else:  # since type == "Drill Down"
		# 	summaryID = str(create_output_artifact_from_dataframe(createDrillDownSummary(longOutput[int(0)])))
		# logging.info('Set the summary IDs')

		# try:
		# 	invalids = outputValidation(df=longOutput[0], jobType=msg['jobType'], tierCounts=msg['tierCounts'],
		# 								increment=msg['increment'])
		# except Exception as e:
		# 	logging.exception('Not entirely sure what is happening')
		# 	return
		# 	# traceback.logging.info_exc(e)
		# logging.info('set the invalids')
	######################################################################################################
	return (dataMerged[1], preOpt, optimRes[len(optimRes)-1],longOutput[1], wideOutput)

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
