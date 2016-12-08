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

import sys
sys.path.append('/vagrant/fo_api/src/')
from artifacts import ArtifactBuilder


def create_output_artifact_from_dataframe(dataframe, *args, **kwargs):
	"""
	Returns the bson.objectid.ObjectId of the resulting GridFS artifact

	"""
	return fs.put(dataframe.to_csv(index=False).encode(), **kwargs)

def fetch_artifact(file):
	# file = fs.get(ObjectId(artifact_id))
	file = pd.read_csv(file, header=0)
	return file

def fetchTransactions(file):
	# file = fs.get(ObjectId(artifact_id))
	file = pd.read_csv(file, header=None)
	return file

def fetchSpace(file):
	# file = fs.get(ObjectId(artifact_id))
	file = pd.read_csv(file, header=0, dtype={'Store': object}, skiprows=[1])
	return file

def fetchLong(file,filtCategory):
    def tierColCreate(dfI):
        """
        Creates the Tier Columns
        :param df: Long Table to be given tiering information
        :return: tiered long table output
        """
        try:
            dfI.sort_values(by='Result Space', inplace=True)
            tierVals = dfI.groupby('Category')
            for (i, category) in tierVals:
                indices = category.index.values
                ranking = sorted(set(category['Result Space'].values.tolist()))
                dfI.loc[indices, 'Tier'] = category['Result Space'].apply(
                    lambda x: 'Tier ' + str(ranking.index(x) + 1))
            dfI.reset_index(drop=True)
        except Exception:
            logging.exception('This is what happened')
            traceback.print_exception()
        return dfI
    # file = fs.get(ObjectId(artifact_id))
    file = pd.read_csv(file,header=0)
    file = file[(file['Category'] == filtCategory)]
    file = tierColCreate(file[['Store','VSG','Climate','Category','Result Space']])
    return file

def fetchExit(file):
	# file = fs.get(ObjectId(artifact_id))
	file = pd.read_csv(file, header=0, skiprows=[1])
	return file
def loadJson(file):
	with open(file, "r") as f:
		j = json.load(f)
	return j
# modify json request by replacing MongoDB fs hash location to local path locations.
def modifyJson(j):
	# store fileNames as (k,v) pair such that
	# j['filenames'] is in style of (filename, filename)
	allHashKeys = sorted(j['filenames'])
	allFileNames = list(v for k,v in j['filenames'].items())
	for fileName in allFileNames:
		j['filenames'][fileName] = fileName
	
	# Replace key in j['artifacts'] by its actual fileName.
	for key in j['artifacts'].keys():
		hashLocation = j['artifacts'][key]
		j['artifacts'][key] = j['filenames'][hashLocation]
	
	# delete the previous (k,v) in j['filenames'].
	for hash_key in allHashKeys:
		del j['filenames'][hash_key]

def create_new_artifact(filename, _type):
	
	inspection = ArtifactBuilder.create(open(filename, 'r'), _type)
	# print(inspection)
	return inspection

