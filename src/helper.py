import datetime as dt
from pulp import *
import config as env
import json
import gridfs
import pandas as pd
import pika
import logging
import traceback
from distutils.util import strtobool

import sys
sys.path.append('/vagrant/fo_api/src/')
# The following path is the path for local env.
sys.path.append('../fo_api/src/')
#from artifacts import ArtifactBuilder


def toCsv(dataframe, filename):
	"""
	Returns the bson.objectid.ObjectId of the resulting GridFS artifact

	"""
	return dataframe.to_csv(filename, index=False)

def fetch_artifact(file):
	# file = fs.get(ObjectId(artifact_id))
	file = pd.read_csv(file, header=0)
	return file

def fetch_sales_data(file):
	# file = fs.get(ObjectId(artifact_id))
	file = pd.read_csv(file, header=None)
	# print (file.loc[range(10), :])
	# return file.loc[range(10), :]
	return file

def fetch_space_data(file):
	# file = fs.get(ObjectId(artifact_id))
	file = pd.read_csv(file, header=0, dtype={'Store': object}, skiprows=[1])
	# print (file.loc[range(10), :])
	# return file.loc[range(10), :]
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

def fetch_brand_exit_data(file):
	# file = fs.get(ObjectId(artifact_id))
	file = pd.read_csv(file, header=0, skiprows=[1])
	return file

def loadJson(file):
	with open(file, "r") as f:
		j = json.load(f)
	return j

def loadOptions(options):
	err_lst = list()
	_dir = options.input_dir if options.input_dir != None else err_lst.append('input dir')
	conf = _dir + options.conf if _dir != None and options.conf != None else err_lst.append("config json")
	sales_data = _dir + options.sales_data if _dir != None and options.sales_data != None else err_lst.append("sales data")
	space_data = _dir + options.space_data if _dir != None and options.space_data != None else err_lst.append("space data")
	brand_exit = _dir + options.brand_exit if _dir != None and options.brand_exit != None else None
	future_space = _dir + options.future_space if _dir != None and options.future_space != None else None
	drill_down = options.drill_down == "True" if options.drill_down != None else err_lst.append("drill down option needs to be either True or False!")

	if len(err_lst) != 0:
		err = ', '.join(err_lst)
		print (err + " is/are required!!!")
		sys.exit(1)
	return (_dir, conf, sales_data, space_data, space_data, future_space, brand_exit, drill_down)

def modifyJson(lst):
	_dir, conf, sales_data, space_data, space_data, future_space, brand_exit, drill_down = lst
	j = loadJson(conf)

	j['filenames'] = dict()
	j['artifacts'] = dict()

	j['filenames'][sales_data] = sales_data
	j['artifacts']['salesArtifactId'] = sales_data

	j['filenames'][space_data] = space_data
	if drill_down:
		j['artifacts']['tieredResultArtifactId'] = space_data
	else:
		j['artifacts']['spaceArtifactId'] = space_data

	if brand_exit != None:
		j['filenames'][brand_exit] = brand_exit
		j['artifacts']['brandExitArtifactId'] = brand_exit

	if future_space != None:
		j['filenames'][future_space] = future_space
		j['artifacts']['futureSpaceId'] = future_space


	return j

# modify json request by replacing MongoDB fs hash location to local path locations.
def jsonFormulator(options):
	input_lst = loadOptions(options)
	return modifyJson(input_lst)

	



def create_new_artifact(filename, _type):
	
	inspection = ArtifactBuilder.create(open(filename, 'r'), _type)
	# print(inspection)
	return inspection

