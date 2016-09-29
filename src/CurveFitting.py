#Same import commands for all scripts
import rpy2
import pandas as pd
import numpy as np
#import rpy2POC
import json
import pika
import time
from pymongo import MongoClient
import gridfs
import rpy2.robjects as robjects
from rpy2.robjects.vectors import DataFrame
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr


import os 
cwd = os.getcwd()
print(cwd)

def curveFittingBS(big_master_data,bound_input,increment_size,PCT_Space_Change_Limit,salesPen,jobType,optimType):
    pandas2ri.activate()
    bound_input=pd.DataFrame.from_dict(bound_input).T.reset_index()
    bound_input.columns=['Categories','Space_Lower_Limit','Space_Upper_Limit','PCT_Space_Lower_Limit','PCT_Space_Upper_Limit']
    gdata = importr("gdata")
    dataTable = importr("pracma")
    sqldf = importr("nloptr")
    tidyr = importr("tidyr")

    print('merged data')
    print(big_master_data.head())
    input('bounds')
    print(bound_input)
    input('increment size')
    print(increment_size)
    input('percent change limit')
    print(PCT_Space_Change_Limit)
    input('sales pen threshold')
    print(salesPen)
    input('job type')
    print(jobType)
    input('optimization type')
    print(optimType)
    input('Done')

    #Source the R code
    r_source = robjects.r['source']
    r_source('src/rCurveFitting.R')

    # # Extract the main function from the R code
    r_curvefitting_boundsetting = robjects.globalenv['curvefitting_boundsetting']
    # pandas2ri.py2ri(big_master_data)
    # # Call the r function with the dataframes
    r_list_output=r_curvefitting_boundsetting(big_master_data,bound_input,increment_size,PCT_Space_Change_Limit,salesPen,jobType,optimType,)

    # Convert R list output into 2 python data frames, put into python list for the return statement
    cfbsArtifact=pandas2ri.ri2py(r_list_output[0])
    # p_output1=pandas2ri.ri2py(r_list_output[1])
    # p_list_output = [p_output0,p_output1]
    cfbs_id = create_output_artifact_from_dataframe(r_list_output[0])
    analytics_id = create_output_artifact_from_dataframe(r_list_output[1])
    return cfbsArtifact