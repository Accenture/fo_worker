#Same import commands for all scripts
# import rpy2
import pandas as pd
import numpy as np
import json
import pika
import time
from pymongo import MongoClient
import gridfs
from rpy2.robjects import pandas2ri
import rpy2.robjects as ro
from rpy2.robjects.vectors import DataFrame
import gridfs
from bson.objectid import ObjectId
import config
import os
import pandas.rpy.common as com

pandas2ri.activate()

def dataMerging(msg):
    db = MongoClient(config.MONGO_CON)['app']
    fs = gridfs.GridFS(db)
    ro.pandas2ri.activate()

    def fetchRdf(artifact_id):
        file = fs.get(ObjectId(artifact_id))
        pFile = pd.read_csv(file)
        rFile=com.convert_to_r_dataframe(pFile)
        # pandas2ri.ri2py(r[])
        # rFile = DataFrame.from_csvfile(pFile,header=False)
        return rFile

    Hist_space_climate_info=fetchRdf(msg["artifacts"]["spaceArtifactId"])
    Hist_perf=fetchRdf(msg["artifacts"]["salesArtifactId"])
    
    try:
        Future_Space_Entry_Data=fetchRdf(msg["artifacts"]["futureSpaceId"])
        print("For R Future Space was Uploaded")
    except:
        Future_Space_Entry_Data=None
        print("For R Future Space was not Uploaded")
    try:
        Brand_Exit=fetchRdf(msg["artifacts"]["brandExitArtifactId"])
        print("For R Brand Exit was Uploaded")
    except:
        print("For R Brand Exit was not Uploaded")
        Brand_Exit=None

    optimType = "Regular"

    # Source the R file into memory so it can be used
    r_source = ro.r['source']
    print("we got here")
    r_source('rDataMerge.R')
    print("But not here")

    # robjects.globalenv gives access to any global variable and functions from all sourced R scripts
    # The main function in the data merging code is called 'Data_merge', so we grab that function and store it into
    # a local python variable.  The variable can then be used to call the R function
    r_data_merge = ro.globalenv['Data_Merge']

    # Call the R function (that's stored as a python variable).  Since there is only one object returned, it can be returned as normal and stored into a python variable
    # Here, the resulting return is in the form of an R DataFrame
    r_big_master_data = r_data_merge(Hist_perf,Hist_space_climate_info,Future_Space_Entry_Data,Brand_Exit,optimType)
    # end Spencer's work
    # Convert the R DataFrame to a pandas dataframes
    p_big_master_data =  pandas2ri.ri2py(r_big_master_data)
    create_output_artifact_from_dataframe(p_big_master_data)
    return (r_big_master_data,p_big_master_data)