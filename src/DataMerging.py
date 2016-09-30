#Same import commands for all scripts
import rpy2
import pandas as pd
from pymongo import MongoClient
from rpy2.robjects import pandas2ri
import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects.vectors import DataFrame
import gridfs
from bson.objectid import ObjectId
import config
# import pandas.rpy.common as com
import sys
# ro.pandas2ri.activate()

def dataMerging(jobType,transactionArtifact,fixtureArtifact,futureSpace,brandExitArtifact):
    pandas2ri.activate()
    gdata = importr("gdata")
    dataTable = importr("data.table")
    sqldf = importr("sqldf")
    tidyr = importr("tidyr")

    # db = MongoClient(config.MONGO_CON)['app']
    # fs = gridfs.GridFS(db)
    # ro.pandas2ri.activate()
    #
    # def fetchRdf(artifact_id):
    #     file = fs.get(ObjectId(artifact_id))
    #     pFile = pd.read_csv(file)
    #     rFile=com.convert_to_r_dataframe(pFile)
    #     pandas2ri.ri2py(r[])
    #     rFile2 = DataFrame.from_csvfile(pFile,header=False)
    #     return rFile
    #
    # transactionArtifact=fetchRdf(msg["artifacts"]["spaceArtifactId"])
    # fixtureArtifact=fetchRdf(msg["artifacts"]["salesArtifactId"])
    #
    # print(type(Hist_space_climate_info))
    # print(type(Hist_perf))
    #
    # try:
    #     futureSpace=fetchRdf(msg["artifacts"]["futureSpaceId"])
    #     print("For R Future Space was Uploaded")
    # except:
    #     futureSpace='null'
    #     print("For R Future Space was not Uploaded")
    # try:
    #     brandExitArtifact=fetchRdf(msg["artifacts"]["brandExitArtifactId"])
    #     print("For R Brand Exit was Uploaded")
    # except:
    #     print("For R Brand Exit was not Uploaded")
    #     brandExitArtifact='null'

    fsFlag = 0 if futureSpace is None else 1
    beFlag = 0 if brandExitArtifact is None else 1
    print(transactionArtifact.head())
    input('Press <ENTER> to continue')
    rTransaction=pandas2ri.py2ri(transactionArtifact)
    print(rTransaction)
    print(type(rTransaction))
    input('Press <ENTER> to continue')
    rSpace=pandas2ri.py2ri(fixtureArtifact)
    print(rSpace)
    print(type(rSpace))
    input('Press <ENTER> to continue')
    rFuture=pandas2ri.py2ri(futureSpace)
    print(rFuture)
    print(type(rFuture))
    input('Press <ENTER> to continue')
    rExit=pandas2ri.py2ri(brandExitArtifact)
    print(rExit.head())
    print(type(rExit))
    input('Press <ENTER> to continue')
    sys.exit("Which doesn't work")
    # Source the R file into memory so it can be used
    r_source = ro.r['source']
    # dm=r_source('src/rDataMerge.R')
    # dm.Data_Merge(jobType, transactionArtifact, fixtureArtifact, futureSpace, fsFlag, brandExitArtifact, beFlag)


    # robjects.globalenv gives access to any global variable and functions from all sourced R scripts
    # The main function in the data merging code is called 'Data_merge', so we grab that function and store it into
    # a local python variable.  The variable can then be used to call the R function
    r_source('src/rDataMerge.R')
    r_data_merge = ro.globalenv['Data_Merge']

    # Call the R function (that's stored as a python variable).  Since there is only one object returned, it can be returned as normal and stored into a python variable
    # Here, the resulting return is in the form of an R DataFrame
    test=r_data_merge(jobType,rTransaction,rSpace,rFuture,fsFlag,rExit,beFlag)
    # r_big_master_data = r_data_merge(jobType,transactionArtifact,fixtureArtifact,futureSpace,fsFlag,brandExitArtifact,beFlag)
    # end Spencer's work
    # Convert the R DataFrame to a pandas dataframes
    p_big_master_data =  pandas2ri.ri2py(r_big_master_data)
    create_output_artifact_from_dataframe(p_big_master_data)
    return (r_big_master_data,p_big_master_data)