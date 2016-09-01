#Same import commands for all scripts
import rpy2
import pandas as pd
import numpy as np
#import rpy2POC

import rpy2.robjects as robjects
from rpy2.robjects.vectors import DataFrame
from rpy2.robjects import pandas2ri

pandas2ri.activate()

import os 

def dataMerging(jobType):
############## Data Merging ##############
    def fetchRdf(artifact_id):
            file = fs.get(ObjectId(artifact_id))
            file = DataFrame.from_csvfile(file,header=False)
            return file
    try:
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

        # Read the csv files into R DataFrames
        # Going forward, these will likely not be read in from csv files, but instead
        # are generated from earlier code in worker

        # Parameter variable that used to be defined in the R code
        # optimType = "Regular"

        # Source the R file into memory so it can be used
        r_source('rDataMerge.R')

        # robjects.globalenv gives access to any global variable and functions from all sourced R scripts
        # The main function in the data merging code is called 'Data_merge', so we grab that function and store it into
        # a local python variable.  The variable can then be used to call the R function
        r_data_merge = robjects.globalenv['Data_merge']

        # Call the R function (that's stored as a python variable).  Since there is only one object returned, it can be returned as normal and stored into a python variable
        # Here, the resulting return is in the form of an R DataFrame
        r_big_master_data = r_data_merge(Hist_perf,Hist_space_climate_info,Future_Space_Entry_Data,Brand_Exit,jobType)
    # end Spencer's work
        # Convert the R DataFrame to a pandas dataframes
        p_big_master_data =  pandas2ri.ri2py(r_big_master_data)
        create_output_artifact_from_dataframe(p_big_master_data)
    except:
        print("Data merging failed :")
    return (r_big_master_data,p_big_master_data)