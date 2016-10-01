import pandas as pd
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr
import pymongo as pm
import gridfs
import config


db = pm.MongoClient(config.MONGO_CON)['app']
fs = gridfs.GridFS(db)

def curveFittingBS(big_master_data,bound_input,increment_size,PCT_Space_Change_Limit,salesPen,jobType,optimType):
    def create_output_artifact_from_dataframe(dataframe, *args, **kwargs):
        """
        Returns the bson.objectid.ObjectId of the resulting GridFS artifact

        """
        return fs.put(dataframe.to_csv().encode(), **kwargs)

    pandas2ri.activate()
    bound_input=pd.DataFrame.from_dict(bound_input).T.reset_index()
    bound_input.columns=['Categories', 'Space Lower Limit', 'Space Upper Limit', 'PCT_Space_Lower_Limit', 'PCT_Space_Upper_Limit']
    # bound_inpunput.columns=['Category','Space Lower Limit','Space Upper Limit','PCT_Space_Lower_Limit','PCT_Space_Upper_Limit']
    gdata = importr("gdata")
    dataTable = importr("pracma")
    sqldf = importr("nloptr")
    tidyr = importr("tidyr")

    #Source the R code
    r_source = robjects.r['source']
    r_source('src/rCurveFitting.R')

    # # Extract the main function from the R code
    r_curvefitting_boundsetting = robjects.globalenv['curvefitting_boundsetting']
    # pandas2ri.py2ri(big_master_data)
    # # Call the r function with the dataframes
    r_list_output=r_curvefitting_boundsetting(big_master_data,bound_input,increment_size,PCT_Space_Change_Limit,salesPen,jobType,optimType,)

    # Convert R list output into 2 python data frames, put into python list for the return statement
    cfbsArtifact=pandas2ri.ri2py(r_list_output[0]).sort_values(by=['Store','Category']).reset_index(drop=True)
    cfbs_id = create_output_artifact_from_dataframe(pandas2ri.ri2py(r_list_output[0]).reset_index(drop=True))
    analytics_id = create_output_artifact_from_dataframe(pandas2ri.ri2py(r_list_output[1]).reset_index(drop=True))
    # print(cfbsArtifact.head())
    return cfbsArtifact