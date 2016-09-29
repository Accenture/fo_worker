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
cwd = os.getcwd()
print(cwd)

def createDataMerging():
    def fetchRdf(artifact_id):
        file = fs.get(ObjectId(artifact_id))
        file = DataFrame.from_csvfile(file,header=False)
        return file

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

### These are currently imported directly in the R code, but could be used here in the future ###

# import rpy2.robjects.packages as rpackages

# pracma = rpackages.importr('pracma')
# minpack.lm = rpackages.importr('minpack.lm')
# tidyr = rpackages.importr('tidyr')
# hydroGOF = rpackages.importr('hydroGOF')

###

from rpy2.robjects.vectors import DataFrame

# Source the R code
r_source = robjects.r['source']

# NOTE: for future use with pandas: http://pandas.pydata.org/pandas-docs/stable/r_interface.html

def create_output_artifact_from_dataframe(dataframe, *args, **kwargs):
    """
    Returns the bson.objectid.ObjectId of the resulting GridFS artifact

    """
    return fs.put(dataframe.to_csv().encode(), **kwargs)

def dataMerging():
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
        optimType = "Regular"

        # Source the R file into memory so it can be used
        r_source('Data_Merging_function_8_10_v3.R')

        # robjects.globalenv gives access to any global variable and functions from all sourced R scripts
        # The main function in the data merging code is called 'Data_merge', so we grab that function and store it into
        # a local python variable.  The variable can then be used to call the R function
        r_data_merge = robjects.globalenv['Data_merge']

        # Call the R function (that's stored as a python variable).  Since there is only one object returned, it can be returned as normal and stored into a python variable
        # Here, the resulting return is in the form of an R DataFrame
        r_big_master_data = r_data_merge(Hist_perf,Hist_space_climate_info,Future_Space_Entry_Data,Brand_Exit,optimType)
    # end Spencer's work
        # Convert the R DataFrame to a pandas dataframes
        p_big_master_data =  pandas2ri.ri2py(r_big_master_data)
        create_output_artifact_from_dataframe(p_big_master_data)
    except:
        print("Data merging failed :")
    return p_big_master_data




############## Curve fitting and bound setting ##############

#initial parameter setting for curve fitting
try:
    strcount_filter = 100
    avgsales_flter = 200
    bucket_width = 0.25
    curve_split = 0.75

    #Productivity parameters
    sales_weight = 0.7
    profit_weight = 0.2
    units_weight = 0.1

    #Bound parameters
    PCT_Space_Change_Limit = 0.5

    #Select Optimization type Regular or Drill Down
    optType = "Drill_Down"

    r_source('Curve Fitting and Bound Setting v8.0.R')

    big_master_data_Input_for_Curve_Fitting = DataFrame.from_csvfile('Input for Curve Fitting.csv',header = True, sep = ',')
    Bound_Parameters = DataFrame.from_csvfile("Bound_Parameters.csv",header=True,sep=",")

    # # Extract the main function from the R code
    r_curvefitting_boundsetting = robjects.globalenv['curvefitting_boundsetting']

    # # Call the r function with the dataframes
    r_curvefitting_boundsetting(big_master_data_Input_for_Curve_Fitting,Bound_Parameters,strcount_filter,avgsales_flter,bucket_width,curve_split,sales_weight,profit_weight,units_weight,PCT_Space_Change_Limit,optType)
except:
    print("Curve fitting failed")




############## Forecasting ##############
try:
    Fcst = DataFrame.from_csvfile("Forecast_Input.csv", header = True, sep = ",")


    r_source('Forecast_function_8_10.R')

    r_forecast = robjects.globalenv['forecast']

    r_forecast(Fcst)
except:
    print("Forecasting failed")
    
    
    
    
    
#################
# Begin KB tested functions/code, based on the above code
#################

import rpy2
import pandas as pd
import numpy as np
#import rpy2POC

import rpy2.robjects as robjects
from rpy2.robjects.vectors import DataFrame
from rpy2.robjects import pandas2ri

pandas2ri.activate()

############## Data Merging ##############

#R_Hist_perf = DataFrame.from_csvfile('transactions_data.csv',header=False,sep=',')
#R_Hist_space_climate_info = DataFrame.from_csvfile('fixture_data.csv',header=False,sep=',')
#R_Future_Space_Entry_Data = DataFrame.from_csvfile('Entry.Future Space.csv',header=True,sep=",")
#R_Brand_Exit = DataFrame.from_csvfile('exit_data.csv',header=False,sep=',')
#optimizationType = "Regular"

# Takes in 4 R data frames and a string, outputs Python data frame
def rpy2_data_merging(R_Hist_perf,R_Hist_space_climate_info,R_Future_Space_Entry_Data,R_Brand_Exit,optimizationType):

    try:
        
        
        # Update: reading in original R objects, so we don't have to convert pi2ri
        # KB Convert python fed dataframes with 2 rows header into R dataframes with 2 rows header    
        #R_Hist_perf = pandas2ri.py2ri(P_Hist_perf)
        #R_Hist_space_climate_info = pandas2ri.py2ri(P_Hist_space_climate_info) 
        #R_Future_Space_Entry_Data = pandas2ri.py2ri(P_Future_Space_Entry_Data) 
        #R_Brand_Exit = pandas2ri.py2ri(P_Brand_Exit)
        
        # Parameter variable that used to be defined in the R code
        optimType = optimizationType

        # Source the R file into memory so it can be used
        r_source('Data_Merging_function_8_10_v4.R')

        # robjects.globalenv gives access to any global variable and functions from all sourced R scripts
        # The main function in the data merging code is called 'Data_merge', so we grab that function and store it into
        # a local python variable.  The variable can then be used to call the R function
        r_data_merge = rpy2.robjects.globalenv['Data_merge']
        
        # Call the R function (that's stored as a python variable).  Since there is only one object returned
        # it can be returned as normal and stored into a python variable
        # Here, the resulting return is in the form of an R DataFrame
        r_big_master_data = r_data_merge(R_Hist_perf,R_Hist_space_climate_info,R_Future_Space_Entry_Data,R_Brand_Exit,optimType)
        
        # KB Convert resulting R Dataframe back to pandas dataframe
        p_big_master_data=pandas2ri.ri2py(r_big_master_data)
        return p_big_master_data
    except:
        print("Data merging failed :(")
    return

# Sample function call, returns python data frame
#a=rpy2_data_merging(R_Hist_perf,R_Hist_space_climate_info,R_Future_Space_Entry_Data,R_Brand_Exit,"Regular")


############## Curve fitting and bound setting ##############
# Mismatch in curve fitting bound setting code, unable to confirm and test at the moment???

# Ex: R function is expecting multiple parameters, but the parameters file only supplies 4. What to do with missing values? How are they input? Are only certain ones populated? Once these rules are defined, this function can be finished. 
def rpy2_curve_fitting_bound_setting():
    

    # Source the R code
    r_source = robjects.r['source']
    r_source('Curve Fitting and Bound Setting.R')

    # Extract the main function from the R code
    r_main_function = robjects.globalenv['main_function']

    # Call the r function with the dataframes
    r_main_function(big_master_data, parameters)

    # Pull the results of the R code from the global variables
    Analytics_Reference_Data = robjects.globalenv['g_analytics_reference_data']
    Output_Data = robjects.globalenv['g_output_data']
    return

############## Forecasting ##############

#R_Forecast_Input = DataFrame.from_csvfile("Forecast_Input.csv", header = True, sep = ",")

def rpy2_forecasting(Fcst):
    # Source R code
    r_source('Forecast_function_8_10.R')
    # Get R function name in environment
    r_forecast = robjects.globalenv['forecast']
    # Run function with R data frame input    
    r_output=r_forecast(Fcst)
    # Convert function output back to Python
    p_output = pandas2ri.ri2py(r_output)
    return(p_output)
# Sample function call, returns python data frame
#f=rpy2_forecasting(R_Forecast_Input)

def rpy2_curve_fitting_bound_setting(big_master_data,bound_input,increment_size,sales_weight,profit_weight,units_weight,PCT_Space_Change_Limit,optimType):
    try:
        #Source the R code
        r_source = robjects.r['source']
        r_source('Curve Fitting and Bound Setting KB_EDIT.r')

        # # Extract the main function from the R code
        r_curvefitting_boundsetting = robjects.globalenv['curvefitting_boundsetting']

        # # Call the r function with the dataframes
        r_list_output=r_curvefitting_boundsetting(big_master_data_Input_for_Curve_Fitting,bound_input,increment_size,sales_weight,profit_weight,units_weight,PCT_Space_Change_Limit,optimType)

        # Convert R list output into 2 python data frames, put into python list for the return statement
        p_output0=pandas2ri.ri2py(r_list_output[0])
        p_output1=pandas2ri.ri2py(r_list_output[1])
        p_list_output = [p_output0,p_output1]
        
        return p_list_output
    except:
        print("Error in curve fitting bound setting :'(")