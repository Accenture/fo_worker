        
import rpy2
import rpy2.robjects as robjects
import rpy2.robjects.vectors import DataFrame
from rpy2.objects import pandas2ri
pandas2ri.activate()
import os

import rpy2.robjects as robjects

def fetchRDF(artifact_id):
    file = fs.get(ObjectId(artifact_id))
    file = DataFrame.from_csvfile(file,header=False)
    return file

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





############## Data Merging ##############
try:
    # Read the csv files into R DataFrames
    # Going forward, these will likely not be read in from csv files, but instead
    # are generated from earlier code in worker
    Brand_Exit = DataFrame.from_csvfile('Brand_Exit.csv',header=False,sep=',')
    Hist_perf = DataFrame.from_csvfile('Hist_perf.csv',header=False,sep=',')
    Hist_space_climate_info = DataFrame.from_csvfile('Hist_space_climate_info.csv',header=False,sep=',')
    Future_Space_Entry_Data = DataFrame.from_csvfile('Future_Space_Entry_Data.csv',header=True,sep=",")

    # Parameter variable that used to be defined in the R code
    type = "Regular"

    # Source the R file into memory so it can be used
    r_source('Data_Merging_function_8_10_v3.R')

    # robjects.globalenv gives access to any global variable and functions from all sourced R scripts
    # The main function in the data merging code is called 'Data_merge', so we grab that function and store it into
    # a local python variable.  The variable can then be used to call the R function
    r_data_merge = robjects.globalenv['Data_merge']

    # Call the R function (that's stored as a python variable).  Since there is only one object returned, it can be returned as normal and stored into a python variable
    # Here, the resulting return is in the form of an R DataFrame
    r_big_master_data = r_data_merge(Hist_perf,Hist_space_climate_info,Future_Space_Entry_Data,Brand_Exit,type)
# end Spencer's work
    # Convert the R DataFrame to a pandas dataframes
    p_big_master_data = 
 
except:
    print("Data merging failed :(")





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