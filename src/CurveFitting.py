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

def curveFittingBS(big_master_data,bound_input,increment_size,sales_weight,profit_weight,units_weight,PCT_Space_Change_Limit,optimType):
    try:
        #Source the R code
        r_source = robjects.r['source']
        r_source('rCurveFitting.r')

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