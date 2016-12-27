import pandas as pd
import numpy as np
from scipy.special import erf
import math
import datetime as dt

# Helper function for createSPU function(s), to forecast weighted combination of sales, profit, and units
# str_cat is the row of the curve-fitting output for an individual store and category
# variable can take on the values of "Sales", "Profit", or "Units"
def forecast(str_cat, space, variable):
    if space < str_cat["Scaled_BP_" + variable]:
        value = space * (str_cat["Scaled_Alpha_" + variable] * (erf(
            (str_cat["Scaled_BP_" + variable] - str_cat["Scaled_Shift_" + variable]) / (
            math.sqrt(2) * str_cat["Scaled_Beta_" + variable]))) / str_cat["Scaled_BP_" + variable])
    else:
        value = str_cat["Scaled_Alpha_" + variable] * erf(
            (space - str_cat["Scaled_Shift_" + variable]) / (math.sqrt(2) * str_cat["Scaled_Beta_" + variable]))

    return value


def optimalForecast(lOutput):

    variables = ["Sales", "Profit", "Units"]

    for v in variables:
        lOutput["Optimal Estimated " + v] = np.where(lOutput["Optimal Space"] < lOutput["Scaled_BP_" + v],lOutput["Optimal Space"] * (lOutput["Scaled_Alpha_" + v] * (erf((lOutput["Scaled_BP_" + v] - lOutput["Scaled_Shift_" + v]) / (math.sqrt(2) * lOutput["Scaled_Beta_" + v]))) / lOutput["Scaled_BP_" + v]),lOutput["Scaled_Alpha_" + v] * erf((lOutput["Optimal Space"] - lOutput["Scaled_Shift_" + v]) / (math.sqrt(2) * lOutput["Scaled_Beta_" + v])))

    return lOutput

# Helper function for single store optimize function, to create SPU by level
def createSPUForSingleStoreOpt(strnum, Categories, curveFittingQuery, w, incr, maxNumIncr):

    # Create n-dimensional array to store Estimated SPU
    spu_for_ss = np.zeros(shape=(len(Categories), maxNumIncr+1))
    incr_spu_for_ss = np.zeros(shape=(len(Categories), maxNumIncr+1))

    s = "Sales"
    p = "Profit"
    u = "Units"

    # Calculate SPU and incremental SPU by level
    for (j, Category) in enumerate(Categories):
        for k in range(maxNumIncr+1):
            str_cat = curveFittingQuery.loc[strnum, Category]
            spu_for_ss[j][k] = (w['sales'] * forecast(str_cat, k*incr, s) + w['profits'] * forecast(str_cat, k*incr, p) + w['units'] * forecast(str_cat, k*incr, u))
            if k == 0:
                incr_spu_for_ss[j][k] = 0
            else:
                incr_spu_for_ss[j][k] = spu_for_ss[j][k] - spu_for_ss[j][k-1]

    return incr_spu_for_ss

# Main function for single store optimization
def optimizeSingleStore(curve_fitting,incr,w):
    print('in single store')
    Stores = curve_fitting.index.levels[0].unique()
    Categories = curve_fitting.index.levels[1].unique()
    ss_optimals_wide=pd.DataFrame(index=Stores,columns=Categories)
    print('entering giant loop')
    for Store in Stores:

        # Select store info from curve fitting
        curr_str_cf = curve_fitting.query('Store == '+str(Store))

        # Identify max number of increments in any category
        max_num_incr = math.ceil(curr_str_cf["Upper_Limit"].max()/incr)
        incr_spu_returned = createSPUForSingleStoreOpt(Store, Categories, curr_str_cf, w, incr, max_num_incr)

        # Initialize single store optimals at historical, or at bound if historical was outside bound
        lower = np.zeros(shape=(len(Categories)),dtype = int)
        upper = np.zeros(shape=(len(Categories)),dtype = int)
        optimal = np.zeros(shape=(len(Categories)),dtype = int)
        for (j, Category) in enumerate(Categories):
            lower[j] = int(curr_str_cf.loc[Store, Category]["Lower_Limit"]/incr)
            upper[j] = int(curr_str_cf.loc[Store, Category]["Upper_Limit"]/incr)
            optimal[j] = min(upper[j], max(lower[j], int(curr_str_cf.loc[Store, Category]["Space"]/incr)))

        # Balance to space to fill except where not feasible
        incr_to_fill = int(curr_str_cf["Space_to_Fill"].mean()/incr)
        adj_incr_to_fill = min(upper.sum(), max(lower.sum(), incr_to_fill))
        eligible_adds_incr_spu = np.zeros(shape=(len(Categories)))
        eligible_rems_incr_spu = np.zeros(shape=(len(Categories)))
        if adj_incr_to_fill > optimal.sum():
            while adj_incr_to_fill > optimal.sum():
                for (j, Category) in enumerate(Categories):
                    if optimal[j] < upper[j]:
                        eligible_adds_incr_spu[j] = incr_spu_returned[j][optimal[j] + 1]
                    else:
                        eligible_adds_incr_spu[j] = 0
                h = np.argmax(eligible_adds_incr_spu)
                optimal[h] += 1
        elif adj_incr_to_fill < optimal.sum():
            while adj_incr_to_fill < optimal.sum():
                for (j, Category) in enumerate(Categories):
                    if optimal[j] > lower[j]:
                        eligible_rems_incr_spu[j] = incr_spu_returned[j][optimal[j]]
                    else:
                        eligible_rems_incr_spu[j] = np.inf
                l = np.argmin(eligible_rems_incr_spu)
                optimal[l] -= 1

        # Trade space to optimize
        trade = True
        eligible_adds_incr_spu = np.zeros(shape=(len(Categories)))
        eligible_rems_incr_spu = np.zeros(shape=(len(Categories)))
        while trade:
            for (j, Category) in enumerate(Categories):
                if optimal[j] < upper[j]:
                    eligible_adds_incr_spu[j] = incr_spu_returned[j][optimal[j] + 1]
                else:
                    eligible_adds_incr_spu[j] = 0
                if optimal[j] > lower[j]:
                    eligible_rems_incr_spu[j] = incr_spu_returned[j][optimal[j]]
                else:
                    eligible_rems_incr_spu[j] = np.inf
            h = np.argmax(eligible_adds_incr_spu)
            l = np.argmin(eligible_rems_incr_spu)
            if h == l:
                eligible_adds_incr_spu[h] = 0
                h = np.argmax(eligible_adds_incr_spu)
            if eligible_rems_incr_spu[l] < eligible_adds_incr_spu[h]:
                optimal[h] += 1
                optimal[l] -= 1
            else:
                trade = False

        # Append optimal space to wide table
        for (j, Category) in enumerate(Categories):
            ss_optimals_wide[Category][Store] = optimal[j]*incr

    print('out of the loop')
    ss_optimals_long = pd.DataFrame(ss_optimals_wide.unstack()).swaplevel()
    ss_optimals_long.rename(columns={ss_optimals_long.columns[-1]: "Optimal Space"}, inplace=True)
    ss_optimals_long["Optimal Space"] = ss_optimals_long["Optimal Space"].astype(float)
    cf_plus_ss_optimals = pd.concat([curve_fitting, ss_optimals_long], axis=1)
    optCFBS=optimalForecast(cf_plus_ss_optimals)
    return (ss_optimals_wide,optCFBS)

def addForecast(lOutput):

    variables = ["Sales", "Profit", "Units"]

    for v in variables:
        lOutput["Optimal Estimated " + v] = np.where(lOutput["Optimal Space"] < lOutput["Scaled_BP_" + v],lOutput["Optimal Space"] * (lOutput["Scaled_Alpha_" + v] * (erf((lOutput["Scaled_BP_" + v] - lOutput["Scaled_Shift_" + v]) / (math.sqrt(2) * lOutput["Scaled_Beta_" + v]))) / lOutput["Scaled_BP_" + v]),lOutput["Scaled_Alpha_" + v] * erf((lOutput["Optimal Space"] - lOutput["Scaled_Shift_" + v]) / (math.sqrt(2) * lOutput["Scaled_Beta_" + v])))

    return lOutput