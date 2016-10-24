import numpy as np
import pandas as pd

#Helper function for optimize function, to create eligible space levels
def createLevels(mergedPreOptCF,increment):

    minLevel = mergedPreOptCF.loc[:, 'Lower_Limit'].min()
    maxLevel = mergedPreOptCF.loc[:, 'Upper_Limit'].max()
    Levels = list(np.arange(minLevel, maxLevel + increment, increment))
    if 0.0 not in Levels:
        Levels.append(np.abs(0.0))

    print(Levels) #for unit testing

    return Levels

# Helper function for createSPUByLevel function, to forecast weighted combination of sales, profit, and units
# str_cat is the row of the curve-fitting output for an individual store and category
# variable can take on the values of "Sales", "Profit", or "Units"
def forecast(str_cat, space, variable) :

    if space < str_cat["Scaled_BP_" + variable]:
        value = space * (str_cat["Scaled_Alpha_" + variable] *(erf((str_cat["Scaled_BP_" + variable] - str_cat["Scaled_Shift_" + variable]) / (math.sqrt(2) * str_cat["Scaled_Beta_" + variable])))/ str_cat["Scaled_BP_" + variable])
    else:
        value = str_cat["Scaled_Alpha_" + variable] * erf((space - str_cat["Scaled_Shift_" + variable]) / (math.sqrt(2) * str_cat["Scaled_Beta_" + variable]))

    return value


# Helper function for optimize function, to create objective function of SPU by level for Enhanced optimizations
def createNegSPUByLevel(Stores, Categories, Levels, curveFittingOutput, enhMetrics):

    # Create n-dimensional array to store Estimated SPU by level
    est_neg_spu_by_lev = np.zeros((len(Stores), len(Categories), len(Levels)))

    s = "Sales"
    p = "Profit"
    u = "Units"

    # Calculate SPU by level
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                str_cat = curveFittingOutput.loc[Store, Category]
                est_neg_spu_by_lev[i][j][k] = - ((enhMetrics[s] / 100) * forecast(str_cat, Level, s) + (enhMetrics[p] / 100) * forecast(str_cat, Level, p) + (enhMetrics[u] / 100) * forecast(str_cat, Level, u))

    return est_neg_spu_by_lev

# Helper function for optimize function, to create objective function of error by level for Traditional optimizations
def createErrorByLevel(Stores, Categories, Levels,cf):

    # Create n-dimensional array to store error by level
    error = np.zeros((len(Stores), len(Categories), len(Levels)))

    # Calculate error by level
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                error[i][j][k] = np.absolute(cf.loc[Store, Category]["Optimal Space"] - Level)
#                error[i][j][k] = np.absolute(opt_amt[Category].iloc[i] - Level)

    return error
