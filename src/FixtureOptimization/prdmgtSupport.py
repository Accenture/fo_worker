import math
from pulp import *
import numpy as np
import pandas as pd

import math
from pulp import *
import numpy as np
import pandas as pd
import os
import json
import pymongo as pm
import gridfs
# import config
import datetime as dt


# Run tiered optimization algorithm
def curveDataInformation(methodology, jobName, Stores, Categories, tierCounts, increment, weights, mergedPreOptCF, salesPen):
#     mergedPreOptCF['Increment Size'] = increment
    print('merged the files in the new optimization')

    def createLevels(mergedPreOptCF, increment):

        minLevel = mergedPreOptCF.loc[:, 'Lower_Limit'].min()
        maxLevel = mergedPreOptCF.loc[:, 'Upper_Limit'].max()
        Levels = list(np.arange(minLevel, maxLevel + increment, increment))
        if 0.0 not in Levels:
            Levels.append(np.abs(0.0))

        # print(Levels)  # for unit testing

        return Levels

    def roundValue(cVal, increment):
        if np.mod(round(cVal, 3), increment) > increment / 2:
            cVal = round(cVal, 3) + (increment - (np.mod(round(cVal, 3), increment)))
        else:
            cVal = round(cVal, 3) - np.mod(round(cVal, 3), increment)
        return cVal


    # Create eligible space levels
    mergedPreOptCF["Upper_Limit"] = mergedPreOptCF["Upper_Limit"].apply(lambda x: roundValue(x, increment))
    mergedPreOptCF["Lower_Limit"] = mergedPreOptCF["Lower_Limit"].apply(lambda x: roundValue(x, increment))
    Levels = createLevels(mergedPreOptCF, increment)
    print('we have levels')

    mergedPreOptCF['Num Indeces'] = (mergedPreOptCF["Upper_Limit"] - mergedPreOptCF["Lower_Limit"]) / mergedPreOptCF[
        "Increment Size"] + 1
    kMax = max(mergedPreOptCF['Num Indeces'])

    def createIndexDict(df, k_max, Stores, Categories):
        kvals = dict()
        opal = 0
        for (j, Category) in enumerate(Categories):
            for (i, Store) in enumerate(Stores):
                # Get Parameters
                min_val = df['Lower_Limit'].loc[Store, Category]
                max_val = df['Upper_Limit'].loc[Store, Category]
                increment = df['Increment Size'].loc[Store, Category]

                c = min_val  # Initialize First Index Value

                for k in range(int(k_max)):
                    if c > max_val:
                        kvals[Category, Store, k] = 10000
                    else:
                        kvals[Category, Store, k] = c
                        c = c + increment
                    if opal % 10000 == 0:
                        print(opal)
                    opal = opal + 1
        return kvals

    indexDict=createIndexDict(mergedPreOptCF,int(kMax),Stores,Categories)

    # Helper function for createSPUByLevel function, to forecast weighted combination of sales, profit, and units
    # str_cat is the row of the curve-fitting output for an individual store and category
    # variable can take on the values of "Sales", "Profit", or "Units"
    def forecast(str_cat, space, variable):

        if space < str_cat["Scaled_BP_" + variable]:
            value = space * (str_cat["Scaled_Alpha_" + variable] * (math.erf(
                (str_cat["Scaled_BP_" + variable] - str_cat["Scaled_Shift_" + variable]) / ((
                    math.sqrt(2) * str_cat["Scaled_Beta_" + variable])))) / str_cat["Scaled_BP_" + variable])
        else:
            value = str_cat["Scaled_Alpha_" + variable] * math.erf(
                (space - str_cat["Scaled_Shift_" + variable]) / (math.sqrt(2) * str_cat["Scaled_Beta_" + variable]))

        return value

    # Helper function for optimize function, to create objective function of SPU by level for Enhanced optimizations
    def createNegSPUByLevel(Stores, Categories, Levels, curveFittingOutput, enhMetrics):

        # Create n-dimensional array to store Estimated SPU by level
        est_neg_spu_by_lev = np.zeros((len(Stores), len(Categories), len(Levels)))

        sU = "Sales"
        pU = "Profit"
        uU = "Units"
        sL = "sales"
        pL = "profits"
        uL = "units"

        print('forecasting outputs')
        # Calculate SPU by level
        for (i, Store) in enumerate(Stores):
            for (j, Category) in enumerate(Categories):
                for (k, Level) in enumerate(Levels):
                    str_cat = curveFittingOutput.loc[Store, Category]
                    est_neg_spu_by_lev[i][j][k] = - (
                        (enhMetrics[sL] / 100) * forecast(str_cat, Level, sU) + (enhMetrics[pL] / 100) * forecast(
                            str_cat,
                            Level,
                            pU) + (
                            enhMetrics[uL] / 100) * forecast(str_cat, Level, uU))
        print('finished forecasting')
        # np.savetxt('spuMatrix.csv',est_neg_spu_by_lev,delimiter=",")
        return est_neg_spu_by_lev

    # Helper function for optimize function, to create objective function of error by level for Traditional optimizations
    def createErrorByLevel(Stores, Categories, Levels, mergedCurveFitting):
        # Create n-dimensional array to store error by level
        error = np.zeros((len(Stores), len(Categories), len(Levels)))

        # Calculate error by level
        for (i, Store) in enumerate(Stores):
            for (j, Category) in enumerate(Categories):
                for (k, Level) in enumerate(Levels):
                    error[i][j][k] = np.absolute(mergedCurveFitting.loc[Store, Category]["Optimal Space"] - Level)
        return error

    # Adjust location balance back tolerance limit so that it's at least 2 increments
    def adjustForTwoIncr(row, bound, increment):
        return max(bound, (2 * increment) / row)

    def adjustForFiveIncr(row, bound, increment):
        return max(bound, (5 * increment) / row)

    print('completed all of the function definitions')
    # Identify the total amount of space to fill in the optimization for each location and for all locations
    # locSpaceToFill = pd.Series(mergedPreOptCF.groupby('Store')['Space_to_Fill'].sum())
    locSpaceToFill = mergedPreOptCF.groupby(level=0)['Space_to_Fill'].agg(np.mean)
    aggSpaceToFill = locSpaceToFill.sum()

    # Hard-coded tolerance limits for balance back constraints
    aggBalBackBound = 0.05  # 5%
    locBalBackBound = 0.10  # 10%

    print('now have balance back bounds')

    locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForTwoIncr(row, locBalBackBound, increment))
    # locBalBackBoundAdj.to_csv(str(jobName)+'balanceBackVector.csv',sep=",")


    print('we have local balance back')
 
    # Create objective function data
    if methodology == "traditional":
        objective = createErrorByLevel(Stores, Categories, Levels, mergedPreOptCF)
        objectivetype = "Total Error"
    else:  # since methodology == "enhanced"
        objective = createNegSPUByLevel(Stores, Categories, Levels, mergedPreOptCF, weights)
        objectivetype = "Total Negative SPU"
    print('created objective function data')

    out_df = pd.DataFrame(columns=["i", "j", "k", "f(i,j,k)", "x(j)", "y(j)", "l(i,j)", "u(i,j)", "a", "b", "w(i)"])
    for (i,Store) in enumerate(Stores):
        for (j,Category) in enumerate(Categories):
            for (k,Level) in enumerate(Levels):
                f_ijk = objective[i][j][k]
                x_j = tierCounts['tierLower'][Category]
                y_j = tierCounts['tierUpper'][Category]
                l_ij = mergedPreOptCF["Lower_Limit"].loc[Store,Category]
                u_ij = mergedPreOptCF["Upper_Limit"].loc[Store,Category]
                a_val = locSpaceToFill[Store] * (1 - locBalBackBoundAdj[Store])
                b_val = locSpaceToFill[Store] * (1 + locBalBackBoundAdj[Store])
                w_i = locSpaceToFill[Store]
                row_data = [Store, Category, indexDict[Category,Store,Level], f_ijk, x_j, y_j, l_ij, u_ij, a_val, b_val, w_i]
                out_df.loc[len(out_df)] = row_data
    return out_df


def curveDataOutput(mergedPreOptCF,Stores,Categories,Levels,tierCounts,locBalBackBoundAdj,locSpaceToFill,objective)
    def createIndexDict(df, k_max, Stores, Categories):
        kvals = dict()
        opal = 0
        for (j, Category) in enumerate(Categories):
            for (i, Store) in enumerate(Stores):
                # Get Parameters
                min_val = df['Lower_Limit'].loc[Store, Category]
                max_val = df['Upper_Limit'].loc[Store, Category]
                increment = df['Increment Size'].loc[Store, Category]

                c = min_val  # Initialize First Index Value

                for k in range(int(k_max)):
                    if c > max_val:
                        kvals[Category, Store, k] = 10000
                    else:
                        kvals[Category, Store, k] = c
                        c = c + increment
                    if opal % 10000 == 0:
                        print(opal)
                    opal = opal + 1
        return kvals

    mergedPreOptCF['Num Indeces'] = (mergedPreOptCF["Upper_Limit"] - mergedPreOptCF["Lower_Limit"]) / mergedPreOptCF[
        "Increment Size"] + 1
    kMax = max(mergedPreOptCF['Num Indeces'])
    indexDict = createIndexDict(mergedPreOptCF, int(kMax), Stores, Categories)

    # def curveTableCreate (Stores,Categories,Levels,indexDict,locSpaceToFill,mergedPreOptCF,tierCounts,objective):
    out_df = pd.DataFrame(columns=["i", "j", "k", "f(i,j,k)", "x(j)", "y(j)", "l(i,j)", "u(i,j)", "a", "b", "w(i)"])
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                f_ijk = objective[i][j][k]
                x_j = tierCounts['tierLower'][Category]
                y_j = tierCounts['tierUpper'][Category]
                l_ij = mergedPreOptCF["Lower_Limit"].loc[Store, Category]
                u_ij = mergedPreOptCF["Upper_Limit"].loc[Store, Category]
                a_val = locSpaceToFill[Store] * (1 - locBalBackBoundAdj[Store])
                b_val = locSpaceToFill[Store] * (1 + locBalBackBoundAdj[Store])
                w_i = locSpaceToFill[Store]
                row_data = [Store, Category, indexDict[Category, Store, Level], f_ijk, x_j, y_j, l_ij, u_ij, a_val, b_val,
                            w_i]
                out_df.loc[len(out_df)] = row_data
    return out_df