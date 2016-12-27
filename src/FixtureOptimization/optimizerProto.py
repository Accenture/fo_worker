# from scipy.special import erf
# from gurobipy import *
import math
from pulp import *
import numpy as np
import pandas as pd
import os
import json
import pymongo as pm
import gridfs
import config
import datetime as dt


# Run tiered optimization algorithm
def optimizeProto(methodology, jobName, Stores, Categories, tierCounts, increment, weights, cfbsOutput, preOpt, salesPen):
    print('in the new optimization')

    cfbsOutput.reset_index(inplace=True)
    cfbsOutput.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
    # print(cfbsOutput.columns)
    print(preOpt.columns)
    mergedPreOptCF = pd.merge(cfbsOutput,
                              preOpt[['Store', 'Category', 'VSG', 'Penetration', 'Exit Flag', 'Sales Penetration']],
                              on=['Store', 'Category'])
    print('just finished merge')
    mergedPreOptCF = mergedPreOptCF.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    print('set the index')
    mergedPreOptCF.set_index(['Store', 'Category'], inplace=True)
    mergedPreOptCF['Increment Size'] = increment
    print('merged the files in the new optimization')

    def createLevels(mergedPreOptCF, increment):

        minLevel = mergedPreOptCF.loc[:, 'Lower_Limit'].min()
        maxLevel = mergedPreOptCF.loc[:, 'Upper_Limit'].max()
        Levels = list(np.arange(minLevel, maxLevel + increment, increment))
        if 0.0 not in Levels:
            Levels.append(np.abs(0.0))

        

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
    
    locSpaceToFill = mergedPreOptCF.groupby(level=0)['Space_to_Fill'].agg(np.mean)
    aggSpaceToFill = locSpaceToFill.sum()

    # Hard-coded tolerance limits for balance back constraints
    aggBalBackBound = 0.05  # 5%
    locBalBackBound = 0.10  # 10%

    print('now have balance back bounds')


    locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForTwoIncr(row, locBalBackBound, increment))
   


    print('we have local balance back')
    # EXPLORATORY ONLY: ELASTIC BALANCE BACK
    

    # Set up created tier decision variable - has a value of 1 if that space level for that category will be a tier, else 0
    ct = LpVariable.dicts('CT', (Categories, range(int(kMax))), 0, upBound=1, cat='Binary')
    print('we have created tiers')
    # Set up selected tier decision variable - has a value of 1 if a store will be assigned to the tier at that space level for that category, else 0
    st = LpVariable.dicts('ST', (Stores, Categories, range(int(kMax))), 0, upBound=1, cat='Binary')
    print('we have selected tiers')

    # Initialize the optimization problem
    NewOptim = LpProblem(jobName, LpMinimize)
    print('initialized problem')

    # Create objective function data
    if methodology == "traditional":
        objective = createErrorByLevel(Stores, Categories, Levels, mergedPreOptCF)
        objectivetype = "Total Error"
    else:  # since methodology == "enhanced"
        objective = createNegSPUByLevel(Stores, Categories, Levels, mergedPreOptCF, weights)
        objectivetype = "Total Negative SPU"
    print('created objective function data')
    # Add the objective function to the optimization problem
    NewOptim += lpSum(
        [(st[Store][Category][k] * objective[i][j][k]) for (i, Store) in enumerate(Stores) for (j, Category)
         in enumerate(Categories) for k in range(int(kMax))]), objectivetype
    print('created objective function')
    # Begin CONSTRAINT SETUP

    for (i, Store) in enumerate(Stores):
        # TODO: Exploratory analysis on impact of balance back on financials for Enhanced
        # Store-level balance back constraint: the total space allocated to products at each location must be within the individual location balance back tolerance limit
        NewOptim += lpSum(
            [(st[Store][Category][k]) * indexDict[Category,Store,k] for (j, Category) in enumerate(Categories) for k in
             range(int(kMax))]) >= locSpaceToFill[Store] * (
                    1 - locBalBackBoundAdj[Store]), "Location Balance Back Lower Limit: STR " + str(Store)
        NewOptim += lpSum(
            [(st[Store][Category][k]) * indexDict[Category,Store,k] for (j, Category) in enumerate(Categories) for k in
             range(int(kMax))]) <= locSpaceToFill[Store] * (
                    1 + locBalBackBoundAdj[Store]), "Location Balance Back Upper Limit: STR " + str(Store)


        for (j, Category) in enumerate(Categories):
            # print('we got through the first part')
            # Only one selected tier can be turned on for each product at each location.
            NewOptim += lpSum([st[Store][Category][k] for k in
                               range(int(kMax))]) == 1, "One Tier per Location: STR " + str(Store) + ", CAT " + str(
                Category)

            if mergedPreOptCF['Sales Penetration'].loc[Store, Category] < salesPen:
                NewOptim += st[Store][Category][0] == 1

    print('finished first block of constraints')
    
    for (j, Category) in enumerate(Categories):
        
        # The number of created tiers must be within the tier count limits for each product.
        try:
            NewOptim += lpSum([ct[Category][k] for k in range(int(kMax))]) >= tierCounts[Category][
                0], "Tier Count Lower Limit: CAT " + str(Category)
            NewOptim += lpSum([ct[Category][k] for k in range(int(kMax))]) <= tierCounts[Category][
                1], "Tier Count Upper Limit: CAT " + str(Category)
        except:
            print(Category)
        print('end of first')
        for k in range(int(kMax)):
            # A selected tier can be turned on if and only if the created tier at that level for that product is turned on.
            try:
                NewOptim += lpSum([st[Store][Category][k] for (i, Store) in enumerate(Stores)]) / len(Stores) <= \
                            ct[Category][k], "Selected-Created Tier Relationship: CAT " + str(
                    Category) + ", LEV: "
            except:
                print(k)
            print('end of second')

            
    print('finished second block of constraints')

    # The total space allocated to products across all locations must be within the aggregate balance back tolerance limit.
    NewOptim += lpSum([st[Store][Category][k] * indexDict[Category,Store,k] for (i, Store) in enumerate(Stores) for (j, Category) in
                       enumerate(Categories) for k in range(int(kMax))]) >= aggSpaceToFill * (
                1 - aggBalBackBound), "Aggregate Balance Back Lower Limit"
    NewOptim += lpSum([st[Store][Category][k] * indexDict[Category,Store,k] for (i, Store) in enumerate(Stores) for (j, Category) in
                       enumerate(Categories) for k in range(int(kMax))]) <= aggSpaceToFill * (
                1 + aggBalBackBound), "Aggregate Balance Back Upper Limit"

    print("to the solver we go")

    NewOptim.solve(pulp.PULP_CBC_CMD(msg=2, threads=4, maxSeconds=115200))#,fracGap=.5))

    # Debugging
    print("#####################################################################")
    print(LpStatus[NewOptim.status])
    print("#####################################################################")
    # Debugging
    NegativeCount = 0
    LowCount = 0
    TrueCount = 0
    OneCount = 0
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                if value(st[Store][Category][Level]) == 1:
                    
                    OneCount += 1
                elif value(st[Store][Category][Level]) > 0:
                    
                    TrueCount += 1
                elif value(st[Store][Category][Level]) == 0:
                    
                    LowCount += 1
                elif value(st[Store][Category][Level]) < 0:
                    
                    NegativeCount += 1

    ctNegativeCount = 0
    ctLowCount = 0
    ctTrueCount = 0
    ctOneCount = 0

    for (j, Category) in enumerate(Categories):
        for (k, Level) in enumerate(Levels):
            if value(ct[Category][Level]) == 1:
                
                ctOneCount += 1
            elif value(ct[Category][Level]) > 0:
                
                ctTrueCount += 1
            elif value(ct[Category][Level]) == 0:
                
                ctLowCount += 1
            elif value(ct[Category][Level]) < 0:
                
                ctNegativeCount += 1

    print("Status:", LpStatus[NewOptim.status])
    print("---------------------------------------------------")
    print("For Selected Tiers")
    print("Number of Negatives Count is: ", NegativeCount)
    print("Number of Zeroes Count is: ", LowCount)
    print("Number Above 0 and Below 1 Count is: ", TrueCount)
    print("Number of Selected Tiers: ", OneCount)
    print("---------------------------------------------------")
    print("For Created Tiers")
    print("Number of Negatives Count is: ", ctNegativeCount)
    print("Number of Zeroes Count is: ", ctLowCount)
    print("Number Above 0 and Below 1 Count is: ", ctTrueCount)
    print("Number of Created Tiers: ", ctOneCount)
    print("Creating Outputs")

    Results = pd.DataFrame(index=Stores, columns=Categories)
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                if value(st[Store][Category][Level]) == 1:
                    Results[Category][Store] = Level

    Results.reset_index(inplace=True)
    Results.columns.values[0] = 'Store'

    Results = pd.melt(Results.reset_index(), id_vars=['Store'], var_name='Category', value_name='Result Space')
    Results = Results.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    mergedPreOptCF.reset_index(inplace=True)
    mergedPreOptCF.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
    mergedPreOptCF = pd.merge(mergedPreOptCF, Results, on=['Store', 'Category'])
    return (LpStatus[NewOptim.status], mergedPreOptCF)