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
def optimize2(methodology, jobName, Stores, Categories, tierCounts, increment, weights, cfbsOutput, preOpt, salesPen):
    print('in the new optimization')
    # Helper function for optimize function, to create eligible space levels
    # print(cfbsOutput.columns)
    # print(preOpt.columns)
    # preOpt.set_index(['Store','Category'],inplace=True)
    # print(preOpt.index)
    # mergedPreOptCF = pd.merge(cfbsOutput, preOpt[['Penetration','Exit Flag','Sales Penetration']])


    cfbsOutput.reset_index(inplace=True)
    cfbsOutput.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
    # print(cfbsOutput.columns)
    # print(preOpt.columns)
    mergedPreOptCF = pd.merge(cfbsOutput, preOpt[['Store', 'Category', 'VSG', 'Penetration','Exit Flag','Sales Penetration']],
                              on=['Store', 'Category'])
    print('just finished merge')
    mergedPreOptCF = mergedPreOptCF.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    print('set the index')
    mergedPreOptCF.set_index(['Store', 'Category'], inplace=True)
    print(mergedPreOptCF.columns)

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
    # EXPLORATORY ONLY: ELASTIC BALANCE BACK
    # Hard-coded tolerance limits for balance back constraints without penalty
    # The free bounds are the % difference from space to fill that is allowed without penalty
    # The penalty is incurred if the filled space goes beyond the free bound % difference from space to fill
    # The tighter the bounds and/or higher the penalties, the slower the optimization run time
    # The penalty incurred should be different for Traditional vs Enhanced as the scale of the objective function differs
    # aggBalBackFreeBound = 0.01 #exploratory, value would have to be determined through exploratory analysis
    # aggBalBackPenalty = increment*10 #exploratory, value would have to be determined through exploratory analysis
    # locBalBackFreeBound = 0.05 #exploratory, value would have to be determined through exploratory analysis
    # locBalBackPenalty = increment #exploratory, value would have to be determined through exploratory analysis

    # try:
    #     locBalBackBoundAdj = locSpaceToFill.apply(lambda row:adjustForTwoIncr(row,locBalBackBound,increment))
    # except:
    #     print("Divide by 0. \n There is a store that doesn't have any space assigned whatsoever.")
    #     return False

    locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForTwoIncr(row, locBalBackBound, increment))

    print('we have local balance back')
    # EXPLORATORY ONLY: ELASTIC BALANCE BACK
    # locBalBackFreeBoundAdj = locSpaceToFill.apply(lambda row:adjustForTwoIncr(row,locBalBackFreeBound,increment))

    # Create eligible space levels
    mergedPreOptCF["Upper_Limit"] = mergedPreOptCF["Upper_Limit"].apply(lambda x: roundValue(x, increment))
    mergedPreOptCF["Lower_Limit"] = mergedPreOptCF["Lower_Limit"].apply(lambda x: roundValue(x, increment))
    Levels = createLevels(mergedPreOptCF, increment)
    print('we have levels')
    # Set up created tier decision variable - has a value of 1 if that space level for that category will be a tier, else 0
    ct = LpVariable.dicts('CT', (Categories, Levels), 0, upBound=1, cat='Binary')
    print('we have created tiers')
    # Set up selected tier decision variable - has a value of 1 if a store will be assigned to the tier at that space level for that category, else 0
    st = LpVariable.dicts('ST', (Stores, Categories, Levels), 0, upBound=1, cat='Binary')
    print('we have selected tiers')
    # EXPLORATORY ONLY: MINIMUM STORES PER TIER
    # m = 50 #minimum stores per tier

    # EXPLORATORY ONLY: SET INITIAL VALUES
    # Could potentially reduce run time
    # This feature is not implemented for CBC or Gurobi solvers but is believed to be implemented for CPLEX (not tested)
    # Could also set initial values for created tiers and/or use a heuristic to set for both in such a way that they align
    # Sets initial value for the selected tier decision variables to single store optimal (only works for Traditional)
    # Other ways to set would include the historical space or the average of the store-category bounds
    # for (i, Store) in enumerate(Stores):
    #     for (j, Category) in enumerate(Categories):
    #         for (k, Level) in enumerate(Levels):
    #             if opt_amt[Category].iloc[i] == k:
    #                 st[Store][Category][Level].setInitialValue(1)
    #             else:
    #                 st[Store][Category][Level].setInitialValue(0)

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

    # pd.DataFrame(objective).to_csv(str(jobName)+'objective.csv',sep=",")
    print('created objective function data')
    # Add the objective function to the optimization problem
    NewOptim += lpSum(
        [(st[Store][Category][Level] * objective[i][j][k]) for (i, Store) in enumerate(Stores) for (j, Category)
         in enumerate(Categories) for (k, Level) in enumerate(Levels)]), objectivetype
    print('created objective function')
    # Begin CONSTRAINT SETUP

    for (i, Store) in enumerate(Stores):
        # TODO: Exploratory analysis on impact of balance back on financials for Enhanced
        # Store-level balance back constraint: the total space allocated to products at each location must be within the individual location balance back tolerance limit
        NewOptim += lpSum(
            [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
             enumerate(Levels)]) >= locSpaceToFill[Store] * (
                    1 - locBalBackBoundAdj[Store]), "Location Balance Back Lower Limit: STR " + str(Store)
        NewOptim += lpSum(
            [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
             enumerate(Levels)]) <= locSpaceToFill[Store] * (
                    1 + locBalBackBoundAdj[Store]), "Location Balance Back Upper Limit: STR " + str(Store)

        # EXPLORATORY ONLY: ELASTIC BALANCE BACK
        # Penalize balance back by introducing an elastic subproblem constraint
        # Increases optimization run time
        # makeElasticSubProblem only works on minimize problems, so Enhanced must be written as minimize negative SPU
        # eLocSpace = lpSum([(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in enumerate(Levels)])
        # cLocBalBackPenalty = LpConstraint(e=eLocSpace, sense=LpConstraintEQ, name="Location Balance Back Penalty: Store " + str(Store),rhs=locSpaceToFill[Store])
        # NewOptim.extend(cLocBalBackPenalty.makeElasticSubProblem(penalty=locBalBackPenalty,proportionFreeBound=locBalBackFreeBoundAdj[Store]))

        for (j, Category) in enumerate(Categories):
            # print('we got through the first part')
            # Only one selected tier can be turned on for each product at each location.
            NewOptim += lpSum([st[Store][Category][Level] for (k, Level) in
                               enumerate(Levels)]) == 1, "One Tier per Location: STR " + str(Store) + ", CAT " + str(
                Category)

            # The space allocated to each product at each location must be between the minimum and the maximum allowed for that product at the location.
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k, Level) in enumerate(Levels)]) >= \
                        mergedPreOptCF["Lower_Limit"].loc[Store, Category], "Space Lower Limit: STR " + str(
                Store) + ", CAT " + str(Category)
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k, Level) in enumerate(Levels)]) <= \
                        mergedPreOptCF["Upper_Limit"].loc[Store, Category], "Space Upper Limit: STR " + str(
                Store) + ", CAT " + str(Category)
            if mergedPreOptCF['Sales Penetration'].loc[Store, Category] < salesPen:
                NewOptim += st[Store][Category][0] == 1

    print('finished first block of constraints')
    # totalTiers=0
    for (j, Category) in enumerate(Categories):
        # totalTiers=totalTiers+tierCounts[Category][1]
        # The number of created tiers must be within the tier count limits for each product.
        NewOptim += lpSum([ct[Category][Level] for (k, Level) in enumerate(Levels)]) >= tierCounts[Category][
            0], "Tier Count Lower Limit: CAT " + str(Category)
        NewOptim += lpSum([ct[Category][Level] for (k, Level) in enumerate(Levels)]) <= tierCounts[Category][
            1], "Tier Count Upper Limit: CAT " + str(Category)

        for (k, Level) in enumerate(Levels):
            # A selected tier can be turned on if and only if the created tier at that level for that product is turned on.
            NewOptim += lpSum([st[Store][Category][Level] for (i, Store) in enumerate(Stores)]) / len(Stores) <= \
                        ct[Category][Level], "Selected-Created Tier Relationship: CAT " + str(
                Category) + ", LEV: " + str(Level)

            # EXPLORATORY ONLY: MINIMUM STORES PER TIER
            # Increases optimization run time
            # if Level > 0:
            #        NewOptim += lpSum([st[Store][Category][Level] for (i, Store) in enumerate(Stores)]) >= m * ct[Category][Level], "Minimum Stores per Tier: CAT " + Category + ", LEV: " + str(Level)
    print('finished second block of constraints')

    # NewOptim += lpSum([ct[Category][Level] for (j, Category) in enumerate(Categories) for (k, Level) in enumerate(Levels)]) <= totalTiers
    # print('finished total tiers constraint')

    # The total space allocated to products across all locations must be within the aggregate balance back tolerance limit.
    NewOptim += lpSum([st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in
                       enumerate(Categories) for (k, Level) in enumerate(Levels)]) >= aggSpaceToFill * (
                1 - aggBalBackBound), "Aggregate Balance Back Lower Limit"
    NewOptim += lpSum([st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in
                       enumerate(Categories) for (k, Level) in enumerate(Levels)]) <= aggSpaceToFill * (
                1 + aggBalBackBound), "Aggregate Balance Back Upper Limit"

    # EXPLORATORY ONLY: ELASTIC BALANCE BACK
    # Penalize balance back by introducing an elastic subproblem constraint
    # Increases optimization run time
    # makeElasticSubProblem only works on minimize problems, so Enhanced must be written as minimize negative SPU
    # eAggSpace = lpSum([st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for (k, Level) in enumerate(Levels)])
    # cAggBalBackPenalty = LpConstraint(e=eAggSpace,sense=LpConstraintEQ,name="Aggregate Balance Back Penalty",rhs = aggSpaceToFill)
    # NewOptim.extend(cAggBalBackPenalty.makeElasticSubProblem(penalty= aggBalBackPenalty,proportionFreeBound = aggBalBackFreeBound))

    # Time stamp for optimization solve time
    # start_seconds = dt.datetime.today().hour*60*60+ dt.datetime.today().minute*60 + dt.datetime.today().second
    print("to the solver we go")
    # NewOptim.solve()

    mergedPreOptCF.reset_index(inplace=True)
    # mergedPreOptCF.to_csv(str(jobName)+'.csv',sep=',')
    # NewOptim.writeMPS(str(jobName)+".mps")
    # return
    # Solve the problem using open source solver
    NewOptim.solve(pulp.PULP_CBC_CMD(msg=2,threads=4,fracGap=.1))
    # NewOptim.solve(pulp.PULP_CBC_CMD(msg=2,threads=4,cuts=100,options=['sec','600'],fracGap=.1))
    # solver = "CBC" #for unit testing

    # Solve the problem using Gurobi
    # NewOptim.solve(pulp.GUROBI())
    # solver = "Gurobi" #for unit testing

    # Time stamp for optimization solve time
    # solve_end_seconds = dt.datetime.today().hour*60*60 + dt.datetime.today().minute*60 + dt.datetime.today().second
    # solve_seconds = solve_end_seconds - start_seconds
    # print("Time taken to solve optimization was:" + str(solve_seconds)) #for unit testing

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
                    # print(st[Store][Category][Level]) #These values should only be a one or a zero
                    OneCount += 1
                elif value(st[Store][Category][Level]) > 0:
                    # print(st[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                    TrueCount += 1
                elif value(st[Store][Category][Level]) == 0:
                    # print(value(st[Store][Category][Level])) #These values should only be a one or a zero
                    LowCount += 1
                elif value(st[Store][Category][Level]) < 0:
                    # print(st[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                    NegativeCount += 1

    ctNegativeCount = 0
    ctLowCount = 0
    ctTrueCount = 0
    ctOneCount = 0

    for (j, Category) in enumerate(Categories):
        for (k, Level) in enumerate(Levels):
            if value(ct[Category][Level]) == 1:
                # print(value(ct[Store][Category][Level])) #These values should only be a one or a zero
                ctOneCount += 1
            elif value(ct[Category][Level]) > 0:
                # print(ct[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                ctTrueCount += 1
            elif value(ct[Category][Level]) == 0:
                # print(value(ct[Category][Level])) #These values should only be a one or a zero
                ctLowCount += 1
            elif value(ct[Category][Level]) < 0:
                # print(ct[Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
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
    # Results.rename(
    #     columns={'level_0': 'Store'},
    #     inplace=True)
    Results = pd.melt(Results.reset_index(), id_vars=['Store'], var_name='Category', value_name='Result Space')
    Results = Results.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    mergedPreOptCF.reset_index(inplace=True)
    mergedPreOptCF.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
    mergedPreOptCF = pd.merge(mergedPreOptCF, Results, on=['Store', 'Category'])
    return (LpStatus[NewOptim.status], mergedPreOptCF)


