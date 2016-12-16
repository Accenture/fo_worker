#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Thu Jun  2 11:33:51 2016

@author: kenneth.l.sylvain
"""

from pulp import *
import numpy as np
import pandas as pd
import datetime as dt
from itertools import product
import logging
from FixtureOptimization.outputFunctions import outputValidation

def optimizeDD(jobName, increment, dataMunged, salesPen,mipGap = None):
    """
    Run an LP-based optimization

    Side-effects:
        - creates file: Fixture_Optimization.lp (constraints)
        - creates file: solvedout.csv <= to be inserted into db
        - creates file: solvedout.text

    Synopsis:
        I just wrapped the script from Ken in a callable - DCE
    """
    def roundValue(cVal, increment):
        if np.mod(round(cVal, 3), increment) > increment / 2:
            cVal = round(cVal, 3) + (increment - (np.mod(round(cVal, 3), increment)))
        else:
            cVal = round(cVal, 3) - np.mod(round(cVal, 3), increment)
        return cVal



    dataMunged['Optimal Space'] = dataMunged['Optimal Space'].apply(lambda x: roundValue(x, increment))
    dataMunged = dataMunged.apply(lambda x: pd.to_numeric(x, errors='ignore'))

    start_time = dt.datetime.today().hour * 60 * 60 + dt.datetime.today().minute * 60 + dt.datetime.today().second
    Tiers = dataMunged['Tier'].unique().tolist()
    Climates = dataMunged['Climate'].unique().tolist()
    # dataMunged.set_index(['Climate','Tier'],inplace=True)
    # dataMunged['ddKey'] = dataMunged['Climate'] + "|" + dataMunged['Tier']
    dataMunged['ddKey'] = dataMunged[['Climate', 'Tier']].apply(lambda x: '|'.join(x), axis=1)
    Categories = dataMunged['Category'].unique().tolist()
    # Concat/ Merge Approach
    # masterData = pd.DataFrame(list(product(dataMunged['Store'].unique().tolist(), Categories)), columns=['Store', 'Category'])

    # Append Approach
    masterData = pd.DataFrame(columns=dataMunged.columns)
    masterData['Result Space'] = pd.Series(index=masterData.index)

    # print(masterData)
    masterSummary = pd.DataFrame(list(product(Climates,Tiers)),columns=['Climate','Tier'])
    masterSummary['Status'] = ''
    masterSummary['Objective Value'] = 0
    masterSummary['Optimization Run Time'] = pd.Series(dtype=type(dt.datetime.utcnow()-dt.datetime.utcnow()),index=masterSummary.index)
    masterSummary.set_index(['Climate','Tier'],inplace=True)
    for (loop,key) in enumerate(dataMunged['ddKey'].unique()):
        # logging.info("We are in loop number {}".format(loop+1))
        optimization_start_time = dt.datetime.utcnow()
        climate = key.split('|',1)[0]
        tier = key.rsplit('|', 1)[1]
        # logging.info("Loop for {} and Climate {}".format(tier,climate))
        loopData = dataMunged[(dataMunged.Climate == climate) & (dataMunged.Tier == tier)]
        # loopData = dataMunged.filter(like= str(climate) and str(tier) ,axis=0)
        salesPenetration = loopData.pivot(index='Store', columns='Category', values='Sales Penetration')
        opt_amt= loopData.pivot(index='Store', columns='Category', values='Optimal Space')
        Stores = loopData['Store'].unique().tolist()
        Categories = dataMunged['Category'].unique().tolist()
        logging.info("\n\n We are in loop number {} of {} \n Loop for {} and Climate {} \n We have {} store(s) for the {} optimization \n\n".format(loop + 1,len(dataMunged['ddKey'].unique()),tier,climate,len(Stores),key))
        # logging.info('We have {} many stores for the {} optimization'.format(len(Stores),key))
        b = .05
        bI = .05

        locSpaceToFill = loopData.groupby('Store')['New Space'].agg(np.mean)
        def adjustForTwoIncr(row, bound, increment):
            """
            Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
            :param row: Individual row of Total Space Available in Store
            :param bound: Percent Bounding for Balance Back
            :param increment: Increment Size Determined by the User in the UI
            :return: Returns an adjusted vector of percentages by which individual store space should be held
            """
            return max(bound, (2 * increment) / row)
        locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForTwoIncr(row, bI, increment))

        logging.info('creating levels')
        minLevel = loopData['Optimal Space'].min()
        maxLevel = loopData['Optimal Space'].max()
        Levels = list(np.arange(minLevel, maxLevel + increment, increment))
        if 0.0 not in Levels:
            Levels.insert(0, 0.0)
        logging.info("created levels")

        # Adjust location balance back tolerance limit so that it's at least 2 increments

        # Create a Vectors & Arrays of required variables
        # Calculate Total fixtures(TotFixt) per store by summing up the individual fixture counts
        W = opt_amt.sum(axis=1).sum(axis=0)


        ct = LpVariable.dicts('CT', (Categories, Levels), 0, upBound=1, cat='Binary')
        st = LpVariable.dicts('ST', (Stores, Categories, Levels), 0, upBound=1, cat='Binary')
        logging.info('tiers created')

        NewOptim = LpProblem(jobName, LpMinimize)  # Define Optimization Problem/

        logging.info('Brand Exit Done')
        BA = np.zeros((len(Stores), len(Categories), len(Levels)))
        error = np.zeros((len(Stores), len(Categories), len(Levels)))
        for (i, Store) in enumerate(Stores):
            for (j, Category) in enumerate(Categories):
                if salesPenetration[Category].loc[Store] < salesPen:
                    NewOptim += st[Store][Category][0.0] == 1
                for (k, Level) in enumerate(Levels):
                    BA[i][j][k] = opt_amt[Category].iloc[i]
                    error[i][j][k] = np.absolute(BA[i][j][k] - Level)

        NewOptim += lpSum(
            [(st[Store][Category][Level] * error[i][j][k]) for (i, Store) in enumerate(Stores) for (j, Category) in
             enumerate(Categories) for (k, Level) in enumerate(Levels)]), ""
        logging.info('created objective function')
        ###############################################################################################################
        ############################################### Constraints
        ###############################################################################################################
        # Makes is to that there is only one Selected tier for each Store/ Category Combination
        for (i, Store) in enumerate(Stores):
            # TODO: Exploratory analysis on impact of balance back on financials for Enhanced
            # Store-level balance back constraint: the total space allocated to products at each location must be within the individual location balance back tolerance limit
            NewOptim += lpSum(
                [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
                 enumerate(Levels)]) >= locSpaceToFill[Store] * (1 - locBalBackBoundAdj[Store])  # , "Location Balance Back Lower Limit - STR " + str(Store)
            NewOptim += lpSum(
                [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
                 enumerate(Levels)]) <= locSpaceToFill[Store] * (1 + locBalBackBoundAdj[Store])  # , "Location Balance Back Upper Limit - STR " + str(Store)
        # for (i, Store) in enumerate(Stores):
        #     # Conditional for Balance Back regarding if in Fixtures || 2 Increment Min & Max instead
        #     if TFC[Store] * bI > increment * 2:
        #         NewOptim += lpSum(
        #             [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
        #              enumerate(Levels)]) <= TFC[Store] * (1 + bI)  # , "Upper Bound for Fixtures per Store"
        #         NewOptim += lpSum(
        #             [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
        #              enumerate(Levels)]) >= TFC[Store] * (1 - bI)  # , "Lower Bound for Fixtures per Store"
        #     else:
        #         NewOptim += lpSum(
        #             [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
        #              enumerate(Levels)]) <= TFC[Store] + (increment * 2)  # , "Upper Bound for Fixtures per Store"
        #         NewOptim += lpSum(
        #             [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
        #              enumerate(Levels)]) >= TFC[Store] - (increment * 2)  # , "Lower Bound for Fixtures per Store"
        #     # One Space per Store Category
            # Makes sure that the number of fixtures, by store, does not go above or below some percentage of the total number of fixtures within the store
            for (j, Category) in enumerate(Categories):
                NewOptim += lpSum([st[Store][Category][Level] for (k, Level) in
                                   enumerate(Levels)]) == 1  # , "One_Level_per_Store-Category_Combination"
        logging.info('finished first block of constraints')

        for (j, Category) in enumerate(Categories):
            NewOptim += lpSum([ct[Category][Level] for (k, Level) in enumerate(Levels)]) == 1
            # Relationship between Selected Tiers & Created Tiers
            # Verify that we still cannot use a constraint if not using a sum - Look to improve efficiency
            for (k, Level) in enumerate(Levels):
                NewOptim += lpSum([st[Store][Category][Level] for (i, Store) in enumerate(Stores)]) / len(Stores) <= \
                            ct[Category][Level]  # , "Relationship between ct & st"
        logging.info('finished the second block of constraints')

        # Global Balance Back
        NewOptim += lpSum(
            [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in
             enumerate(Categories) for
             (k, Level) in enumerate(Levels)]) >= W * (1 - b)
        NewOptim += lpSum(
            [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in
             enumerate(Categories) for
             (k, Level) in enumerate(Levels)]) <= W * (1 + b)
        # NewOptim.writeLP("Fixture_Optimization.lp")
        # LpSolverDefault.msg = 1
        logging.info("The problem has been formulated")

        # Solving the Problem
        # NewOptim.writeLP("Fixture_Optimization.lp")
        # NewOptim.writeMPS(str(jobName)+".mps")
        # NewOptim.solve(pulp.GUROBI(mip=True, msg=True, MIPgap=.01))
        if mipGap < 1 or mipGap == None:
            mipGap=99
        try:
            NewOptim.solve(pulp.GUROBI(mip=True, msg=True, MIPgap=mipGap, LogFile="/tmp/gurobi.log"))

        except Exception as e:
            logging.info(e)

        # #Debugging
        logging.info("#####################################################################")
        logging.info(LpStatus[NewOptim.status])
        logging.info("#####################################################################")
        # # Debugging
        # NegativeCount = 0
        # LowCount = 0
        # TrueCount = 0
        # OneCount = 0
        # for (i, Store) in enumerate(Stores):
        #     for (j, Category) in enumerate(Categories):
        #         for (k, Level) in enumerate(Levels):
        #             if value(st[Store][Category][Level]) == 1:
        #                 # logging.info(st[Store][Category][Level]) #These values should only be a one or a zero
        #                 OneCount += 1
        #             elif value(st[Store][Category][Level]) > 0:
        #                 # logging.info(st[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
        #                 TrueCount += 1
        #             elif value(st[Store][Category][Level]) == 0:
        #                 # logging.info(value(st[Store][Category][Level])) #These values should only be a one or a zero
        #                 LowCount += 1
        #             elif value(st[Store][Category][Level]) < 0:
        #                 # logging.info(st[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
        #                 NegativeCount += 1
        # if tierCounts is not None:
        #     ctNegativeCount = 0
        #     ctLowCount = 0
        #     ctTrueCount = 0
        #     ctOneCount = 0
        #
        #     for (j, Category) in enumerate(Categories):
        #         for (k, Level) in enumerate(Levels):
        #             if value(ct[Category][Level]) == 1:
        #                 # logging.info(value(ct[Store][Category][Level])) #These values should only be a one or a zero
        #                 ctOneCount += 1
        #             elif value(ct[Category][Level]) > 0:
        #                 # logging.info(ct[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
        #                 ctTrueCount += 1
        #             elif value(ct[Category][Level]) == 0:
        #                 # logging.info(value(ct[Category][Level])) #These values should only be a one or a zero
        #                 ctLowCount += 1
        #             elif value(ct[Category][Level]) < 0:
        #                 # logging.info(ct[Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
        #                 ctNegativeCount += 1
        #
        # logging.info("Status:", LpStatus[NewOptim.status])
        # logging.info("---------------------------------------------------")
        # logging.info("For Selected Tiers")
        # logging.info("Number of Negatives Count is: ", NegativeCount)
        # logging.info("Number of Zeroes Count is: ", LowCount)
        # logging.info("Number Above 0 and Below 1 Count is: ", TrueCount)
        # logging.info("Number of Selected Tiers: ", OneCount)
        # logging.info("---------------------------------------------------")
        # if tierCounts is not None:
        #     logging.info("For Created Tiers")
        #     logging.info("Number of Negatives Count is: ", ctNegativeCount)
        #     logging.info("Number of Zeroes Count is: ", ctLowCount)
        #     logging.info("Number Above 0 and Below 1 Count is: ", ctTrueCount)
        #     logging.info("Number of Created Tiers: ", ctOneCount)
        #     logging.info("Creating Outputs")
        optimization_end_time = dt.datetime.utcnow()
        if LpStatus[NewOptim.status] == 'Optimal':
            logging.info('Found an optimal solution')
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
            # Either Append this
            loopData = pd.merge(loopData, Results, on=['Store', 'Category'])
            masterData = masterData.append(loopData, ignore_index=True)

            #Or Concatenate this
            # dataMunged = pd.concat([dataMunged,Results],axis=1)
            # masterData= dataMunged.copy()

            # masterData = pd.merge(loopData,Results,on=['Store','Category'])
            print('the columns of masterData are the following \n {}'.format(masterData.columns))
            print('there are {} unique values for result space in masterData'.format(len(masterData['Result Space'].unique())))
            print('masterData is now {} rows'.format(len(masterData)))
            masterSummary['Status'].loc[climate, tier] = LpStatus[NewOptim.status]
            masterSummary['Objective Value'].loc[climate, tier] = value(NewOptim.objective)
            runTime=optimization_end_time - optimization_start_time
            masterSummary['Optimization Run Time'].loc[climate, tier] = runTime
        else:
            masterSummary['Status'].loc[climate, tier] = LpStatus[NewOptim.status]
            masterSummary['Objective Value'].loc[climate, tier] = 0
    return (masterData,masterSummary) # (longOutput)#,wideOutput)

# if __name__ == '__main__':
#     df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
#     create_output_artifact_from_dataframe(df, filename='hello.csv')
