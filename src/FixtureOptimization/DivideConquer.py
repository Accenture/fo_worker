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

    def searchParam(search, jobName):
        if search in jobName:
            begin = jobName.find(search)
            length = 0
            for char in jobName[(len(search) + begin)::]:
                try:
                    int(char)
                    length = length + 1
                except:
                    break
            try:
                searchParam = int(jobName[(len(search) + begin):(len(search) + begin + length)]) / 100
                logging.info('{} has been changed to {}'.format(search,searchParam))
                return searchParam
            except:
                return True
        else:
            return None

    dataMunged['Optimal Space'] = dataMunged['Optimal Space'].apply(lambda x: roundValue(x, increment))
    dataMunged = dataMunged.apply(lambda x: pd.to_numeric(x, errors='ignore'))

    start_time = dt.datetime.today().hour * 60 * 60 + dt.datetime.today().minute * 60 + dt.datetime.today().second
    Tiers = dataMunged['Tier'].unique().tolist()
    Climates = dataMunged['Climate'].unique().tolist()
    dataMunged['Drill Down Group'] = dataMunged[['Climate', 'Tier']].apply(lambda x: '|'.join(x), axis=1)
    Categories = dataMunged['Category'].unique().tolist()

    # Append Approach
    masterData = pd.DataFrame(columns=dataMunged.columns)
    masterData['Result Space'] = pd.Series(index=masterData.index)

    masterSummary = pd.DataFrame(list(product(Climates,Tiers)),columns=['Climate','Tier'])
    masterSummary['Status'] = ''
    masterSummary['Objective Value'] = 0
    masterSummary['Optimization Run Time'] = pd.Series(dtype=type(dt.datetime.utcnow()-dt.datetime.utcnow()),index=masterSummary.index)
    masterSummary.set_index(['Climate','Tier'],inplace=True)
    for (loop,key) in enumerate(dataMunged['Drill Down Group'].unique()):
        optimization_start_time = dt.datetime.utcnow()
        climate = key.split('|',1)[0]
        tier = key.rsplit('|', 1)[1]
        loopData = dataMunged[(dataMunged.Climate == climate) & (dataMunged.Tier == tier)]
        salesPenetration = loopData.pivot(index='Store', columns='Category', values='Sales Penetration')
        opt_amt= loopData.pivot(index='Store', columns='Category', values='Optimal Space')
        Stores = loopData['Store'].unique().tolist()
        Categories = dataMunged['Category'].unique().tolist()
        logging.info("\n\n We are in loop number {} of {} \n Loop for {} and Climate {} \n We have {} store(s) for the {} optimization \n\n".format(loop + 1,len(dataMunged['Drill Down Group'].unique()),tier,climate,len(Stores),key))

        b = .05
        bI = searchParam('BBI', jobName)
        if bI == None:
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
        def adjustForOneIncr(row, bound, increment):
            """
            Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
            :param row: Individual row of Total Space Available in Store
            :param bound: Percent Bounding for Balance Back
            :param increment: Increment Size Determined by the User in the UI
            :return: Returns an adjusted vector of percentages by which individual store space should be held
            """
            return max(bound, (increment) / row)

        incrAdj = searchParam('ADJ', jobName)
        if incrAdj == None:
            locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForTwoIncr(row, bI, increment))
        else:
            locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForOneIncr(row, bI, increment))

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
        optGap = searchParam('MIP', jobName)

        if optGap == None:
            optGap = .1
        try:
            NewOptim.solve(pulp.GUROBI(mip=True, msg=True, MIPgap=optGap, LogFile="/tmp/gurobi.log"))

        except Exception as e:
            logging.info(e)

        #Debugging
        logging.info("#####################################################################")
        logging.info(LpStatus[NewOptim.status])
        logging.info("#####################################################################")
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
            logging.info('the columns of masterData are the following \n {}'.format(masterData.columns))
            logging.info('there are {} unique values for result space in masterData'.format(len(masterData['Result Space'].unique())))
            logging.info('masterData is now {} rows'.format(len(masterData)))
            masterSummary['Status'].loc[climate, tier] = LpStatus[NewOptim.status]
            masterSummary['Objective Value'].loc[climate, tier] = value(NewOptim.objective)
            runTime=optimization_end_time - optimization_start_time
            masterSummary['Optimization Run Time'].loc[climate, tier] = runTime
        else:
            masterSummary['Status'].loc[climate, tier] = LpStatus[NewOptim.status]
            masterSummary['Objective Value'].loc[climate, tier] = 0
    return (masterData,masterSummary) # (longOutput)#,wideOutput)
