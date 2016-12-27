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


def optimizeDD(jobName, increment, dataMunged, salesPen):
    """
    Run an LP-based optimization

    Side-effects:
        - creates file: Fixture_Optimization.lp (constraints)
        - creates file: solvedout.csv <= to be inserted into db
        - creates file: solvedout.text

    Synopsis:
        I just wrapped the script from Ken in a callable - DCE
    """
    dataMunged.to_csv('DrillDownMunged.csv',sep=",")
    def roundValue(cVal, increment):
        if np.mod(round(cVal, 3), increment) > increment / 2:
            cVal = round(cVal, 3) + (increment - (np.mod(round(cVal, 3), increment)))
        else:
            cVal = round(cVal, 3) - np.mod(round(cVal, 3), increment)
        return cVal

    Stores = dataMunged['Store'].unique().tolist()
    Categories = dataMunged['Category'].unique().tolist()
    Tiers = dataMunged['Tier'].unique().tolist()
    Climates = dataMunged['Climate'].unique().tolist()

    dataMunged['Optimal Space'] = dataMunged['Optimal Space'].apply(lambda x: roundValue(x, increment))
    logging.info('set up new space bounds')
    dataMunged = dataMunged.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    start_time = dt.datetime.today().hour * 60 * 60 + dt.datetime.today().minute * 60 + dt.datetime.today().second
    dataMunged["Optimal Space"] = dataMunged["Optimal Space"].apply(lambda x: roundValue(x, increment))
    salesPenetration = dataMunged.pivot(index='Store', columns='Category', values='Sales Penetration')
    brandExitArtifact = dataMunged.pivot(index='Store', columns='Category', values='Exit Flag')
    opt_amt= dataMunged.pivot(index='Store', columns='Category', values='Optimal Space')

    b = .05
    bI = .05

    locSpaceToFill = dataMunged.groupby('Store')['New Space'].agg(np.mean)
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

    logging.info("HEY I'M IN THE OPTIMIZATION!!!!!!!")
    ###############################################################################################
    # Reading in of Files & Variable Set Up|| Will be changed upon adoption into tool
    ###############################################################################################

    ##########################################################################################
    ##################Vector Creation ||May be moved to another module/ program in the future
    ##########################################################################################
 
    logging.info('creating levels')
    minLevel = dataMunged['Optimal Space'].min()
    maxLevel = dataMunged['Optimal Space'].max()
    Levels = list(np.arange(minLevel, maxLevel + increment, increment))
    if 0.0 not in Levels:
        Levels.insert(0, 0.0)
    logging.info("created levels")

    # Adjust location balance back tolerance limit so that it's at least 2 increments

    W = opt_amt.sum(axis=1).sum(axis=0)

    ct = LpVariable.dicts('CT', (Tiers, Climates, Categories, Levels), 0, upBound=1, cat='Binary')
    st = LpVariable.dicts('ST', (Stores, Categories, Levels), 0, upBound=1, cat='Binary')
    logging.info('tiers created')

    NewOptim = LpProblem(jobName, LpMinimize)  # Define Optimization Problem/

    logging.info('Brand Exit Done')
    BA = np.zeros((len(Stores), len(Categories), len(Levels)))
    error = np.zeros((len(Stores), len(Categories), len(Levels)))
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
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
        for (j, Category) in enumerate(Categories):
            NewOptim += lpSum([st[Store][Category][Level] for (k, Level) in
                               enumerate(Levels)]) == 1  # , "One_Level_per_Store-Category_Combination"
    logging.info('finished first block of constraints')

    dataMunged['ddKey'] = dataMunged[['Climate', 'Tier']].apply(lambda x: '|'.join(x), axis=1)
    for (loop,key) in enumerate(dataMunged['ddKey'].unique()):
        Climate = key.split('|',1)[0]
        Tier = key.rsplit('|', 1)[1]
        for (j, Category) in enumerate(Categories):
            NewOptim += lpSum([ct[Tier][Climate][Category][Level] for (k, Level) in enumerate(Levels)]) == 1
            # Relationship between Selected Tiers & Created Tiers
            # Verify that we still cannot use a constraint if not using a sum - Look to improve efficiency
            for (k, Level) in enumerate(Levels):
                NewOptim += lpSum([st[Store][Category][Level] for (i, Store) in enumerate(Stores)]) / len(Stores) <= \
                            ct[Tier][Climate][Category][Level]  # , "Relationship between ct & st"
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

    logging.info("The problem has been formulated")


    try:
        NewOptim.solve(pulp.GUROBI(mip=True, msg=True, MIPgap=.2, LogFile="/tmp/gurobi.log"))

    except Exception as e:
        logging.info(e)

    #Debugging
    print("#####################################################################")
    print(LpStatus[NewOptim.status])
    print("#####################################################################")

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
        dataMunged = pd.merge(dataMunged, Results, on=['Store', 'Category'])
        # return (LpStatus[NewOptim.status], dataMunged, value(NewOptim.objective))  # (longOutput)#,wideOutput)
        return (dataMunged, LpStatus[NewOptim.status], value(NewOptim.objective))  # (longOutput)#,wideOutput)
    else:
        dataMunged['Result Space'] = 0
        return (LpStatus[NewOptim.status], dataMunged, 0)
