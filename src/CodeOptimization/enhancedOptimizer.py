#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
from pulp import *
import numpy as np
import pandas as pd
import socket
import os
import json
import pymongo as pm
import gridfs
import config
import datetime as dt
from codeoptimization.baseOptimizer import BaseOptimizer
import logging

class EnhancedOptimizer(BaseOptimizer):
    """
    Created on Wed Jan 4 16:00:51 2017

    @author: omkar.marathe

    This is the class for 
    Enhanced Optimization
    """

    def __init__(self,methodology,jobType,jobName,Stores,Categories,increment,weights,cfbsOutput,preOpt,salesPen,tierCounts=None,threadCount=None,fractGap=None):
        super(EnhancedOptimizer,self).__init__(jobName,Stores,Categories,salesPen,tierCounts=None)
        self.methodology = methodology
        self.jobType = jobType
        self.increment = increment
        self.weights = weights 
        self.cfbsOutput = cfbsOutput    
        self.preOpt = preOpt
        self.threadCount = threadCount
        self.fractGap = fractGap

    # Run tiered optimization algorithm
    def optimize(self):
    """

    :param methodology: Enhanced or a Traditional Optimization
    :param jobType: Tiered, Unconstrained, or Drill Down Optimization
    :param jobName: Name of the Job entered by the user
    :param Stores: Vector of all stores within the transactions data set
    :param Categories: Vector of all categories within the transactions data set
    :param increment: The increment size to which everything should be rounded, defined by the user in the UI
    :param weights: The weights by which the different transaction metrics are combined to the objective function
    :param cfbsOutput: Output with Elasticity Curve related information
    :param preOpt: Output with intial data set inputs
    :param salesPen: Sales Penetration threshold required to pass for a store-category combination to tbe considered for optimization
    :param tierCounts: The upper and lower limits for the number of tiers for each product/ category, defined by the user
    :param threadCount: The number of threads to be used by the solver for the optimization
    :param fractGap: The optimiality gap to be used by the solver for the optimization
    :return: Optimization Status, DataFrame of all information, objective function value
    """
    logging.debug('In the Enhanced optimization')
    # Helper function for optimize function, to create eligible space levels
    cfbsOutput.reset_index(inplace=True)
    cfbsOutput.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
    mergedPreOptCF = pd.merge(cfbsOutput, preOpt[['Store', 'Category', 'VSG', 'Store Space', 'Penetration','Exit Flag','Sales Penetration','BOH $', 'Receipts  $','BOH Units', 'Receipts Units', 'Profit %','CC Count w/ BOH',]],on=['Store', 'Category'])
    logging.info('Finished merge')
    mergedPreOptCF = mergedPreOptCF.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    logging.info('Set the index')
    mergedPreOptCF.set_index(['Store','Category'],inplace=True)

    logging.info('merged the files in the new optimization')

    if jobType == 'tiered' or 'unconstrained':
        def createLevels(mergedPreOptCF, increment):
            """
            Creates all possible levels given the global maximum & global minimum for space with the increment size
            :param mergedPreOptCF: Contains all of the merged data from previous scripts
            :param increment: Determined by the user around
            :return: Returns a vector of all possible levels
            """

            minLevel = mergedPreOptCF.loc[:, 'Lower_Limit'].min()
            maxLevel = mergedPreOptCF.loc[:, 'Upper_Limit'].max()
            Levels = list(np.arange(minLevel, maxLevel + increment, increment))
            if 0.0 not in Levels:
                Levels.append(np.abs(0.0))
            
            return Levels

        # Helper function for createSPUByLevel function, to forecast weighted combination of sales, profit, and units
        # str_cat is the row of the curve-fitting output for an individual store and category
        # variable can take on the values of "Sales", "Profit", or "Units"
        def forecast(str_cat, space, variable):
            """
            Forecasts estimated Sales, Profit, or Sales Units
            :param str_cat:
            :param space:
            :param variable:
            :return:
            """
            if space < str_cat["Scaled_BP_" + variable]:
                value = space * (str_cat["Scaled_Alpha_" + variable] * (math.erf(
                    (str_cat["Scaled_BP_" + variable] - str_cat["Scaled_Shift_" + variable]) / ((
                    math.sqrt(2) * str_cat["Scaled_Beta_" + variable])))) / str_cat["Scaled_BP_" + variable])
            else:
                value = str_cat["Scaled_Alpha_" + variable] * math.erf(
                    (space - str_cat["Scaled_Shift_" + variable]) / (math.sqrt(2) * str_cat["Scaled_Beta_" + variable]))

            return round(value,2)

        # Helper function for optimize function, to create objective function of SPU by level for Enhanced optimizations
        def createNegSPUByLevel(Stores, Categories, Levels, curveFittingOutput, enhMetrics):
            """
            Creates the objective for enhanced optimizations in which the goal to minimize the weighted combination of sales, profits, and units multiplied by -1
            :param Stores:
            :param Categories:
            :param Levels:
            :param curveFittingOutput:
            :param enhMetrics:
            :return:
            """
            # Create n-dimensional array to store Estimated SPU by level
            est_neg_spu_by_lev = np.zeros((len(Stores), len(Categories), len(Levels)))

            sU = "Sales"
            pU = "Profit"
            uU = "Units"
            sL = "sales"
            pL = "profits"
            uL = "units"

            logging.debug('Forecasting outputs')
            # Calculate SPU by level
            for (i, Store) in enumerate(Stores):
                for (j, Category) in enumerate(Categories):
                    for (k, Level) in enumerate(Levels):
                        str_cat = curveFittingOutput.loc[Store, Category]
                        est_neg_spu_by_lev[i][j][k] = - (
                        (enhMetrics[sL] / 100) * forecast(str_cat, Level, sU) + (enhMetrics[pL] / 100) * forecast(str_cat,
                                                                                                                  Level,
                                                                                                                  pU) + (
                        enhMetrics[uL] / 100) * forecast(str_cat, Level, uU))
            logging.info('finished forecasting')
            return est_neg_spu_by_lev

        # Helper function for optimize function, to create objective function of error by level for Traditional optimizations
        def createErrorByLevel(Stores, Categories, Levels, mergedCurveFitting):
            """
            Creates the objective for traditional optimizations in which the goal to minimize the distance between the optimal & result space
            :param Stores: Vector of Stores within the transactions data
            :param Categories: Vector of Categories within the transactions data
            :param Levels:
            :param mergedCurveFitting:
            :return:
            """
            # Create n-dimensional array to store error by level
            error = np.zeros((len(Stores), len(Categories), len(Levels)))

            # Calculate error by level
            for (i, Store) in enumerate(Stores):
                for (j, Category) in enumerate(Categories):
                    for (k, Level) in enumerate(Levels):
                        error[i][j][k] = np.absolute(mergedCurveFitting.loc[Store, Category]["Optimal Space"] - Level)
            return error

        # Adjust location balance back tolerance limit so that it's at least 2 increments
        def adjustForOneIncr(row, bound, increment):
            """
            Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
            :param row: Individual row of Total Space Available in Store
            :param bound: Percent Bounding for Balance Back
            :param increment: Increment Size Determined by the User in the UI
            :return: Returns an adjusted vector of percentages by which individual store space should be held
            """
            return max(bound, increment / row)

        # Adjust location balance back tolerance limit so that it's at least 2 increments
        def adjustForTwoIncr(row,bound,increment):
            """
            Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
            :param row: Individual row of Total Space Available in Store
            :param bound: Percent Bounding for Balance Back
            :param increment: Increment Size Determined by the User in the UI
            :return: Returns an adjusted vector of percentages by which individual store space should be held
            """
            return max(bound,(2*increment)/row)

        logging.info('completed all of the function definitions')
        # Identify the total amount of space to fill in the optimization for each location and for all locations
        
        locSpaceToFill = mergedPreOptCF.groupby(level=0)['Space_to_Fill'].agg(np.mean)

        aggSpaceToFill = locSpaceToFill.sum()

        # Hard-coded tolerance limits for balance back constraints
        aggBalBackBound = 0.05 # 5%
        locBalBackBound = 0.05 # 10%

        logging.info('now have balance back bounds')
        incrAdj = super(EnhancedOptimizer,self).searchParam('ADJ', jobName)
        if incrAdj == None:
            locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForOneIncr(row, bI, increment))
        else:
            locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForTwoIncr(row, bI, increment))

        logging.info('local balance back is found')
     
        # Create eligible space levels
        mergedPreOptCF["Upper_Limit"] = mergedPreOptCF["Upper_Limit"].apply(lambda x: super(EnhancedOptimizer,self).roundValue(x,increment))
        mergedPreOptCF["Lower_Limit"] = mergedPreOptCF["Lower_Limit"].apply(lambda x: super(EnhancedOptimizer,self).roundValue(x,increment))
        Levels = createLevels(mergedPreOptCF, increment)
        logging.info('levels are obtained')
        # Set up created tier decision variable - has a value of 1 if that space level for that category will be a tier, else 0
        
        ct = LpVariable.dicts('CT', (Categories, Levels), 0, upBound=1, cat='Binary')
        logging.info('tiers are created')
        # Set up selected tier decision variable - has a value of 1 if a store will be assigned to the tier at that space level for that category, else 0
        st = LpVariable.dicts('ST', (Stores, Categories, Levels), 0, upBound=1, cat='Binary')
        logging.info('tiers are selected')
        
        # Initialize the optimization problem
        NewOptim = LpProblem(jobName, LpMinimize)
        logging.info('problem is initialized')

        # Create objective function data
        if methodology == "traditional":
            objective = createErrorByLevel(Stores, Categories,Levels,mergedPreOptCF)
            objectivetype = "Total Error"
        else: #since methodology == "enhanced"
            objective = createNegSPUByLevel(Stores, Categories, Levels, mergedPreOptCF, weights)            
            objectivetype = "Total Negative SPU"

        
        logging.info('objective function data is created')
        # Add the objective function to the optimization problem
        NewOptim += lpSum(
            [(st[Store][Category][Level] * objective[i][j][k]) for (i, Store) in enumerate(Stores) for (j, Category)
             in enumerate(Categories) for (k, Level) in enumerate(Levels)])#, objectivetype
        logging.info('objective function is created')
        # Begin CONSTRAINT SETUP

        for (i,Store) in enumerate(Stores):
            # TODO: Exploratory analysis on impact of balance back on financials for Enhanced
            # Store-level balance back constraint: the total space allocated to products at each location must be within the individual location balance back tolerance limit
            NewOptim += lpSum([(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
                               enumerate(Levels)]) >= locSpaceToFill[Store] * (1 - locBalBackBoundAdj[Store])
            NewOptim += lpSum([(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
                               enumerate(Levels)]) <= locSpaceToFill[Store] * (1 + locBalBackBoundAdj[Store])
            for (j,Category) in enumerate(Categories):
                
                # Only one selected tier can be turned on for each product at each location.
                NewOptim += lpSum([st[Store][Category][Level] for (k,Level) in enumerate(Levels)]) == 1

                # The space allocated to each product at each location must be between the minimum and the maximum allowed for that product at the location.
                NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)] ) >= mergedPreOptCF["Lower_Limit"].loc[Store,Category],"Space Lower Limit - STR " + str(Store) + ", CAT " + str(Category)
                NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)] ) <= mergedPreOptCF["Upper_Limit"].loc[Store,Category],"Space Upper Limit - STR " + str(Store) + ", CAT " + str(Category)
                if mergedPreOptCF['Sales Penetration'].loc[Store,Category] < salesPen:
                    NewOptim += st[Store][Category][0] == 1

        logging.info('finished first block of constraints')        
        try:
            if jobType == 'tiered':
                logging.info('jobType is tiered')
                for (j,Category) in enumerate(Categories):                    
                    # The number of created tiers must be within the tier count limits for each product.
                    NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) >= tierCounts[Category][0], "Tier Count Lower Limit - CAT " + str(Category)
                    NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) <= tierCounts[Category][1], "Tier Count Upper Limit - CAT " + str(Category)
            for (j,Category) in enumerate(Categories):
                for (k,Level) in enumerate(Levels):
                    # A selected tier can be turned on if and only if the created tier at that level for that product is turned on.
                    NewOptim += lpSum([st[Store][Category][Level] for (i,Store) in enumerate(Stores)])/len(Stores) <= ct[Category][Level], "Selected-Created Tier Relationship - CAT " + str(Category) + ", LEV: " + str(Level)
                    
            logging.info('finished second block of constraints')
        except Exception as e:
            logging.exception('Exception in processing second block of constraints')

        
        # The total space allocated to products across all locations must be within the aggregate balance back tolerance limit.
        NewOptim += lpSum([st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for (k, Level) in enumerate(Levels)]) >= aggSpaceToFill * (1 - aggBalBackBound), "Aggregate Balance Back Lower Limit"
        NewOptim += lpSum([st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for (k, Level) in enumerate(Levels)]) <= aggSpaceToFill * (1 + aggBalBackBound), "Aggregate Balance Back Upper Limit"

        #Time stamp for optimization solve time
        start_seconds = dt.datetime.today().hour*60*60+ dt.datetime.today().minute*60 + dt.datetime.today().second

        mergedPreOptCF.reset_index(inplace=True)

        # Solve the problem using open source solver
        logging.info('optional hidden parameters')

        if 'PreSolve' in jobName:
            preSolving = True
        else:
            preSolving = False

        #Solve the problem using Gurobi
        NewOptim.solve(pulp.GUROBI(mip=True, msg=True, MIPgap=.01, LogFile="/tmp/gurobi.log"))

        logging.debug('out of the solver')

        # Time stamp for optimization solve time
        solve_end_seconds = dt.datetime.today().hour*60*60 + dt.datetime.today().minute*60 + dt.datetime.today().second
        solve_seconds = solve_end_seconds - start_seconds
        logging.info("Time taken to solve optimization is:" + str(solve_seconds))

        # Debugging
        logging.debug("#####################################################################")
        logging.debug(LpStatus[NewOptim.status])
        logging.debug("#####################################################################")
        if LpStatus[NewOptim.status] == 'Optimal':
            logging.info('an optimal solution is found')
            Results=pd.DataFrame(index=Stores,columns=Categories)
            for (i,Store) in enumerate(Stores):
                for (j,Category) in enumerate(Categories):
                    for (k,Level) in enumerate(Levels):
                        if value(st[Store][Category][Level]) == 1:
                            Results[Category][Store] = Level

            Results.reset_index(inplace=True)
            Results.columns.values[0]='Store'
            Results = pd.melt(Results.reset_index(), id_vars=['Store'], var_name='Category', value_name='Result Space')
            Results=Results.apply(lambda x: pd.to_numeric(x, errors='ignore'))
            mergedPreOptCF.reset_index(inplace=True)
            mergedPreOptCF.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
            mergedPreOptCF=pd.merge(mergedPreOptCF,Results,on=['Store','Category'])
            return (LpStatus[NewOptim.status],mergedPreOptCF,value(NewOptim.objective)*-1)
        else:
            mergedPreOptCF['Result Space']= 0
            mergedPreOptCF.reset_index(inplace=True)
            mergedPreOptCF.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
            return (LpStatus[NewOptim.status], mergedPreOptCF, 0)
    else:
        mergedPreOptCF.reset_index(inplace=True)
        mergedPreOptCF['Result Space'] = mergedPreOptCF['Optimal Space'].apply(lambda x: super(EnhancedOptimizer,self).roundValue(x,increment))
        mergedPreOptCF.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
        return ('Optimal', mergedPreOptCF, mergedPreOptCF['Result Space'].sum())
    
    

    

