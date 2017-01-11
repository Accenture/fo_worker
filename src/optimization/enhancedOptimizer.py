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
from optimization.baseOptimizer import BaseOptimizer
import logging

class EnhancedOptimizer(BaseOptimizer):
    """
    Created on Wed Jan 4 16:00:51 2017

    @author: omkar.marathe

    This is the class for 
    Enhanced Optimization
    """
    def __init__(self,methodology,job_name,job_type,stores,categories,increment,weights,cfbsOutput,preopt,salesPen,tierCounts=None,thread_count=None,fract_gap=None):
        super(EnhancedOptimizer,self).__init__(job_name,job_type,stores,categories,salesPen,tierCounts=None)
        self.methodology = methodology
        tself.job_Type = job_type
        self.increment = increment
        self.weights = weights 
        self.cfbs_output = cfbs_output    
        self.preopt = preopt
        self.thread_count = thread_count
        self.fract_gap = fract_gap
        self.merged_preoptCF = None
        self.solver = CbcSolver("CBC Solver")

    """
    Creates all possible levels given the global maximum & global minimum for space with the increment size
    :param merged_preoptCF: Contains all of the merged data from previous scripts
    :param increment: Determined by the user around
    :return: Returns a vector of all possible levels
    """
    def create_space_levels(self):     
         # Create eligible space levels
        self.merged_preoptCF["Upper_Limit"] = self.merged_preoptCF["Upper_Limit"].apply(lambda x: super(EnhancedOptimizer,self).roundValue(x,self.increment))
        self.merged_preoptCF["Lower_Limit"] = self.merged_preoptCF["Lower_Limit"].apply(lambda x: super(EnhancedOptimizer,self).roundValue(x,self.increment))
        minLevel = merged_preoptCF.loc[:, 'Lower_Limit'].min()
        maxLevel = merged_preoptCF.loc[:, 'Upper_Limit'].max()
        self.levels = list(np.arange(minLevel, maxLevel + increment, increment))
        if 0.0 not in levels:
            self.levels.append(np.abs(0.0))
        
        #return levels

    # Helper function for createSPUByLevel function, to forecast weighted combination of sales, profit, and units
    # str_cat is the row of the curve-fitting output for an individual store and category
    # variable can take on the values of "Sales", "Profit", or "Units"
    """
    Forecasts estimated Sales, Profit, or Sales Units
    :param str_cat:
    :param space:
    :param variable:
    :return:
    """
    def forecast(self,str_cat, space, variable):
     
        if space < str_cat["Scaled_BP_" + variable]:
            self.value = space * (str_cat["Scaled_Alpha_" + variable] * (math.erf(
                (str_cat["Scaled_BP_" + variable] - str_cat["Scaled_Shift_" + variable]) / ((
                math.sqrt(2) * str_cat["Scaled_Beta_" + variable])))) / str_cat["Scaled_BP_" + variable])
        else:
            self.value = str_cat["Scaled_Alpha_" + variable] * math.erf(
                (space - str_cat["Scaled_Shift_" + variable]) / (math.sqrt(2) * str_cat["Scaled_Beta_" + variable]))

        return round(self.value,2)

    # Helper function for optimize function, to create objective function of SPU by level for Enhanced optimizations
    """
    Creates the objective for enhanced optimizations in which the goal to minimize the weighted combination of sales, profits, and units multiplied by -1
    :param Stores:
    :param Categories:
    :param Levels:
    :param curveFittingOutput:
    :param enh_metrics:
    :return:
    """
    def create_negSPUbylevel(self, stores, categories, levels, curveFittingOutput, enh_metrics):
   
        # Create n-dimensional array to store Estimated SPU by level
        est_neg_spu_by_lev = np.zeros((len(stores), len(categories), len(levels)))

        sU = "Sales"
        pU = "Profit"
        uU = "Units"
        sL = "sales"
        pL = "profits"
        uL = "units"

        logging.debug('Forecasting outputs')
        # Calculate SPU by level
        for (i, Store) in enumerate(stores):
            for (j, Category) in enumerate(categories):
                for (k, Level) in enumerate(levels):
                    str_cat = curveFittingOutput.loc[stores, Category]
                    est_neg_spu_by_lev[i][j][k] = - (
                    (enh_metrics[sL] / 100) * self.forecast(str_cat, Level, sU) + (enh_metrics[pL] / 100) * forecast(str_cat,
                                                                                                              Level,
                                                                                                              pU) + (
                    enh_metrics[uL] / 100) * self.forecast(str_cat, Level, uU))
        logging.info('finished forecasting')
        return est_neg_spu_by_lev

    # Helper function for optimize function, to create objective function of error by level for Traditional optimizations
    """
    Creates the objective for traditional optimizations in which the goal to minimize the distance between the optimal & result space
    :param Stores: Vector of Stores within the transactions data
    :param Categories: Vector of Categories within the transactions data
    :param Levels:
    :param mergedCurveFitting:
    :return:
    """
    def create_errorbylevel(self, stores, categories, levels, mergedCurveFitting):     
        # Create n-dimensional array to store error by level
        error = np.zeros((len(stores), len(categories), len(levels)))

        # Calculate error by level
        for (i, Store) in enumerate(stores):
            for (j, Category) in enumerate(categories):
                for (k, Level) in enumerate(levels):
                    error[i][j][k] = np.absolute(mergedCurveFitting.loc[Store, Category]["Optimal Space"] - Level)
        return error

    """
    Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
    :param row: Individual row of Total Space Available in Store
    :param bound: Percent Bounding for Balance Back
    :param increment: Increment Size Determined by the User in the UI
    :return: Returns an adjusted vector of percentages by which individual store space should be held
        """
    # Adjust location balance back tolerance limit so that it's at least 2 increments
    def adjust_foroneIncr(self, row, bound, increment):
  
        return max(bound, increment / row)

    """
    Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
    :param row: Individual row of Total Space Available in Store
    :param bound: Percent Bounding for Balance Back
    :param increment: Increment Size Determined by the User in the UI
    :return: Returns an adjusted vector of percentages by which individual store space should be held
    """
    # Adjust location balance back tolerance limit so that it's at least 2 increments
    
    def adjust_fortwoIncr(row,bound,increment):    
    
        return max(bound,(2*increment)/row)

    # Helper function for optimize function, to create eligible space levels
    def eligible_space_levels(self):        
        self.cfbs_output.reset_index(inplace=True)
        self.cfbs_output.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
        self.merged_preoptCF = pd.merge(self.cfbs_output, self.preopt[['Store', 'Category', 'VSG', 'Store Space', 'Penetration','Exit Flag','Sales Penetration','BOH $', 'Receipts  $','BOH Units', 'Receipts Units', 'Profit %','CC Count w/ BOH',]],on=['Store', 'Category'])
        logging.info('Finished merge')
        self.merged_preoptCF = self.merged_preoptCF.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        logging.info('Set the index')
        self.merged_preoptCF.set_index(['Store','Category'],inplace=True)
        
        
    
    """     
     Run tiered optimization algorithm   
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
    # Create objective function data
    def add_objective(self,st):         
        if self.methodology == "traditional":
            objective = self.create_errorbylevel(self.stores, self.categories,self.levels,self.merged_preoptCF)
            objectivetype = "Total Error"
        else: #since methodology == "enhanced"
            objective = self.create_negSPUbylevel(self.stores, self.categories, self.levels, self.merged_preoptCF, self.weights)            
            objectivetype = "Total Negative SPU"    
        
        logging.info('objective function data is created')
        # Add the objective function to the optimization problem
        self.solver.add_objective([(st[store][category][level] * objective[i][j][k]) for (i, store) in enumerate(self.stores) 
                                   for (j, category) in enumerate(self.categories) for (k, level) in enumerate(self.levels)])
    def balance_back(self):
        incradj = super(EnhancedOptimizer,self).search_param('ADJ', job_name)
        if incradj == None:
            self.locbalback_boundadj = locspacetofill.apply(lambda row: self.adjust_foroneIncr(row, bI, self.increment))
        else:
            self.locbalback_boundadj = locspacetofill.apply(lambda row: self.adjust_fortwoIncr(row, bI, self.increment))
        
    def add_constraints_forspaclevelstorecategory(self):
        for (i,Store) in enumerate(self.stores):
            # TODO: Exploratory analysis on impact of balance back on financials for Enhanced
            # Store-level balance back constraint: the total space allocated to products at each location must be within the individual location balance back tolerance limit
            self.solver.add_constraint([(st[Store][Category][Level]) * Level for (j, Category) in enumerate(self.categories)\
                                                   for (k, Level) in enumerate(self.levels)],\
                                                   'gte', locspacetofill[Store] * (1 - self.locbalback_boundadj[Store]))
            
            self.solver.add_constraint([(st[Store][Category][Level]) * Level for (j, Category) in enumerate(self.categories)\
                                                      for (k, Level) in enumerate(self.levels)],\
                                                      'lte', locspacetofill[Store] * (1 + self.locbalback_boundadj[Store]))
            
            for (j,Category) in enumerate(self.categories):
                
                # Only one selected tier can be turned on for each product at each location.
                self.solver.add_constraint([st[Store][Category][Level] for (k,Level) in enumerate(self.levels)],'eq', 1)
    
                # The space allocated to each product at each location must be between the minimum and the maximum allowed for that product at the location.
                self.solver.add_constraint([st[Store][Category][Level] * Level for (k,Level) in enumerate(self.levels)],'gte',\
                                                           self.merged_preoptCF["Lower_Limit"].loc[Store,Category],\
                                                           "Space Lower Limit - STR " + str(Store) + ", CAT " + str(Category))
                self.solver.add_constraint([st[Store][Category][Level] * Level for (k,Level) in enumerate(self.levels)] ),'lte',\
                                                           self.merged_preoptCF["Upper_Limit"].loc[Store,Category],\
                                                           "Space Upper Limit - STR " + str(Store) + ", CAT " + str(Category)
                if self.merged_preoptCF['Sales Penetration'].loc[Store,Category] < salesPen:
                    self.solver.problem += st[Store][Category][0] == 1
        
    def add_constraintsfortiered(self):
       try:
        if self.job_type == 'tiered':
            logging.info('jobType is tiered')
            for (j,Category) in enumerate(self.categories):                    
                # The number of created tiers must be within the tier count limits for each product.
                self.solver.add_constraint([ct[Category][Level] for (k,Level) in enumerate(self.levels)],\
                                 'gte',tierCounts[Category][0],\
                                 "Tier Count Lower Limit - CAT " + str(Category))
                self.solver.add_constraint([ct[Category][Level] for (k,Level) in enumerate(self.levels)],
                                'lte',tierCounts[Category][1],\
                                 "Tier Count Upper Limit - CAT " + str(Category))
        for (j,Category) in enumerate(self.categories):
            for (k,Level) in enumerate(self.levels):
                # A selected tier can be turned on if and only if the created tier at that level for that product is turned on.
                self.solver.add_constraint([st[Store][Category][Level] for (i,Store) in enumerate(self.stores)]/len(self.stores),\
                                'lte',ct[Category][Level],\
                                "Selected-Created Tier Relationship - CAT " + str(Category) + ", LEV: " + str(Level))
                
        logging.info('finished second block of constraints')
       except Exception as e:
            logging.exception('Exception in processing second block of constraints')
        
    def add_constraints_aggregatebalanceback(self):
        # The total space allocated to products across all locations must be within the aggregate balance back tolerance limit.
        self.solver.add_constraint([st[Store][Category][Level] * Level for (i, Store) in enumerate(self.stores) \
                             for (j, Category) in enumerate(self.categories) \
                             for (k, Level) in enumerate(self.levels)],\
                             'gte',agg_space_to_till * (1 - agg_balback_bound),\
                              "Aggregate Balance Back Lower Limit")
        self.solver.add_constraint([st[Store][Category][Level] * Level for (i, Store) in enumerate(self.stores)\
                              for (j, Category) in enumerate(self.categories)\
                              for (k, Level) in enumerate(self.levels)],\
                              'lte',agg_space_to_till * (1 + agg_balback_bound),\
                              "Aggregate Balance Back Upper Limit")
        
    
    # Debugging
    def debugging(self):        
        logging.debug("#####################################################################")
        #logging.debug(LpStatus[self.problem.status])
        solver_status = self.solver.getStatus()
        logging.debug(solver_status)
        logging.debug("#####################################################################")
        if solver_status == 'Optimal':
            logging.info('an optimal solution is found')
            results=pd.DataFrame(index=self.stores,columns=self.categories)
            for (i,Store) in enumerate(self.stores):
                for (j,Category) in enumerate(self.categories):
                    for (k,Level) in enumerate(self.levels):
                        if self.value(st[Store][Category][Level]) == 1:
                            results[Category][Store] = Level

            results.reset_index(inplace=True)
            results.columns.values[0]='Store'
            results = pd.melt(results.reset_index(), id_vars=['Store'], var_name='Category', value_name='Result Space')
            results=results.apply(lambda x: pd.to_numeric(x, errors='ignore'))
            self.merged_preoptCF.reset_index(inplace=True)
            self.merged_preoptCF.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
            self.merged_preoptCF=pd.merge(self.merged_preoptCF,results,on=['Store','Category'])
            return (LpStatus[self.problem.status],self.merged_preoptCF,self.value(self.problem.objective)*-1)
        else:
            self.merged_preoptCF['Result Space']= 0
            self.merged_preoptCF.reset_index(inplace=True)
            self.merged_preoptCF.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
            return (LpStatus[self.problem.status], self.merged_preoptCF, 0)
        
    def optimize(self):
        logging.debug('In the Enhanced optimization')    
        self.eligible_space_levels()
    
        logging.info('merged the files in the new optimization')
    
        if self.job_type == 'tiered' or 'unconstrained':          
    
            logging.info('completed all of the function definitions')
            # Identify the total amount of space to fill in the optimization for each location and for all locations
            
            locspacetofill = self.merged_preoptCF.groupby(level=0)['Space_to_Fill'].agg(np.mean)
    
            agg_space_to_till = locspacetofill.sum()
    
            # Hard-coded tolerance limits for balance back constraints
            agg_balback_bound = 0.05 # 5%
            loc_balback_Bound = 0.05 # 10%
    
            logging.info('now have balance back bounds')
            self.balance_back()    
            logging.info('local balance back is found')
         
            self.create_space_levels()         
            logging.info('levels are obtained')
            # Set up created tier decision variable - has a value of 1 if that space level for that category will be a tier, else 0
            
            #ct = LpVariable.dicts('CT', (self.categories, levels), 0, upBound=1, cat='Binary')
            ct = solver.create_variables('CT', self.categories, self.levels, 0)
            logging.info('tiers are created')
            # Set up selected tier decision variable - has a value of 1 if a store will be assigned to the tier at that space level for that category, else 0
            #st = LpVariable.dicts('ST', (self.stores, self.categories, levels), 0, upBound=1, cat='Binary')
            st = solver.add_variables('ST', self.stores, self.categories, self.levels, 0)
            logging.info('tiers are selected')
            
            # Initialize the optimization problem
            self.solver.create_problem(self.job_name, 'MIN')
            logging.info('problem is initialized')
    
            logging.info('adding objectives')
            self.add_objecitve(st)
            #self.problem += lpSum(
            # [(st[store][category][level] * objective[i][j][k]) for (i, store) in enumerate(self.stores) for (j, category)
            #     in enumerate(self.categories) for (k, level) in enumerate(levels)])#, objectivetype
            logging.info('objective function is created')
            
            logging.info('Begin adding constraint')
    
            self.add_constraints_forspaclevelstorecategory() 
    
            logging.info('finished first block of constraints') 
            self.add_constraintsfortiered()     
    
            
            self.add_constraints_aggregatebalanceback()
                   
            #Time stamp for optimization solve time
            start_seconds = dt.datetime.today().hour*60*60+ dt.datetime.today().minute*60 + dt.datetime.today().second
    
            self.merged_preoptCF.reset_index(inplace=True)
    
            # Solve the problem using open source solver
            logging.info('optional hidden parameters')
    
            if 'PreSolve' in job_name:
                preSolving = True
            else:
                preSolving = False    
            #Solve the problem using Gurobi
            self.solver.solve(pulp.GUROBI(mip=True, msg=True, MIPgap=.01, LogFile="/tmp/gurobi.log"))
    
            logging.debug('out of the solver')
    
            # Time stamp for optimization solve time
            solve_end_seconds = dt.datetime.today().hour*60*60 + dt.datetime.today().minute*60 + dt.datetime.today().second
            solve_seconds = solve_end_seconds - start_seconds
            logging.info("Time taken to solve optimization is:" + str(solve_seconds))
            return self.debugging()                     
        else:
            self.merged_preoptCF.reset_index(inplace=True)
            self.merged_preoptCF['Result Space'] = self.merged_preoptCF['Optimal Space'].apply(lambda x: super(EnhancedOptimizer,self).roundValue(x,self.increment))
            self.merged_preoptCF.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
            return ('Optimal', self.merged_preoptCF, self.merged_preoptCF['Result Space'].sum()) 

