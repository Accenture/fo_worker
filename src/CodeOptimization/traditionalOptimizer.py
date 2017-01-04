#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pulp import *
import numpy as np
import pandas as pd
import datetime as dt
import logging
from baseOptimizer import BaseOptimizer

class TraditionalOptimizer(BaseOptimizer):
    """
    Created on Wed Jan 4 16:00:51 2017

    @author: omkar.marathe

    This is the class for 
    Traditional Optimization
    """

    def __init__(self,jobName,Stores,Categories,spaceBound,increment,dataMunged,salesPen,tierCounts=None):
        super(TraditionalOptimizer,self).__init__(jobName,Stores,Categories,salesPen,tierCounts=None)
        self.spaceBound = spaceBound
        self.dataMunged = dataMunged

    def optimize(self):
    """
    Run an LP-based optimization

    Side-effects:
        - creates file: Fixture_Optimization.lp (constraints)
        - creates file: solvedout.csv <= to be inserted into db
        - creates file: solvedout.text

    Synopsis:
        I just wrapped the script from Ken in a callable - DCE
    """

    dataMunged['Optimal Space']=dataMunged['Optimal Space'].apply(lambda x: super(TraditionalOptimizer,self).roundValue(x, increment))
    spaceBound = pd.DataFrame.from_dict(spaceBound).T.reset_index()
    spaceBound.columns = ['Category', 'Space Lower Limit', 'Space Upper Limit', 'PCT_Space_Lower_Limit',
                       'PCT_Space_Upper_Limit']
    spaceBound['Space Lower Limit'] = spaceBound['Space Lower Limit'].apply(lambda x: super(TraditionalOptimizer,self).roundValue(x, increment))
    spaceBound['Space Upper Limit'] = spaceBound['Space Upper Limit'].apply(lambda x: super(TraditionalOptimizer,self).roundValue(x, increment))
    spaceBound=spaceBound[[0,1,2]]
    logging.info('set up new space bounds')
    dataMunged = dataMunged.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    start_time = dt.datetime.today().hour*60*60+ dt.datetime.today().minute*60 + dt.datetime.today().second
    opt_amt=dataMunged.pivot(index='Store', columns='Category', values='Optimal Space') #preOpt[1]
    salesPenetration=dataMunged.pivot(index='Store', columns='Category', values='Sales Penetration')
    brandExitArtifact = dataMunged.pivot(index='Store', columns='Category', values='Exit Flag')

    logging.info("HEY I'M IN THE OPTIMIZATION!!!!!!!")
    ###############################################################################################
    # Reading in of Files & Variable Set Up|| Will be changed upon adoption into tool
    ###############################################################################################

    ##########################################################################################
    ##################Vector Creation ||May be moved to another module/ program in the future
    ##########################################################################################
    
    logging.info('creating levels')
    minLevel = min(spaceBound[[1]].min())
    maxLevel = max(spaceBound[[2]].max())
    Levels = list(np.arange(minLevel, maxLevel + increment, increment))
    if 0.0 not in Levels:
        Levels.insert(0,0.0)
    logging.info("created levels")
    spaceBound = spaceBound.set_index('Category')

    b = .05
    bI = super(TraditionalOptimizer,self).searchParam('BBI', jobName)
    if bI == None:
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

    def adjustForOneIncr(row, bound, increment):
        """
        Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
        :param row: Individual row of Total Space Available in Store
        :param bound: Percent Bounding for Balance Back
        :param increment: Increment Size Determined by the User in the UI
        :return: Returns an adjusted vector of percentages by which individual store space should be held
        """
        return max(bound, (1 * increment) / row)

    incrAdj = super(TraditionalOptimizer,self).searchParam('ADJ', jobName)
    if incrAdj == None:
        locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForTwoIncr(row, bI, increment))
    else:
        locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForOneIncr(row, bI, increment))

    
    logging.info('created balance back vector')


    W = opt_amt.sum(axis=1).sum(axis=0)

    logging.info('Balance Back Vector')
    if tierCounts is not None:
        ct = LpVariable.dicts('CT', (Categories, Levels), 0, upBound=1,cat='Binary')

    st = LpVariable.dicts('ST', (Stores, Categories, Levels), 0,upBound=1, cat='Binary')
    logging.info('tiers created')

    NewOptim = LpProblem(jobName, LpMinimize)  # Define Optimization Problem/
    # Created Re

    # Brand Exit Enhancement & Sales Penetration Constraint
    if brandExitArtifact is None:
        for (i, Store) in enumerate(Stores):
            for (j, Category) in enumerate(Categories):
                if salesPenetration[Category].loc[Store] < salesPen:
                    NewOptim += st[Store][Category][0.0] == 1
        logging.info("No Brand Exit in the Optimization")
    else:
        logging.info('There is Brand Exit')
        for (i, Store) in enumerate(Stores):
            for (j, Category) in enumerate(Categories):
                if salesPenetration[Category].loc[Store] < salesPen:
                    NewOptim += st[Store][Category][0.0] == 1
                if (brandExitArtifact[Category].loc[Store] != 0):
                    
                    opt_amt[Category].loc[Store] = 0
                    NewOptim += st[Store][Category][0.0] == 1
                    if tierCounts is not None:
                        NewOptim += ct[Category][0.0] == 1
                    spaceBound['Space Lower Limit'].loc[Category] = 0
      

    logging.info('Brand Exit Done')
    BA = np.zeros((len(Stores), len(Categories), len(Levels)))
    error = np.zeros((len(Stores), len(Categories), len(Levels)))
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                BA[i][j][k] = opt_amt[Category].iloc[i]
                error[i][j][k] = np.absolute(BA[i][j][k] - Level)

    NewOptim += lpSum([(st[Store][Category][Level] * error[i][j][k]) for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for (k, Level) in enumerate(Levels)]), ""
    logging.info('created objective function')
###############################################################################################################
############################################### Constraints
###############################################################################################################
#Makes is to that there is only one Selected tier for each Store/ Category Combination
    for (i, Store) in enumerate(Stores):
    #     TODO: Exploratory analysis on impact of balance back on financials for Enhanced
    #     Store-level balance back constraint: the total space allocated to products at each location must be within the individual location balance back tolerance limit
        NewOptim += lpSum(
            [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
             enumerate(Levels)]) >= locSpaceToFill[Store] * (1 - locBalBackBoundAdj[Store])  
        NewOptim += lpSum(
            [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
             enumerate(Levels)]) <= locSpaceToFill[Store] * (1 + locBalBackBoundAdj[Store])  

        

    #One Space per Store Category
    #Makes sure that the number of fixtures, by store, does not go above or below some percentage of the total number of fixtures within the store 
        for (j,Category) in enumerate(Categories):
            NewOptim += lpSum([st[Store][Category][Level] for (k,Level) in enumerate(Levels)]) == 1
        # Test Again to check if better performance when done on ct level
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) <= spaceBound['Space Upper Limit'].loc[Category]
            
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound['Space Lower Limit'].loc[Category]


    logging.info("After Space Bounds")
    #Tier Counts Enhancement
    
    if tierCounts is not None:
        for (j,Category) in enumerate(Categories):
            
            NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) >= tierCounts[Category][0] #, "Number_of_Tiers_per_Category"
            NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) <= tierCounts[Category][1]
    #Relationship between Selected Tiers & Created Tiers
        #Verify that we still cannot use a constraint if not using a sum - Look to improve efficiency
            for (k,Level) in enumerate(Levels):
                NewOptim += lpSum([st[Store][Category][Level] for (i,Store) in enumerate(Stores)])/len(Stores) <= ct[Category][Level]#, "Relationship between ct & st" 
    

#Global Balance Back  
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) >= W * (1 - b)
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) <= W * (1 + b)
    
    logging.info("The problem has been formulated")

    optGap=searchParam('MIP',jobName)

    if optGap == None:
        optGap = .1

    try:
        #if config.SOLVER == 'GUROBI':
        #    NewOptim.solve(pulp.GUROBI(mip=True, msg=True, MIPgap=optGap, LogFile="/tmp/gurobi.log"))
        #else:

        NewOptim.solve(pulp.PULP_CBC_CMD(msg=2))

    except Exception:
        logging.exception('A thing')


    # #Debugging
    logging.info("#####################################################################")
    logging.info('\n\n\n {} \n\n\n'.format(LpStatus[NewOptim.status]))
    logging.info("#####################################################################")
    if LpStatus[NewOptim.status] == 'Optimal':
        logging.info('Found an optimal solution')
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
        dataMunged=pd.merge(dataMunged,Results,on=['Store','Category'])
        return (LpStatus[NewOptim.status],dataMunged,value(NewOptim.objective)) #(longOutput)#,wideOutput)
    else:
        dataMunged['Result Space'] = 0
        return(LpStatus[NewOptim.status],dataMunged,0)

    

