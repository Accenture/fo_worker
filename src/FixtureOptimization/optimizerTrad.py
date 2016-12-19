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


def optimizeTrad(jobName,Stores,Categories,spaceBound,increment,dataMunged,salesPen,tierCounts=None):
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
                return None
        else:
            return None

    dataMunged['Optimal Space']=dataMunged['Optimal Space'].apply(lambda x: roundValue(x, increment))
    spaceBound = pd.DataFrame.from_dict(spaceBound).T.reset_index()
    spaceBound.columns = ['Category', 'Space Lower Limit', 'Space Upper Limit', 'PCT_Space_Lower_Limit',
                       'PCT_Space_Upper_Limit']
    spaceBound['Space Lower Limit'] = spaceBound['Space Lower Limit'].apply(lambda x: roundValue(x, increment))
    spaceBound['Space Upper Limit'] = spaceBound['Space Upper Limit'].apply(lambda x: roundValue(x, increment))
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
    # Categories = opt_amt.columns.values
    logging.info('creating levels')
    minLevel = min(spaceBound[[1]].min())
    maxLevel = max(spaceBound[[2]].max())
    Levels = list(np.arange(minLevel, maxLevel + increment, increment))
    if 0.0 not in Levels:
        Levels.insert(0,0.0)
    logging.info("created levels")
    spaceBound = spaceBound.set_index('Category')

    b = .05
    bI = searchParam('BBI', jobName)
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

    locBalBackBoundAdj = locSpaceToFill.apply(lambda row: adjustForTwoIncr(row, bI, increment))

    # locBalBackBoundAdj = pd.Series(0,index=locSpaceToFill)
    logging.info('created balance back vector')

    # Adjust location balance back tolerance limit so that it's at least 2 increments
    # def adjustForTwoIncr(row,bound,increment):
    #     return max(bound,(2*increment)/row)

    # Create a Vectors & Arrays of required variables
    # Calculate Total fixtures(TotFixt) per store by summing up the individual fixture counts
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
                    # upper_bound[Category].loc[Store] = 0
                    # lower_bound[Category].loc[Store] = 0
                    opt_amt[Category].loc[Store] = 0
                    NewOptim += st[Store][Category][0.0] == 1
                    if tierCounts is not None:
                        NewOptim += ct[Category][0.0] == 1
                    spaceBound['Space Lower Limit'].loc[Category] = 0


        # for (j, Category) in enumerate(Categories):
        #     if (sum(brandExitArtifact[Category].values()) > 0):
        #         tier_count["Upper_Bound"][Category] = tier_count["Upper_Bound"][Category] + 1

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
             enumerate(Levels)]) >= locSpaceToFill[Store] * (1 - locBalBackBoundAdj[Store])  # , "Location Balance Back Lower Limit - STR " + str(Store)
        NewOptim += lpSum(
            [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
             enumerate(Levels)]) <= locSpaceToFill[Store] * (1 + locBalBackBoundAdj[Store])  # , "Location Balance Back Upper Limit - STR " + str(Store)

        # Testing Out Exact Balance Back
        # NewOptim += lpSum(
        #     [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
        #      enumerate(Levels)]) == locSpaceToFill[Store]  # , "Location Balance Back Lower Limit - STR " + str(Store)

#One Space per Store Category
    #Makes sure that the number of fixtures, by store, does not go above or below some percentage of the total number of fixtures within the store 
        for (j,Category) in enumerate(Categories):
            NewOptim += lpSum([st[Store][Category][Level] for (k,Level) in enumerate(Levels)]) == 1#, "One_Level_per_Store-Category_Combination"
        # Test Again to check if better performance when done on ct level
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) <= spaceBound['Space Upper Limit'].loc[Category]
            # if brandExitArtifact is not None:
            #     if brandExitArtifact[Category].iloc[int(i)] == 0:
            #         NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound[Category][0] + increment
            #     else:
            #         NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound[Category][0]
            # else:
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound['Space Lower Limit'].loc[Category]


    logging.info("After Space Bounds")
#Tier Counts Enhancement
    # totalTiers=0
    if tierCounts is not None:
        for (j,Category) in enumerate(Categories):
            # totalTiers=totalTiers+tierCounts[Category][1]
            NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) >= tierCounts[Category][0] #, "Number_of_Tiers_per_Category"
            NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) <= tierCounts[Category][1]
    #Relationship between Selected Tiers & Created Tiers
        #Verify that we still cannot use a constraint if not using a sum - Look to improve efficiency
            for (k,Level) in enumerate(Levels):
                NewOptim += lpSum([st[Store][Category][Level] for (i,Store) in enumerate(Stores)])/len(Stores) <= ct[Category][Level]#, "Relationship between ct & st"
   
    # NewOptim += lpSum([ct[Category][Level] for (j,Category) in enumerate(Categories) for (k,Level) in enumerate(Levels)]) <= totalTiers #len(Categories)*sum(tier_count[Category][1].values())

#Global Balance Back  
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) >= W * (1 - b)
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) <= W * (1 + b)
    # NewOptim.writeLP("Fixture_Optimization.lp")
    # LpSolverDefault.msg = 1
    logging.info("The problem has been formulated")

#Solving the Problem
    # NewOptim.writeLP("Fixture_Optimization.lp")
    # NewOptim.writeMPS(str(jobName)+".mps")
    # Solve the problem using Gurobi
    optGap=searchParam('MIP',jobName)

    if optGap == None:
        optGap = .1

    try:
        NewOptim.solve(pulp.GUROBI(mip=True, msg=True, MIPgap=optGap, LogFile="/tmp/gurobi.log"))
    except Exception:
        logging.exception('A thing')


    # local development uses CBC until
    #NewOptim.solve(pulp.PULP_CBC_CMD(msg=2))

    # #Debugging
    logging.info("#####################################################################")
    logging.info('\n\n\n {} \n\n\n'.format(LpStatus[NewOptim.status]))
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


# if __name__ == '__main__':
#     df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
#     create_output_artifact_from_dataframe(df, filename='hello.csv')
