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
import logging as logging


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
                sParam = int(jobName[(len(search) + begin):(len(search) + begin + length)]) / 100
                logging.info('{} has been changed to {}'.format(search,sParam))
                return sParam
            except:
                return True
        else:
            return None

    def adjustForOneIncr(row, bound, increment):
        """
        Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
        :param row: Individual row of Total Space Available in Store
        :param bound: Percent Bounding for Balance Back
        :param increment: Increment Size Determined by the User in the UI
        :return: Returns an adjusted vector of percentages by which individual store space should be held
        """
        return max(bound, increment / row)

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
    # Hard-coded tolerance limits for balance back constraints
    aggBalBackBound = 0.05  # 5%
    locBalBackBound = 0.05  # 10%

    logging.info('now have balance back bounds')
    # EXPLORATORY ONLY: ELASTIC BALANCE BACK
    # Hard-coded tolerance limits for balance back constraints without penalty
    # The free bounds are the % difference from space to fill that is allowed without penalty
    # The penalty is incurred if the filled space goes beyond the free bound % difference from space to fill
    # The tighter the bounds and/or higher the penalties, the slower the optimization run time
    # The penalty incurred should be different for Traditional vs Enhanced as the scale of the objective function differs
    indBalBackFreeBound = pd.DataFrame(data=increment,index=Stores,columns=Categories) #pd.DataFrame(data=increment,index=Stores,columns=Categories) #exploratory, value would have to be determined through exploratory analysis
    indBalBackPenalty = increment #exploratory, value would have to be determined through exploratory analysis
    # indBalBackFreeBoundAdj = pd.DataFrame(data=increment,index=Stores,columns=Categories)
    # Option
    # indBalBackFreeBoundAdj = locSpaceToFill.apply(lambda row:adjustForOneIncr(row,locBalBackFreeBound,increment))

    logging.info('created balance back vector')

    W = opt_amt.sum(axis=1).sum(axis=0)

    logging.info('Balance Back Vector')
    if tierCounts is not None:
        ct = LpVariable.dicts('CT', (Categories, Levels), 0, upBound=1,cat='Binary')

    st = LpVariable.dicts('ST', (Stores, Categories, Levels), 0,upBound=1, cat='Binary')

    bbt = LpVariable.dicts('BBT', (Stores), 0,upBound=1, cat='Binary')
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
                # st[Store][Category][Level].setInitialValue(BA[i][j][k])
                error[i][j][k] = np.absolute(BA[i][j][k] - Level)

    # NewOptim += lpSum([(bbt[Store] for Store in Stores)])/len(Stores)
    NewOptim += lpSum([(bbt[Store] for Store in Stores)])

    logging.info('created objective function')
###############################################################################################################
# Constraints
###############################################################################################################
#Makes is to that there is only one Selected tier for each Store/ Category Combination
    NewOptim += lpSum([bbt for (i, Store) in enumerate(Stores)])/len(Stores) <= .03
    for (i, Store) in enumerate(Stores):
        # Conditional because you can't take the absolute using PuLP
        if lpSum([(st[Store][Category][Level] * Level) for (j, Category) in enumerate(Categories) for
             (k, Level) in enumerate(Levels)]) - locSpaceToFill[Store] >= 0:
            NewOptim += lpSum(
                [(st[Store][Category][Level] * Level) for (j, Category) in enumerate(Categories) for (k, Level)
                 in
                 enumerate(Levels)]) - locSpaceToFill[Store] <= bbt[Store]
        else:
            NewOptim += locSpaceToFill[Store] - lpSum(
                [(st[Store][Category][Level] * Level) for (j, Category) in enumerate(Categories) for (k, Level)
                 in
                 enumerate(Levels)]) <= bbt[Store]

    #One Space per Store Category
    #Makes sure that the number of fixtures, by store, does not go above or below some percentage of the total number of fixtures within the store
        for (j,Category) in enumerate(Categories):
            NewOptim += lpSum([st[Store][Category][Level] for (k,Level) in enumerate(Levels)]) == 1#, "One_Level_per_Store-Category_Combination"
            # Test Again to check if better performance when done on ct level
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) <= spaceBound['Space Upper Limit'].loc[Category], "Space_Upper_Limit-Store_" + str(Store) + ",Category_" + str(Category)
            # if brandExitArtifact is not None:
            #     if brandExitArtifact[Category].iloc[int(i)] == 0:
            #         NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound[Category][0] + increment
            #     else:
            #         NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound[Category][0]
            # else:
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound['Space Lower Limit'].loc[Category], "Space_Lower_Limit-Store_" + str(Store) + ",Category_" + str(Category)
            eIndSpace = lpSum([(st[Store][Category][Level]) * Level for (k,Level) in enumerate(Levels)])
            cIndBalBackPenalty = LpConstraint(e=eIndSpace, sense=LpConstraintEQ,
                                              name="Optimal Space Balance Back Penalty:_" + str(Store) + str(
                                                  Category), rhs=opt_amt[Category].loc[Store])
            NewOptim.extend(cIndBalBackPenalty.makeElasticSubProblem(penalty=indBalBackPenalty,
                                                                     proportionFreeBound=indBalBackFreeBound.loc[
                                                                         Store, Category]))

            # for (k, Level) in enumerate(Levels):
            #     Trying an Elastic Constraint for the Optimal Space
            #     NewOptim += lpSum(
                # [(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
                #  enumerate(Levels)]) >= BA[i][j][k] * 1(test), "Optimal Space Balance Back Lower Limit-Store" + str(Store)

    logging.info("After Space Bounds")
#Tier Counts Enhancement
    # totalTiers=0
    if tierCounts is not None:
        for (j,Category) in enumerate(Categories):
            # totalTiers=totalTiers+tierCounts[Category][1]
            NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) >= tierCounts[Category][0], "Tier_Count_Lower_Limit-Category_" + str(Category)
            NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) <= tierCounts[Category][1], "Tier Count Upper Limit-Category_" + str(Category)
    #Relationship between Selected Tiers & Created Tiers
        #Verify that we still cannot use a constraint if not using a sum - Look to improve efficiency
            for (k,Level) in enumerate(Levels):
                NewOptim += lpSum([st[Store][Category][Level] for (i,Store) in enumerate(Stores)])/len(Stores) <= ct[Category][Level], "Relationship between_"+str(Store)+"_"+str(Category)+"_"+str(Level)

#Global Balance Back
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) >= W * (1 - b), "All Stores Lower Bound Balance Back Constraint"
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) <= W * (1 + b), "All Stores Upper Bound Balance Back Constraint"
    # NewOptim.writeLP("Fixture_Optimization.lp")
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
    logging.info("Creating Outputs")
    # Debugging
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
