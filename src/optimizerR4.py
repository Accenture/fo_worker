#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Thu Jun  2 11:33:51 2016

@author: kenneth.l.sylvain
"""

from pulp import *
# from pulp import LpVariable, LpInteger, LpProblem, LpMinimize, lpSum, LpStatus,PULP_CBC_CMD, pulpTestAll, value, LpBinary
import numpy as np
import pandas as pd
import os
import json

class SampleFile(object):

    src_path = os.path.dirname(__file__)
    test_files_path = os.path.join(os.path.dirname(src_path),
                                   'test/test_optimizer_files')

    @classmethod
    def get(cls, filename):
        return os.path.join(cls.test_files_path, filename)


def optimize(opt_amt,tier_count,store_bounding,increment):

    """
    Run an LP-based optimization

    Side-effects:
        - creates file: Fixture_Optimization.lp (constraints)
        - creates file: solvedout.csv <= to be inserted into db
        - creates file: solvedout.text

    Synopsis:
        I just wrapped the script from Ken in a callable - DCE
    """

    #############################################################################################
    # Reading in of Files & Variable Set Up|| Will be changed upon adoption into tool
    ###############################################################################################
   
    ##########################################################################################
    ##################Vector Creation ||May be moved to another module/ program in the future
    ##########################################################################################
    Stores = list(optimal_space.index)

    # Setting up the Selected Tier Combinations -- Need to redo if not getting or creating data for all possible levels
    Categories = optimal_space.columns.values
    minLevel = min(optimal_space.min())
    maxLevel = max(optimal_space.max())
    Levels = list(np.arange(minLevel, maxLevel + increment, increment))
    Levels.append(np.abs(0.0))

    b = .05
    bI = .1

    # Create a Vectors & Arrays of required variables
    # Calculate Total fixtures(TotFixt) per store by summing up the individual fixture counts
    W = optimal_space.sum(axis=1).sum(axis=0)
    # TFC = optimal_space.sum(axis=1).reshape(optimal_space.sum(axis=1).shape[0], 1)
    TFC = optimal_space.sum(axis=1)
    ct = LpVariable.dicts('Created Tiers', (Categories, Levels), 0, upBound=1,
                          cat='Binary')
    st = LpVariable.dicts('Selected Tiers', (Stores, Categories, Levels), 0,
                          upBound=1, cat='Binary')
    '''
    # for (i,Store) in Stores:
        # for (j,Category) in Categories:
            # for (k,Level) in Levels:
                # if optimal_space(Store,Category) = Level: #exists
                    # st[Store][Category][Level].setInitialValue(1)
    '''
    # zt = LpVariable.dicts('Tier', df,0, upBound=1, cat='Binary')

    NewOptim = LpProblem("FixtureOptim", LpMaximize)  # Define Optimization Problem/

    # Brand Exit Enhancement
    for (j, Category) in enumerate(Categories):
        for (i, Store) in enumerate(Stores):
            # if (upper_bound[Category][Store] == 0):
            #     brand_exit[Category][Store] == 1
            if (brand_exit[Category][Store] != 0):
                upper_bound[Category][Store] = 0
                lower_bound[Category][Store] = 0
                NewOptim += st[Store][Category][0.0] == 1
                NewOptim += ct[Category][0.0] == 1
                #df['Estimated Sales'][Store, Category, 0.0] = 0

    for (j, Category) in enumerate(Categories):
        if (sum(brand_exit[Category].values()) > 0):
            tier_count["Upper_Bound"][Category] += 1
    
    NewOptim += lpSum([(st[Store][Category][Level] * error[i][j][k]) \
    for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for (k, Level) in enumerate(Levels)]), ""

###############################################################################################################
############################################### Constraints
###############################################################################################################
#Makes is to that there is only one Selected tier for each Store/ Category Combination
    for (i,Store) in enumerate(Stores):
    #Conditional for Balance Back regarding if in Fixtures || 2 Increment Min & Max instead
        if TFC[Store] > increment * 5:
            NewOptim += lpSum([(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
                               enumerate(Levels)]) <= TFC[Store] * (1 + bI)#, "Upper Bound for Fixtures per Store"
            NewOptim += lpSum([(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
                               enumerate(Levels)]) >= TFC[Store] * (1 - bI)#, "Lower Bound for Fixtures per Store"
        else:
            NewOptim += lpSum([(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
                               enumerate(Levels)]) <= TFC[Store] + (increment * 2)#, "Upper Bound for Fixtures per Store"
            NewOptim += lpSum([(st[Store][Category][Level]) * Level for (j, Category) in enumerate(Categories) for (k, Level) in
                               enumerate(Levels)]) >= TFC[Store] - (increment * 2)#, "Lower Bound for Fixtures per Store"
        
#Makes sure that the number of fixtures, by store, does not go above or below some percentage of the total number of fixtures within the store 
    for (j,Category) in enumerate(Categories):
        NewOptim += lpSum([st[Store][Category][Level] for (k,Level) in enumerate(Levels)]) == 1#, "One_Level_per_Store-Category_Combination"

        NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)] ) <= min(upper_bound[Category][Store],spaceBound[Category][1])
        
        if brand_exit[Category][Store] == 0:
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= max(spaceBound[Category][0],lower_bound[Category][Store]) + increment
        else:
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= max(spaceBound[Category][0],lower_bound[Category][Store])
        
    # Tier Counts            
        NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) >= tierCounts[Category][0] #, "Number_of_Tiers_per_Category"
        NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) <= tierCounts[Category][1]
    
    #Verify that we still cannot use a constraint if not using a sum - Look to improve efficiency   
        for (k,Level) in enumerate(Levels):
            NewOptim += lpSum([st[Store][Category][Level] for (i,Store) in enumerate(Stores)])/len(Stores) <= ct[Category][Level]#, "Relationship between ct & st"
                          

    #NewOptim += lpSum([ct[Category][Level] for (j,Category) in enumerate(Categories) for (k,Level) in enumerate(Levels)]) <= len(Categories)*sum(tier_count["Upper_Bound"].values())

    # Makes sure that the number of fixtures globally does not go above or below some percentage of the total number of fixtures within  
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) >= W * (1 - b)
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) <= W * (1 + b)

    #NewOptim.writeLP("Fixture_Optimization.lp")
    NewOptim.solve()
    ''''
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

    solvedout = open('solvedout.txt', 'w')
    solvedout.write("Status:" + str(LpStatus[NewOptim.status]) + "\n")
    solvedout.write("--------------------------------------------------- \n")
    solvedout.write("For Selected Tiers \n")
    solvedout.write("Number of Negatives Count is: " + str(NegativeCount) + "\n")
    solvedout.write("Number of Zeroes Count is: " + str(LowCount) + "\n")
    solvedout.write("Number Above 0 and Below 1 Count is: " + str(TrueCount) + "\n")
    solvedout.write("Number of Selected Tiers: " + str(OneCount) + "\n")
    solvedout.write("--------------------------------------------------- \n")
    solvedout.write("For Created Tiers \n")
    solvedout.write("Number of Negatives Count is: " + str(ctNegativeCount) + "\n")
    solvedout.write("Number of Zeroes Count is: " + str(ctLowCount) + "\n")
    solvedout.write(
        "Number Above 0 and Below 1 Count is: " + str(ctTrueCount) + "\n")
    solvedout.write("Number of Created Tiers: " + str(ctOneCount) + "\n")
    solvedout.write("--------------------------------------------------- \n")
    solvedout.write("--------------------------------------------------- \n\n\n")
    '''
    
    solvedout = open("solvedout.csv", 'w')
    solvedout.write("Store,")
    for (j, Category) in enumerate(Categories):
        solvedout.write(optimal_space.columns.values[j] + ",")
    for (i, Store) in enumerate(Stores):
        solvedout.write("\n" + str(Store))
        for (j, Category) in enumerate(Categories):
            solvedout.write(",")
            for (k, Level) in enumerate(Levels):
                if value(st[Store][Category][Level]) == 1:
                    solvedout.write(str(Level))
    solvedout.close()

    # testing=pd.read_csv("solvedout.csv").drop

# if __name__ == '__main__':
#     optimize()
# Should optimize after completion here call preop instead of in worker?