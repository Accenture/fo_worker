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


def optimize(opt_amt,tierCounts,spaceBound,increment):

    """
    Run an LP-based optimization

    Side-effects:
        - creates file: Fixture_Optimization.lp (constraints)
        - creates file: solvedout.csv <= to be inserted into db
        - creates file: solvedout.text

    Synopsis:
        I just wrapped the script from Ken in a callable - DCE
    """

    ###############################################################################################
    # Reading in of Files & Variable Set Up|| Will be changed upon adoption into tool
    ###############################################################################################
    '''
    # TODO: get from gridfs (space artifact)
    opt_amt = pd.read_csv(
        SampleFile.get('fixture_data.csv'),
        header=0).set_index("Store")

    # TODO: get from gridfs (brand exit artifact)
    brand_exit = pd.read_csv(
        SampleFile.get('Brand_Exit.csv'),
        header=0).set_index("Store").to_dict()

    # TODO: Get from job context (webform data)
    tier_count = pd.read_csv(
        SampleFile.get('Tier_Counts.csv'),
        header=0).set_index("Product").to_dict()

    # TODO: Get from gridfs (sales artifact)
    sales = pd.read_csv(
        SampleFile.get('transactions_data.csv'),
        header=0).set_index("Store")  # .to_dict()
    '''

    # Created within Python in Future
    # TODO: "Integrate Later" -ken
    # lower_bound = pd.read_csv(
    #     SampleFile.get('Lower_Bound.csv'),
    #     header=0).set_index("Store").to_dict()

    # TODO: this is store-category stuff (not used yet) -ken
    # upper_bound = pd.read_csv(
    #     SampleFile.get('Upper_Bound.csv'),
    #     header=0).set_index("Store").to_dict()

    #increment = 2.5  # Increment for Linear Feet or Fixture Levels | Needs to be created as a NumPy float

    # str(for_dict['Store'][0]),str(for_dict['Product'][0]),str(for_dict['Level'][0])

    ##########################################################################################
    ##################Vector Creation ||May be moved to another module/ program in the future
    ##########################################################################################
    opt_amt.index=opt_amt.index.astype(int)
    Stores = list(opt_amt.index)

    # Setting up the Selected Tier Combinations -- Need to redo if not getting or creating data for all possible levels
    Categories = opt_amt.columns.values
    minLevel = min(opt_amt.min())
    maxLevel = max(opt_amt.max())
    Levels = list(np.arange(minLevel, maxLevel + increment, increment))
    Levels.append(np.abs(0.0))

    b = .05
    bI = .1

    # Create a Vectors & Arrays of required variables
    # Calculate Total fixtures(TotFixt) per store by summing up the individual fixture counts
    W = opt_amt.sum(axis=1).sum(axis=0)
    # TFC = opt_amt.sum(axis=1).reshape(opt_amt.sum(axis=1).shape[0], 1)
    TFC = opt_amt.sum(axis=1)
    ct = LpVariable.dicts('Created Tiers', (Categories, Levels), 0, upBound=1,
                          cat='Binary')
    st = LpVariable.dicts('Selected Tiers', (Stores, Categories, Levels), 0,
                          upBound=1, cat='Binary')
    '''
    for (i,Store) in Stores:
        for (j,Category) in Categories:
            for (k,Level) in Levels:
                if opt_amt(Store,Category) = Level: #exists
                    st[Store][Category][Level].setInitialValue(1)
    '''
    # zt = LpVariable.dicts('Tier', df,0, upBound=1, cat='Binary')

    NewOptim = LpProblem("FixtureOptim", LpMinimize)  # Define Optimization Problem/

    # Brand Exit Enhancement
    # for (j, Category) in enumerate(Categories):
    #     for (i, Store) in enumerate(Stores):
    #         # if (upper_bound[Category][Store] == 0):
    #         #     brand_exit[Category][Store] == 1
    #         if (brand_exit[Category][Store] != 0):
    #             # upper_bound[Category][Store] = 0
    #             # lower_bound[Category][Store] = 0
    #             NewOptim += st[Store][Category][0.0] == 1
    #             NewOptim += ct[Category][0.0] == 1
    #             #df['Estimated Sales'][Store, Category, 0.0] = 0

    # for (j, Category) in enumerate(Categories):
    #     if (sum(brand_exit[Category].values()) > 0):
    #         tier_count["Upper_Bound"][Category] += 1
    '''
    # sales = sales_data
    # finding sales penetration for each brand in given stores
    sum_sales = sales.sum(axis=1)
    sp = sales.div(sum_sales, axis='index')
    # calculate adjusted penetration
    adj_p = sp

    opt_amt = adj_p.multiply(TFC, axis='index').as_matrix()
    '''
    # opt_amt = np.array(adj_p)
    # for (i,Store) in Stores:
    #    for (j,Category) in Categories:
    #        opt_amt[i, j] = adj_p[Store, Category] * TFC[i]

    # rounded = np.around(opt_amt, 0) + (10-(np.mod(np.around(opt_amt, 0), 10)))
    # opt_amt=roundArray(opt_amt,increment)
    # print(len(opt_amt))
    # print(len(Stores))
    BA = np.zeros((len(Stores), len(Categories), len(Levels)))
    error = np.zeros((len(Stores), len(Categories), len(Levels)))
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                # print("i equals "+i+"and Store equals "+Store)
                BA[i][j][k] = opt_amt[Category].iloc[i]
                error[i][j][k] = np.absolute(BA[i][j][k] - Level)

    NewOptim += lpSum(
        [(st[Store][Category][Level] * error[i][j][k]) for (i, Store) in
         enumerate(Stores) for (j, Category) in enumerate(Categories) for (k, Level)
         in enumerate(Levels)]), ""

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
#Test Again to check if better performance when done on ct level
#Different Bounding Structures
        NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) <= spaceBound[Category][1]         
        # if brand_exit[Category][Store] == 0:
        #     NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound[Category][0] + increment
        # else:
        NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound[Category][0]
            
#Store Category Level Bounding
        #NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)] ) >= lower_bound[Category][Store]#,
        #NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)] ) <= upper_bound[Category][Store]#,
        

    for (j,Category) in enumerate(Categories):
        NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) >= tierCounts[Category][0] #, "Number_of_Tiers_per_Category"
        NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) <= tierCounts[Category][1]
    #Verify that we still cannot use a constraint if not using a sum - Look to improve efficiency   
        for (k,Level) in enumerate(Levels):
            NewOptim += lpSum([st[Store][Category][Level] for (i,Store) in enumerate(Stores)])/len(Stores) <= ct[Category][Level]#, "Relationship between ct & st"
                          

#NewOptim += lpSum([ct[Category][Level] for (j,Category) in enumerate(Categories) for (k,Level) in enumerate(Levels)]) <= len(Categories)*sum(tier_count["Upper_Bound"].values())

#Makes sure that the number of fixtures globally does not go above or below some percentage of the total number of fixtures within  
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) >= W * (1 - b)
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
         (k, Level) in enumerate(Levels)]) <= W * (1 + b)

    #NewOptim.writeLP("Fixture_Optimization.lp")
    NewOptim.solve()
    print(LpStatus)
    print(LpStatusInfeasible)
    print(LpStatusUndefined)
    print(LpStatusOptimal)
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
    
    # solvedout = open("solvedout.csv", 'w')
    # solvedout.write("Store,")
    # for (j, Category) in enumerate(Categories):
    #     solvedout.write(opt_amt.columns.values[j] + ",")
    # for (i, Store) in enumerate(Stores):
    #     solvedout.write("\n" + str(Store))
    #     for (j, Category) in enumerate(Categories):
    #         solvedout.write(",")
    #         for (k, Level) in enumerate(Levels):
    #             if value(st[Store][Category][Level]) == 1:
    #                 solvedout.write(str(Level))
    # solvedout.close()

    results=pd.DataFrame(index=Stores, columns=Categories)
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                if value(st[Store][Category][Level]) == 1:
                    results[Store][Category]=Level

    # return results
    # testing=pd.read_csv("solvedout.csv").drop

# if __name__ == '__main__':
#     optimize()
# Should optimize after completion here call preop instead of in worker?