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
import pymongo as pm
import gridfs
db = pm.MongoClient()['app']
fs = gridfs.GridFS(db)

def optimize(opt_amt,tierCounts,spaceBound,increment,brandExitArtifact=None):

    """
    Run an LP-based optimization

    Side-effects:
        - creates file: Fixture_Optimization.lp (constraints)
        - creates file: solvedout.csv <= to be inserted into db
        - creates file: solvedout.text

    Synopsis:
        I just wrapped the script from Ken in a callable - DCE
        """

    print("HEY I'M IN THE OPTIMIZATION!!!!!!!")
    try:
        print(spaceBound[Brand1])
    except:
        print("didn't Work")
    ###############################################################################################
    # Reading in of Files & Variable Set Up|| Will be changed upon adoption into tool
    ###############################################################################################

    ##########################################################################################
    ##################Vector Creation ||May be moved to another module/ program in the future
    ##########################################################################################
    # opt_amt.index=opt_amt.index.values.astype(int)
    Stores = opt_amt.index.tolist()

    # Setting up the Selected Tier Combinations -- Need to redo if not getting or creating data for all possible levels
    print(opt_amt.columns.values)
    print(opt_amt.head())
    Categories = opt_amt.columns.values
    minLevel = min(opt_amt.min())
    maxLevel = max(opt_amt.max())
    print("minLevel")
    print(minLevel)
    print("maxLevel")
    print(maxLevel)
    Levels = list(np.arange(minLevel, maxLevel + increment, increment))
    print("Levels: ")
    print(Levels)
    # Levels.append(np.abs(0.0))

    print(Categories)
    print(Levels)

    b = .05
    bI = .1

    # Create a Vectors & Arrays of required variables
    # Calculate Total fixtures(TotFixt) per store by summing up the individual fixture counts
    W = opt_amt.sum(axis=1).sum(axis=0)
    TFC = opt_amt.sum(axis=1)
    ct = LpVariable.dicts('Created Tiers', (Categories, Levels), 0, upBound=1,
                          cat='Binary')
    st = LpVariable.dicts('Selected Tiers', (Stores, Categories, Levels), 0,
                          upBound=1, cat='Binary')

    NewOptim = LpProblem("FixtureOptim", LpMinimize)  # Define Optimization Problem/

    try:
        # Brand Exit Enhancement
        for (j, Category) in enumerate(Categories):
            for (i, Store) in enumerate(Stores):
                # if (upper_bound[Category][Store] == 0):
                #     brandExitArtifact[Category][Store] == 1
                if (brandExitArtifact[Category].loc[Store] != 0):
                    upper_bound[Category].loc[Store] = 0
                    lower_bound[Category].loc[Store] = 0
                    NewOptim += st[Store][Category].loc[0.0] == 1
                    NewOptim += ct[Category].loc[0.0] == 1
                    #df['Estimated Sales'][Store, Category, 0.0] = 0

        for (j, Category) in enumerate(Categories):
            if (sum(brandExitArtifact[Category].values()) > 0):
                tier_count["Upper_Bound"][Category] += 1
    except:
        print("No Brand Exit")

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
        # if brandExitArtifact[Category].iloc[Store] == 0:
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
    print("#####################################################################")
    print(LpStatus[NewOptim.status])
    print("#####################################################################")
    # print(LpStatusInfeasible)
    # print(LpStatusUndefined)
    # print(LpStatusOptimal)
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
    
    # solvedout, result_id = fs.open_upload_stream("test_file",chunk_size_bytes=4,metadata={"contentType": "text/csv"}) 
    #open("solvedout.csv", 'w')
    with fs.new_file(filename="spaceResults.csv",content_type="type/csv") as solvedout:
        solvedout.write("Store,".encode("UTF-8"))
        for (j, Category) in enumerate(Categories):
            solvedout.write(opt_amt.columns.values[j].encode("UTF-8") + ",".encode("UTF-8"))
        for (i, Store) in enumerate(Stores):
            solvedout.write(str("\n"+str(Store)).encode("UTF-8"))
            for (j, Category) in enumerate(Categories):
                solvedout.write(",".encode("UTF-8"))
                for (k, Level) in enumerate(Levels):
                    if value(st[Store][Category][Level]) == 1:
                        solvedout.write(str(Level).encode("UTF-8"))
        solvedout.close()
    # print(LpStatus[LpStatus])
    return #results

    # testing=pd.read_csv("solvedout.csv").drop

# if __name__ == '__main__':
#     optimize()
# Should optimize after completion here call preop instead of in worker?