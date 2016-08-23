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


def create_output_artifact_from_dataframe(dataframe, *args, **kwargs):
    """
    Returns the bson.objectid.ObjectId of the resulting GridFS artifact

    """
    return fs.put(dataframe.to_csv().encode(), **kwargs)


def createLong(Stores, Categories, Levels, st, Optimal, Penetration, Historical):
    """
    Return str oid of GridFS artifact
    """
    storeDict=Historical[[0,1,2]].T.to_dict()
    #Historical=Historical.drop(Historical.columns[[1,2]],axis=1)
    l=0
    lOutput=pd.DataFrame(index=np.arange(len(Stores)*len(Categories)),columns=["Store","Climate","VSG","Category","Result Space","Optimal Space","Penetration","Historical Space"])
    for (i,Store) in enumerate(Stores):    
        for (j,Category) in enumerate(Categories):
            lOutput["Store"].iloc[l]=Store
            lOutput["Category"].iloc[l] = Category
            lOutput["Optimal Space"].iloc[l] = Optimal[Category].iloc[i]        
            lOutput["Penetration"].iloc[l] = Penetration[Category].iloc[i]
            # lOutput["Climate"].iloc[l] = storeDict.get("Store",{}).get("Climate",{}) 
            # lOutput["VSG"].iloc[l] = storeDict.get("Store",{}).get("VSG",{})
            lOutput["Historical Space"].iloc[l] = Historical[Category].iloc[i]
            for (k,Level) in enumerate(Levels):        
                if value(st[Store][Category][Level])== 1:
                    lOutput["Result Space"].iloc[l] = Level
            l=l+1
    lOutput['VSG']=lOutput.Store.apply(lambda x: (storeDict[x]['VSG ']))
    lOutput['Climate']=lOutput.Store.apply(lambda x: (storeDict[x]['Climate']))
    print(lOutput.head())
    return str(create_output_artifact_from_dataframe(lOutput,filename=jo))
    # return lOutput.to_csv("Long_Output",sep=",")


def createWide(Stores, Categories, Levels, st, Results, Optimal, Penetration, Historical):
    """
    Return str oid of GridFS artifact
    """
    Historical=Historical.drop(Historical.columns[[1,2]],axis=1)
    Optimal.columns = [str(col) + '_optimal' for col in Categories]
    Penetration.columns = [str(col) + '_penetration' for col in Categories]
    Results.columns = [str(col) + '_result' for col in Categories]
    Historical.columns = [str(col) + '_current' for col in Categories]
    print("Historical")
    print(Historical.head(2))
    wOutput=pd.concat([Results,Optimal,Penetration,Historical],axis=1) #Results.append([Optimal,Penetration,Historical])
    print(wOutput.head())
    return str(create_output_artifact_from_dataframe(wOutput,filename=jo))
    # return wOutput.to_csv("Wide_Output",sep=",")


def optimize(job_id,preOpt,tierCounts,spaceBound,increment,spaceArtifact,brandExitArtifact=None):

    """
    Run an LP-based optimization

    Side-effects:
        - creates file: Fixture_Optimization.lp (constraints)
        - creates file: solvedout.csv <= to be inserted into db
        - creates file: solvedout.text

    Synopsis:
        I just wrapped the script from Ken in a callable - DCE
    """
    print("Top of spaceArtifact")
    print(spaceArtifact.head())
    penetration=preOpt[0]
    opt_amt=preOpt[1]

    print("HEY I'M IN THE OPTIMIZATION!!!!!!!")
    try:
        print(spaceBound["Brand 1"][0])
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
    Categories = opt_amt.columns.values
    minLevel = min(opt_amt.min())
    maxLevel = max(opt_amt.max())
    Levels = list(np.arange(minLevel, maxLevel + increment, increment))
    # Levels.append(np.abs(0.0))
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
                    # brandExitArtifact[Category][Store] == 1
                if (brandExitArtifact[Category].loc[Store] != 0):
                    upper_bound[Category].loc[Store] = 0
                    lower_bound[Category].loc[Store] = 0
                    NewOptim += st[Store][Category].loc[0.0] == 1
                    NewOptim += ct[Category].loc[0.0] == 1

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
    '''
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
    '''
    print("Creating Outputs")
    Results=pd.DataFrame(index=Stores,columns=Categories)
    for (i,Store) in enumerate(Stores):
        for (j,Category) in enumerate(Categories):
            for (k,Level) in enumerate(Levels):
                if value(st[Store][Category][Level]) == 1:
                    Results[Category][Store] = Level
    # fs.put(createLong(Stores,Categories,Levels,st,preOpt[1],preOpt[0],spaceArtifact))
    # fs.put(createWide(preopt[1],preOpt[0],Results,spaceArtifact))
    # TODO: use jobid in long and wide filenames(filename key word argument)
    long_id = createLong(Stores,Categories,Levels,st,preOpt[1],preOpt[0],spaceArtifact)
    wide_id = createWide(Stores,Categories,Levels,st,Results,preOpt[1],preOpt[0],spaceArtifact)
    # print(type(longOutput))
    # wideOutput=createWide(preopt[1],preOpt[0],Results,spaceArtifact)

    db.jobs.find_one_and_update(
        {'_id': job_id},
        {
            "$set": {
                "artifactResults": {
                    'long_table':long_id
                    'wide_table':wide_id
                }
            }
        }
    )

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
    '''
    return #(longOutput)#,wideOutput)

    # testing=pd.read_csv("solvedout.csv").drop

# if __name__ == '__main__':
#     optimize()
# Should optimize after completion here call preop instead of in worker?


if __name__ == '__main__':
    df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
    create_output_artifact_from_dataframe(df, filename='hello.csv')
