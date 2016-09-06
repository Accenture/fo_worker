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
# import os
import datetime as dt
from openpyxl import load_workbook
import time

optName=input("Please Name your Optimization: \n")

file="ScriptTestData.xlsx"

def brandExitMung(df,Stores,Categories):
    df=df.drop(df.index[0])
    df=df.reset_index(drop=True)
    brand_exit = pd.DataFrame(index=Stores,columns=Categories)
    for (i,Store) in enumerate(Stores):
        for (j,Category) in enumerate(Categories):    
            if str(Store) in pd.unique(df[Category].values):
                brand_exit[Category].iloc[i] = 1
            else:
                brand_exit[Category].iloc[i] = 0
    return brand_exit

def calcPen(metric):
    return metric.div(metric.sum(axis=1),axis='index')
    
def getColumns(df):
    # print(df[[ *np.arange(len(df.columns))[0::9] ]].drop(df.index[[0]]).convert_objects(convert_numeric=True).columns)
    return df[[ *np.arange(len(df.columns))[0::9] ]].drop(df.index[[0]]).convert_objects(convert_numeric=True).columns

def spreadCalc(sales,boh,receipt,master_columns,salesPenetrationThreshold):
    # storing input sales and inventory data in separate 2D arrays
    # finding sales penetration, GAFS and spread for each brand in given stores
    # calculate adjusted penetration
    #Not necessary -- sales.columns = master_columns
    boh.columns = master_columns
    receipt.columns = master_columns
    inv=boh + receipt
    return calcPen(sales) + ((calcPen(sales) - calcPen(inv)) * float(salesPenetrationThreshold))

def spCalc(metric,master_columns):
    # storing input sales data in an array
    # finding sales penetration for each brand in given stores
    # calculate adjusted penetration
    metric.columns = master_columns
    return calcPen(metric)

def metric_per_fixture(metric1,metric2,salesPenetrationThreshold,master_columns,newSpace):
    # storing input sales data in an array
        # finding penetration for each brand in given stores
        # calculate adjusted penetration
    metric1.columns = master_columns
    # metric2.columns = master_columns
    spacePen = metric2.div(newSpace,axis='index')
    return calcPen(metric1) + ((calcPen(metric1) - spacePen) * float(salesPenetrationThreshold))

def metric_per_metric(metric1,metric2,salesPenetrationThreshold,master_columns):
    # storing input sales data in an array
        # finding penetration for each brand in given stores
        # calculate adjusted penetration
    metric1.columns = master_columns
    metric2.columns = master_columns
    return calcPen(metric1) + ((calcPen(metric1) - calcPen(metric2)) * float(salesPenetrationThreshold))

def invTurn_Calc(sold_units,boh_units,receipts_units,master_columns):
    sold_units.columns = master_columns
    boh_units.columns = master_columns
    receipts_units.columns = master_columns
    calcPen(sold_units)
    calcPen(boh_units+receipts_units)
    inv_turn = calcPen(sold_units).div(calcPen(boh_units+receipts_units),axis='index')
    inv_turn[np.isnan(inv_turn)] = 0
    inv_turn[np.isinf(inv_turn)] = 0
    return calcPen(inv_turn)
    
def roundArray(array,increment):
    rounded=np.copy(array)
    for i in range(len(array)):
        for j in range(len(list(array[0,:]))):
            if np.mod(np.around(array[i][j], 0), increment) > increment/2:
                rounded[i][j] = np.around(array[i][j], 0) + (increment-(np.mod(np.around(array[i][j], 0), increment)))
            else:         
                rounded[i][j] = np.around(array[i][j], 0) - np.mod(np.around(array[i][j], 0), increment)
    return rounded

def roundDF(array,increment):
    rounded = array.copy(True)
    for i in array.index:
        for j in array.columns:
            if np.mod(np.around(array[j].loc[i], 3), increment) > increment/2:
                rounded[j].loc[i] = np.around(array[j].loc[i], 3) + (increment-(np.mod(np.around(array[j].loc[i], 3), increment)))
            else:         
                rounded[j].loc[i] = np.around(array[j].loc[i], 3) - np.mod(np.around(array[j].loc[i], 3), increment)
    return rounded

def futureSpace(bfc,futureFixt,Stores):
    futureFixt=futureFixt.drop(futureFixt.index[[0]])
    futureFixt=futureFixt.drop(futureFixt.columns[[0,1]],axis=1)
    futureSpace=pd.Series(0,futureFixt.index)
    for (i,Store) in enumerate(Stores):
        if pd.to_numeric(futureFixt['Future Space'].iloc[i]) == 0 or pd.isnull(pd.to_numeric(futureFixt['Future Space'].iloc[i])):
            futureFixt['Future Space'].iloc[i] = bfc.sum(axis=1).iloc[i]
    futureSpace=pd.to_numeric(futureFixt['Future Space'])-pd.to_numeric(futureFixt['Entry Space'])
    return futureSpace
    # return futureFixt['New_Space']

def brandExitSpace(spaceData,brandExit,Stores,Categories):
    # brandExit.index.apply(lambda x: if(brandExit[Category].iloc[x]==1: spaceData[Category].iloc[x]=0))
    # newSpace=pd.DataFrame(index=Stores,columns=Categories)
    for (i,Store) in enumerate(Stores):
        for (j,Category) in enumerate(Categories):
            if brandExit[Category][Store] == 1:
                spaceData[Category][Store] = 0
                # transaction.iloc[i]=pd.Series(0,np.arange(9))
    return spaceData

def brandExitTransac(Transactions,brandExit,Stores,Categories):
    Transactions.columns=Categories
    for (i,Store) in enumerate(Stores):
        for (j,Category) in enumerate(Categories):
            if brandExit[Category][Store] == 1:
                Transactions[Category].loc[Store]=0
    return Transactions

def preoptimize(Stores,Categories,spaceData,data,metricAdjustment,salesPenetrationThreshold,optimizedMetrics,increment,newSpace=None,brandExitArtifact=None):
    fixture_data=spaceData.drop(spaceData.columns[[0,1]],axis=1)
    # spaceData.drop(spaceData.columns[[0,1]],axis=1,inplace=True) 
    # fixture_data.drop(fixture_data.columns[[0,1]],axis=1,inplace=True) # Access Columns dynamically
    bfc = fixture_data[[ *np.arange(len(fixture_data.columns))[0::1] ]].convert_objects(convert_numeric=True)
    if newSpace is None:
        newSpace=bfc.sum(axis=1)
        print("We don't have futureSpace in preoptimize.")
    else:
        print("We have futureSpace in preoptimize!")
        newSpace=futureSpace(bfc,newSpace,Stores)
        print("Result of Future Space Function")

    
    if brandExitArtifact is not None:
        print("We have brandExitArtifact in preoptimize!")    
        fixture_data=brandExitSpace(fixture_data,brandExitArtifact,Stores,Categories)
        sales = brandExitTransac(data[[ *np.arange(len(data.columns))[0::9] ]].convert_objects(convert_numeric=True),brandExitArtifact,Stores,Categories)
        boh = brandExitTransac(data[[ *np.arange(len(data.columns))[1::9] ]].convert_objects(convert_numeric=True),brandExitArtifact,Stores,Categories)
        receipt = brandExitTransac(data[[ *np.arange(len(data.columns))[2::9] ]].convert_objects(convert_numeric=True),brandExitArtifact,Stores,Categories)
        sold_units = brandExitTransac(data[[ *np.arange(len(data.columns))[3::9] ]].convert_objects(convert_numeric=True),brandExitArtifact,Stores,Categories)
        boh_units = brandExitTransac(data[[ *np.arange(len(data.columns))[4::9] ]].convert_objects(convert_numeric=True),brandExitArtifact,Stores,Categories)
        receipts_units = brandExitTransac(data[[ *np.arange(len(data.columns))[5::9] ]].convert_objects(convert_numeric=True),brandExitArtifact,Stores,Categories)
        profit = brandExitTransac(data[[ *np.arange(len(data.columns))[6::9] ]].convert_objects(convert_numeric=True),brandExitArtifact,Stores,Categories)
        gm_perc = brandExitTransac(data[[ *np.arange(len(data.columns))[7::9] ]].convert_objects(convert_numeric=True),brandExitArtifact,Stores,Categories)
    else:
        sales = data[[ *np.arange(len(data.columns))[0::9] ]].convert_objects(convert_numeric=True)
        boh = data[[ *np.arange(len(data.columns))[1::9] ]].convert_objects(convert_numeric=True)
        receipt = data[[ *np.arange(len(data.columns))[2::9] ]].convert_objects(convert_numeric=True)
        sold_units = data[[ *np.arange(len(data.columns))[3::9] ]].convert_objects(convert_numeric=True)
        boh_units = data[[ *np.arange(len(data.columns))[4::9] ]].convert_objects(convert_numeric=True)
        receipts_units = data[[ *np.arange(len(data.columns))[5::9] ]].convert_objects(convert_numeric=True)
        profit = data[[ *np.arange(len(data.columns))[6::9] ]].convert_objects(convert_numeric=True)
        gm_perc = data[[ *np.arange(len(data.columns))[7::9] ]].convert_objects(convert_numeric=True)
        print("We don't have brandExitArtifact in preoptimize")        

    salesPenetrationThreshold=float(salesPenetrationThreshold)
    adj_p = int(optimizedMetrics['spread'])*spreadCalc(sales,boh,receipt,getColumns(data),salesPenetrationThreshold) + int(optimizedMetrics['salesPenetration'])*spCalc(sales,getColumns(data)) + int(optimizedMetrics['salesPerSpaceUnit'])*metric_per_fixture(sales,bfc,salesPenetrationThreshold,getColumns(data),newSpace) + int(optimizedMetrics['grossMargin'])*spCalc(gm_perc,getColumns(data)) + int(optimizedMetrics['inventoryTurns'])*invTurn_Calc(sold_units,boh_units,receipts_units,getColumns(data))
    

    # adj_p.fillna(np.float(0))
    # adj_p[np.isnan(adj_p)] = 0
    # adj_p.where(adj_p < metricAdjustment, 0, inplace=True)
    for i in adj_p.index:
        for j in adj_p.columns:
            if adj_p[j].loc[i] < metricAdjustment:
                adj_p[j].loc[i] = 0
    adj_p=calcPen(adj_p)
    adj_p.fillna(0)    
    # adj_p[np.isnan(adj_p)] = 0
        
    #Create Code to make adjustments to adj_p
    opt_amt = roundDF(adj_p.multiply(newSpace,axis='index'),increment)
    return (adj_p,opt_amt)

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
    lOutput.set_index("Store")
    # lOutput.to_excel('outputs.xlsx',sheet_name='Long_Table')
    return lOutput

def createWide(Stores,Categories,Levels,st,Results,Optimal,Penetration,Historical):
    bfc = Historical[[ *np.arange(len(Historical.columns))[0::1] ]].convert_objects(convert_numeric=True)
    storeDict=Historical[[0,1]]#.T.to_dict()
    # storeDict["Store"]=storeDict.index
    Historical=Historical.drop(Historical.columns[[0,1]],axis=1)
    Optimal.columns = [str(col) + '_optimal' for col in Categories]
    Penetration.columns = [str(col) + '_penetration' for col in Categories]
    Results.columns = [str(col) + '_result' for col in Categories]
    Historical.columns = [str(col) + '_current' for col in Historical.columns]
    sumOutput=pd.DataFrame(index=Stores,columns=['Total_result','Total_current'])
    sumOutput['Total_result']=Results.sum(axis=1)
    sumOutput['Total_current']=bfc.sum(axis=1)
    wOutput=pd.concat([storeDict,Results,Historical,Optimal,sumOutput,Penetration],axis=1) #Results.append([Optimal,Penetration,Historical])
    # wOutput.to_excel('outputs.xlsx',sheet_name='Wide_Table')
    return wOutput

def createTieredSummary(longTable) :
    #pivot the long table to create a data frame providing the store count for each Category-ResultSpace by Climate along with the total for all climates
    tieredSummaryPivot = pd.pivot_table(longTable, index=['Category', 'Result Space'], columns='Climate', values='Store', aggfunc=len, margins=True)
    #rename the total for all climates column
    tieredSummaryPivot.rename(columns = {'All':'Total Store Count'}, inplace = True)
    #delete the last row of the pivot, as it is a sum of all the values in the column and has no business value in this context
    tieredSummaryPivot = tieredSummaryPivot.ix[:-1]
    # tieredSummaryPivot.to_excel('outputs.xlsx',sheet_name='Summary_Table')
    return tieredSummaryPivot


def optimize(optName,preOpt,tierCounts,spaceBound,increment,spaceArtifact,brandExitArtifact=None):
    """
    Run an LP-based optimization

    Side-effects:
        - creates file: Fixture_Optimization.lp (constraints)
        - creates file: solvedout.csv <= to be inserted into db
        - creates file: solvedout.text

    Synopsis:
        I just wrapped the script from Ken in a callable - DCE
    """
    start_seconds = dt.datetime.today().hour*60*60+ dt.datetime.today().minute*60 + dt.datetime.today().second
    penetration=preOpt[0]
    opt_amt=preOpt[1]

    print("HEY I'M IN THE OPTIMIZATION!!!!!!!")
    print("Hey it's Space Bounds")
    print(spaceBound)
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
    if 0.0 not in Levels:
        Levels.append(np.abs(0.0))
    b = .05
    bI = .1

    # Create a Vectors & Arrays of required variables
    # Calculate Total fixtures(TotFixt) per store by summing up the individual fixture counts
    W = opt_amt.sum(axis=1).sum(axis=0)
    TFC = opt_amt.sum(axis=1)
    ct = LpVariable.dicts('CT', (Categories, Levels), 0, upBound=1,
                        cat='Binary')
    st = LpVariable.dicts('ST', (Stores, Categories, Levels), 0,
                        upBound=1, cat='Binary')

    NewOptim = LpProblem("FixtureOptim", LpMinimize)  # Define Optimization Problem/

    # Brand Exit Enhancement
    if brandExitArtifact is None:
        print("No Brand Exit in the Optimization")
    else:
        for (i, Store) in enumerate(Stores):
            for (j, Category) in enumerate(Categories):
                if (brandExitArtifact[Category][Store] != 0):
                    # upper_bound[Category].loc[Store] = 0
                    # lower_bound[Category].loc[Store] = 0
                    opt_amt[Category][Store] = 0
                    NewOptim += st[Store][Category][0.0] == 1
                    NewOptim += ct[Category][0.0] == 1
                    spaceBound[Category][0] = 0

        # for (j, Category) in enumerate(Categories):
        #     if (sum(brandExitArtifact[Category].values()) > 0):
        #         tier_count["Upper_Bound"][Category] += 1

    BA = np.zeros((len(Stores), len(Categories), len(Levels)))
    error = np.zeros((len(Stores), len(Categories), len(Levels)))
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                BA[i][j][k] = opt_amt[Category].iloc[i]
                error[i][j][k] = np.absolute(BA[i][j][k] - Level)

    NewOptim += lpSum([(st[Store][Category][Level] * error[i][j][k]) for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for (k, Level) in enumerate(Levels)]), ""

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

#One Space per Store Category
    #Makes sure that the number of fixtures, by store, does not go above or below some percentage of the total number of fixtures within the store 
        for (j,Category) in enumerate(Categories):
            NewOptim += lpSum([st[Store][Category][Level] for (k,Level) in enumerate(Levels)]) == 1#, "One_Level_per_Store-Category_Combination"
        # Test Again to check if better performance when done on ct level
            NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) <= spaceBound[Category]['Space_Max']         
            if brandExitArtifact is not None:
                if brandExitArtifact[Category].iloc[int(i)] == 0:
                    NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound[Category]['Space_Min'] + increment
                else:
                    NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound[Category]['Space_Min']
            else:
                NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)]) >= spaceBound[Category]['Space_Min']
            
#Store Category Level Bounding
        #NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)] ) >= lower_bound[Category][Store]#,
        #NewOptim += lpSum([st[Store][Category][Level] * Level for (k,Level) in enumerate(Levels)] ) <= upper_bound[Category][Store]#,

#Tier Counts Enhancement
    totalTiers=0
    for (j,Category) in enumerate(Categories):
        totalTiers=totalTiers+tierCounts[Category]['Tier_Count_Max']
        NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) >= tierCounts[Category]['Tier_Count_Min'] #, "Number_of_Tiers_per_Category"
        NewOptim += lpSum([ct[Category][Level] for (k,Level) in enumerate(Levels)]) <= tierCounts[Category]['Tier_Count_Max']
#Relationship between Selected Tiers & Created Tiers
    #Verify that we still cannot use a constraint if not using a sum - Look to improve efficiency   
        for (k,Level) in enumerate(Levels):
            NewOptim += lpSum([st[Store][Category][Level] for (i,Store) in enumerate(Stores)])/len(Stores) <= ct[Category][Level]#, "Relationship between ct & st"

    print("totalTiers")
    print(totalTiers)
    NewOptim += lpSum([ct[Category][Level] for (j,Category) in enumerate(Categories) for (k,Level) in enumerate(Levels)]) <= totalTiers #len(Categories)*sum(tier_count[Category][1].values())

#Global Balance Back  
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
        (k, Level) in enumerate(Levels)]) >= W * (1 - b)
    NewOptim += lpSum(
        [st[Store][Category][Level] * Level for (i, Store) in enumerate(Stores) for (j, Category) in enumerate(Categories) for
        (k, Level) in enumerate(Levels)]) <= W * (1 + b)
    # NewOptim.writeLP("Fixture_Optimization.lp")
    # LpSolverDefault.msg = 1
    print("The problem has been formulated")

#Solving the Problem    
    # NewOptim.msg=1
    NewOptim.solve(pulp.PULP_CBC_CMD(msg=1))
    # NewOptim.solve()    
    # NewOptim.solve(pulp.COIN_CMD(msg=1))
    
#Debugging
    # print("#####################################################################")
    # print(LpStatus[NewOptim.status])
    # print("#####################################################################")
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

#Create Outputs
    longTable= createLong(Stores,Categories,Levels,st,preOpt[1],preOpt[0],spaceArtifact)
    # long_id = longOutput[0]
    wideTable = createWide(Stores,Categories,Levels,st,Results,preOpt[1],preOpt[0],spaceArtifact)
    summaryTable = createTieredSummary(longTable)

    writer = pd.ExcelWriter(str(optName)+"-Output_"+str(time.strftime("%Y-%m-%d_%H-%M"))+".xlsx")
    longTable.to_excel(writer, 'Long Table',index=False)
    wideTable.to_excel(writer, 'Wide Table')
    summaryTable.to_excel(writer, 'Summary Table')
    writer.save()

    end_seconds = dt.datetime.today().hour*60*60 + dt.datetime.today().minute*60 + dt.datetime.today().second 
    total_seconds= end_seconds - start_seconds
    print("Total number of seconds taken is:" + str(total_seconds))
    return LpStatus[NewOptim.status]#(longOutput,wideOutput)

def foFunction(optName,fileName,brandExitFlag,futureSpaceFlag):
    # fileName="ScriptTestData.xlsx"
    startTime=dt.datetime.today()
    print("Job Started at: "+str(startTime))
    fixtureArtifact=pd.read_excel(fileName,sheetname='Space_Data')
    transactionArtifact=pd.read_excel(fileName,sheetname='Transaction_Data')
    transactionArtifact=transactionArtifact.drop(transactionArtifact.index[[0]]).set_index("Store")
    fixtureArtifact=fixtureArtifact.drop(fixtureArtifact.index[[0]]).set_index("Store")
    Stores=fixtureArtifact.index.values.astype(int)
    Categories=fixtureArtifact.columns[2:].values
    spaceBounds=pd.read_excel(fileName,sheetname='Space_Bounds').set_index("Space").to_dict()
    inputs=pd.read_excel(fileName,sheetname='Values').set_index('Input').to_dict()
    metrics=pd.read_excel(fileName,sheetname='Weighted_Metrics').set_index('Metric').to_dict()
    tierCounts=pd.read_excel(fileName,sheetname='Tier_Counts').set_index('Tier_Value').to_dict()
    if futureSpaceFlag == 1:
        try:
            futureSpace=pd.read_excel(fileName,sheetname=Future_Space).set_index("Store")
            print("Future Space was Uploaded")
        except:
            futureSpace=None
            print("Future Space was not Uploaded")
    else:
        futureSpace=None
        print("Future Space was not Uploaded")
    if brandExitFlag == 1:
        try:
            brandExitArtifact=pd.read_excel(fileName,sheetname=Brand_Exit)
            print("Brand Exit was Uploaded")
            brandExitArtifact=brandExitMung(brandExitArtifact,Stores,Categories)
            print("Brand Exit Munged")
        except:
            print("Brand Exit was not Uploaded")
            brandExitArtifact=None
    else:
        print("Brand Exit was not Uploaded")
        brandExitArtifact=None
    preOpt = preoptimize(Stores=Stores,Categories=Categories,spaceData=fixtureArtifact,data=transactionArtifact,metricAdjustment=float(inputs['Value']["Metric_Adjustment"]),salesPenetrationThreshold=float(inputs['Value']["Sales_Penetration_Threshold"]),optimizedMetrics=metrics['Weight'],increment=inputs['Value']["Increment"],brandExitArtifact=brandExitArtifact,newSpace=futureSpace)
    optimStatus=optimize(optName=optName,preOpt=preOpt,tierCounts=tierCounts,spaceBound=spaceBounds,increment=inputs['Value']["Increment"],spaceArtifact=fixtureArtifact,brandExitArtifact=brandExitArtifact)
    endTime=dt.datetime.today()
    # print("\n\nJob Ended at: \n"+str(endTime))
    print("The status of the job was: "+str(optimStatus))
    print("Total Time Taken for Job (Hours:Minutes::Seconds:Milliseconds): "+str(endTime-startTime))
    # str((max(max(spaceBounds))-min(min(spaceBounds)))/inputs['Value']['Increment'])
    print("This optimization had " + str(len(Stores)) + " stores and " + str(len(Categories)) + " categories.")

    return

foFunction(optName,file,0,0)

def mvpFO():
    file="ScriptTestData.xlsx" 
    foFunction(file,0,0)
    return

if __name__ == '__main__':
    print("Process Completed")