# -*- coding: utf-8 -*-
"""
Created on Thu Jun 30 09:55:06 2016

@author: kenneth.l.sylvain
"""
import numpy as np
import pandas as pd

# Need to change functions to allow for TFC as result of Future Space/ Brand Entry
# Need to create a max & min for category level informaiton to be passed as bounds in a long table format // Matching format to Bounding Info that is added to job context

# import os

def calcPen(metric):
    return metric.div(metric.sum(axis=1),axis='index')
    
def getColumns(df):
    # print(df[[ *np.arange(len(df.columns))[0::9] ]].drop(df.index[[0]]).convert_objects(convert_numeric=True).columns)
    return df[[ *np.arange(len(df.columns))[0::9] ]].drop(df.index[[0]]).convert_objects(convert_numeric=True).columns

# def calcPen(metric,master_columns):
    # metric.columns = master_columns    
#     return metric.div(metric.sum(axis=1),axis='index')

def spreadCalc(sales,boh,receipt,master_columns,mAdjustment):
     # storing input sales and inventory data in separate 2D arrays
	 # finding sales penetration, GAFS and spread for each brand in given stores
	 # calculate adjusted penetration
     #Not necessary -- sales.columns = master_columns
     boh.columns = master_columns
     receipt.columns = master_columns
     inv=boh + receipt
     return calcPen(sales) + ((calcPen(sales) - calcPen(inv)) * float(mAdjustment))

def spCalc(metric,master_columns):
    # storing input sales data in an array
    # finding sales penetration for each brand in given stores
    # calculate adjusted penetration
    metric.columns = master_columns
    return calcPen(metric)

def metric_per_fixture(metric1,metric2,mAdjustment,master_columns,newSpace):
    # storing input sales data in an array
		# finding penetration for each brand in given stores
		# calculate adjusted penetration
    metric1.columns = master_columns
    # metric2.columns = master_columns
    spacePen = metric2.div(newSpace,axis='index')
    return calcPen(metric1) + ((calcPen(metric1) - spacePen) * float(mAdjustment))

def metric_per_metric(metric1,metric2,mAdjustment,master_columns):
    # storing input sales data in an array
		# finding penetration for each brand in given stores
		# calculate adjusted penetration
    metric1.columns = master_columns
    metric2.columns = master_columns
    return calcPen(metric1) + ((calcPen(metric1) - calcPen(metric2)) * float(mAdjustment))

def invTurn_Calc(sold_units,boh_units,receipts_units,master_columns):
    sold_units.columns = master_columns
    boh_units.columns = master_columns
    receipts_units.columns = master_columns
    soldPen = calcPen(sold_units)
    gafsPen = calcPen(boh_units+receipts_units)
    inv_turn = soldPen/gafsPen
    # inv_turn = calcPen(sold_units).div((calcPen(boh_units+receipts_units).sum(axis=1)),axis='index')
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

def futureSpace(futureFixt,bfc,Stores):
    futureFixt=futureFixt.drop(futureFixt.index[[0]])
    futureFixt=futureFixt.drop(futureFixt.columns[[0,1]],axis=1)
    futureSpace=pd.Series(0,futureFixt.index)
    for (i,Store) in enumerate(Stores):
        if pd.isnull(futureFixt['Future Space'].iloc[i]): #pd.to_numeric(futureFixt['Future Space'].iloc[i]) == 0 or 
            futureFixt['Future Space'].iloc[i] = bfc.sum(axis=1).iloc[i]
    futureFixt['Entry Space']=pd.to_numeric(futureFixt['Entry Space']).fillna(0)
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

def preoptimize(Stores,Categories,spaceData,data,salesPenThreshold,mAdjustment,optimizedMetrics,increment,newSpace=None,brandExitArtifact=None):
    fixture_data=spaceData.drop(spaceData.columns[[0,1]],axis=1)
    # spaceData.drop(spaceData.columns[[0,1]],axis=1,inplace=True)
    # fixture_data.drop(fixture_data.columns[[0,1]],axis=1,inplace=True) # Access Columns dynamically
    #TODO Verify that this works correctly after changes in what is commented anch
    # Was previously not commented, see other changes
    # bfc = fixture_data[[ *np.arange(len(fixture_data.columns))[0::1] ]].convert_objects(convert_numeric=True)
    if brandExitArtifact is None:
        print("We don't have brandExitArtifact in preoptimize")
        #### New, may or may not work correctly after switching the order
        bfc = fixture_data[[*np.arange(len(fixture_data.columns))[0::1]]].convert_objects(convert_numeric=True)
        #### bfc was previously not recreated for Brand Exit... should verify if this is an issue

        sales = data[[ *np.arange(len(data.columns))[0::9] ]].convert_objects(convert_numeric=True)
        boh = data[[ *np.arange(len(data.columns))[1::9] ]].convert_objects(convert_numeric=True)
        receipt = data[[ *np.arange(len(data.columns))[2::9] ]].convert_objects(convert_numeric=True)
        sold_units = data[[ *np.arange(len(data.columns))[3::9] ]].convert_objects(convert_numeric=True)
        boh_units = data[[ *np.arange(len(data.columns))[4::9] ]].convert_objects(convert_numeric=True)
        receipts_units = data[[ *np.arange(len(data.columns))[5::9] ]].convert_objects(convert_numeric=True)
        profit = data[[ *np.arange(len(data.columns))[6::9] ]].convert_objects(convert_numeric=True)
        gm_perc = data[[ *np.arange(len(data.columns))[7::9] ]].convert_objects(convert_numeric=True)
    else:
        print("We have brandExitArtifact in preoptimize!")
        fixture_data = brandExitSpace(fixture_data, brandExitArtifact, Stores, Categories)
        #### New, may or may not work correctly after switching the order
        bfc=fixture_data[[ *np.arange(len(fixture_data.columns))[0::1] ]].convert_objects(convert_numeric=True)
        #### bfc was previously not recreated for Brand Exit... should verify if this is an issue
        sales = brandExitTransac(data[[*np.arange(len(data.columns))[0::9]]].convert_objects(convert_numeric=True),
                                 brandExitArtifact, Stores, Categories)
        boh = brandExitTransac(data[[*np.arange(len(data.columns))[1::9]]].convert_objects(convert_numeric=True),
                               brandExitArtifact, Stores, Categories)
        receipt = brandExitTransac(data[[*np.arange(len(data.columns))[2::9]]].convert_objects(convert_numeric=True),
                                   brandExitArtifact, Stores, Categories)
        sold_units = brandExitTransac(data[[*np.arange(len(data.columns))[3::9]]].convert_objects(convert_numeric=True),
                                      brandExitArtifact, Stores, Categories)
        boh_units = brandExitTransac(data[[*np.arange(len(data.columns))[4::9]]].convert_objects(convert_numeric=True),
                                     brandExitArtifact, Stores, Categories)
        receipts_units = brandExitTransac(data[[*np.arange(len(data.columns))[5::9]]].convert_objects(convert_numeric=True),
                                          brandExitArtifact, Stores, Categories)
        profit = brandExitTransac(data[[*np.arange(len(data.columns))[6::9]]].convert_objects(convert_numeric=True),
                                  brandExitArtifact, Stores, Categories)
        gm_perc = brandExitTransac(data[[*np.arange(len(data.columns))[7::9]]].convert_objects(convert_numeric=True),
                                   brandExitArtifact, Stores, Categories)

    if newSpace is None:
        newSpace=bfc.sum(axis=1)
        print("We don't have futureSpace in preoptimize.")
    else:
        print("We have futureSpace in preoptimize!")
        newSpace=futureSpace(newSpace,bfc,Stores)
        # print("Result of Future Space Function")
        # print(newSpace)

    mAdjustment=float(mAdjustment)
    adj_p = int(optimizedMetrics['spread'])*spreadCalc(sales,boh,receipt,getColumns(data),mAdjustment) + int(optimizedMetrics['salesPenetration'])*spCalc(sales,getColumns(data)) + int(optimizedMetrics['salesPerSpaceUnit'])*metric_per_fixture(sales,bfc,mAdjustment,getColumns(data),newSpace) + int(optimizedMetrics['grossMargin'])*spCalc(gm_perc,getColumns(data)) + int(optimizedMetrics['inventoryTurns'])*invTurn_Calc(sold_units,boh_units,receipts_units,getColumns(data))
    

    # adj_p.fillna(np.float(0))
    # adj_p[np.isnan(adj_p)] = 0
    # adj_p.where(adj_p < salesPenThreshold, 0, inplace=True)
    for i in adj_p.index:
        for j in adj_p.columns:
            if adj_p[j].loc[i] < salesPenThreshold:
                adj_p[j].loc[i] = 0
    adj_p=calcPen(adj_p)
    adj_p.fillna(0)    
    # adj_p[np.isnan(adj_p)] = 0
        
    #Create Code to make adjustments to adj_p
    # opt_amt = roundDF(adj_p.multiply(newSpace,axis='index'),increment)
    opt_amt = adj_p.multiply(newSpace,axis='index')   
    return (adj_p,opt_amt)

'''
#For Testing 
salesPenThreshold=0
mAdjustment=0
metric=6
increment=.25
adj_p = metric_creation(transaction_data, bfc,salesPenThreshold,mAdjustment,metric,increment)
adj_p.head()
'''
#opt_amt=adj_p.multiply(bfc.sum(axis=1),axis='index').as_matrix()