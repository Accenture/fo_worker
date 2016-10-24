import pandas as pd
import numpy as np


#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Tue Sep  20 2016

@author: hardik sanchawat
"""

import pandas as pd
import numpy as np


def approximateSpaceData(transactions, total_space, total_space_id, boh_weight, time, month_qtr_mapping):
    # TODO: overwrite negatives with 0's
    transactions[transactions < 0] = 0

    # TODO: if quarterly, summarize monthly transactions by quarter using mapping, and use summarized data as the transactions data set for rest of function

    stores = list(transactions.index.levels[0].astype(int))
    times = list(transactions.index.levels[1].astype(int))
    categories = [c for c in list(transactions.columns.levels[0]) if c[:7] != 'Unnamed']

    #create data frames containing store & time as index, with just boh units or cc count as the columns

    # TODO: select columns from transactions where second level index = boh units
    boh_units = transactions[[*np.arange(len(transactions.columns))[4::9]]].convert_objects(convert_numeric=True)

    # TODO: populate first level index with category list (how to ensure order of category list matches original column order?)
    # TODO: transform data so each cell contains its previous value divided by the sum of all values in the row
    boh_norm = boh_units.div(boh_units.sum(axis=1), axis=0)
    boh_norm.columns = categories

    # TODO: select columns from transactions where second level index = cc count
    cc_count = transactions[[*np.arange(len(transactions.columns))[8::9]]].convert_objects(convert_numeric=True)

    # TODO: populate first level index with category list (how to ensure order of category list matches original column order?)
    # TODO: transform data so each cell contains its previous value divided by the sum of all values in the row
    cc_norm = cc_count.div(cc_count.sum(axis=1), axis=0)
    cc_norm.columns = categories

    # Create a new data frame for final_result
    cols = categories
    cols.insert(0,"Climate")
    cols.insert(1,"VSG")
    final_result = pd.DataFrame(index = transactions.index.copy(),columns=cols)
    cat = categories
    del cat[0]
    del cat[0]

    # Populate final result data frame
    for s in stores:
        for t in times:
            for c in cat:
                final_result["Climate"].loc[s, t] = total_space["Climate"].loc[s]
                final_result["VSG"].loc[s, t] = total_space["VSG"].loc[s]
                final_result[c].loc[s, t] = np.around((boh_weight * boh_norm[c].loc[s, t] + (1 - boh_weight) * cc_norm[c].loc[s, t]) * total_space[total_space_id].loc[s], decimals=1)

    #Format for printing to csv
    final_result = final_result.reset_index()
    final_result.rename(columns = {'level_0':'Store','level_1':'Category'}, inplace = True)

    # TODO: insert row and populate that row in space columns with "Current Space")
    final_result.to_csv("C:\\Users\\hardik.sanchawat\\Documents\\Optimize Code Updates P4\\Historical Space Data Approximation\\Monthly_Space_Data_Report_%s.csv"% boh_weight,
              index=None)
    return final_result


filepath = "C:\\Users\\hardik.sanchawat\\Documents\\Optimize Code Updates P4\\Historical Space Data Approximation\\"
total_space_data_test = pd.read_csv(filepath+"MISSES APT 9_SP16 Space.csv", header=0, skiprows=[1],index_col=[0])
transaction_data_test = pd.read_csv(filepath+"MISSES CLASSIFICATION APT 9_Monthly Sales Data.csv", header=[0,1],index_col=[0,1])
total_space_id_test = 'APT 9'
boh_weight_test = .75
time_test = 'monthly'

#TODO: initialize as standard fiscal quarter mapping (1:1,2:1,3:1,4:2,etc.)
month_qtr_mapping_test = None

returned = approximateSpaceData(transaction_data_test, total_space_data_test, total_space_id_test, boh_weight_test, time_test, month_qtr_mapping_test)
returned.to_csv(filepath+"Approximate Space Data Output.csv",index=False)

import pandas as pd

df = pd.read_csv('C:\\Users\\hardik.sanchawat\\Documents\\Drill Down Filtering Code\\DD_Time Loop_Input.csv')

def drilldownTierClimateLoop(df):
    df2 = sorted(df.loc[:, 'Climate'].unique().tolist())
    df3 = sorted(df.loc[:, 'Future Space'].unique().tolist())
    for d2 in df2:
        for d3 in df3:
            df5 = df[(df['Climate'] == d2) & (df['Future Space'] == d3)]
            del df5['Time']
            df5.to_csv('C:\\Users\\hardik.sanchawat\\Documents\\Drill Down Filtering Code\\Drill Down Time Loop_Output_%s-%s.csv' % (d2,d3),index=None)
drilldownTierClimateLoop(df)

import pandas as pd

df = pd.read_csv('C:\\Users\\hardik.sanchawat\\Documents\\Drill Down Filtering Code\\DD_Time Loop_Input_Dup.csv')

def drilldownTimeLoop(df):
    df1 = sorted(df.loc[:, 'Time'].unique().tolist())
    for d1 in df1:
        df2 = (df[df['Time'] == d1])
        del df2['Time']
        df2.to_csv('C:\\Users\\hardik.sanchawat\\Documents\\Drill Down Filtering Code\\Drill Down Time Loop_Output_%s.csv' % d1,index=None)
drilldownTimeLoop(df)

def tierKeyCreate(df):
    '''
    tierKeyCreate(dataframe)

    Takes in a dataframe and creates the key associated with the tiered results 
    ''''
    df.pivot
    headers = iter(df.columns.values) #grabs all the brand headers from the csv
    next(headers) #Skip first column
    dataFrame = df.drop(df.columns.values, axis=1) #creates the empty dataframe
    # Inserted new columns in dataframe
    dataFrame.insert(0, 'Category_Name', '', allow_duplicates=False)
    dataFrame.insert(1, 'Tier_Value', '', allow_duplicates=False)
    dataFrame.insert(2, 'Space_Value', '', allow_duplicates=False)
    num_row = 0
    for head in headers:
        #df1 = df.loc[:,head].unique() #Get each column values in Series
        #df1 = np.sort(df1).tolist()
        spaceVals = sorted(df.loc[:,head].unique().tolist()) #Get unique sorted value of that column
        #print(df2) 
        #for d1 in df1:
        for val in spaceVals:
                #if d1 == d2:
            dataFrame.loc[num_row, 'Category_Name'] = head
            dataFrame.loc[num_row, 'Tier_Value'] = "Tier {0}".format(str(spaceVals.index(val) + 1))
            dataFrame.loc[num_row, 'Space_Value'] = val
            num_row += 1
    return dataFrame

def tierDef(df):
    storeVals = [] #Hold Store Values
    for storeVal in df['Store']: 
        storeVals.append(storeVal)
    df = df.drop(['Store'], axis=1) #Drop Store Column
    for (d,column) in enumerate(df.columns):
        tierVals = df[column].unique() #Get each unique spaceValue
        tierVals = np.sort(tierVals).tolist() #Sorts Unique Space values Ascending Order
        spaceVals = df.loc[:,column].tolist() #All Space Values in each column to be assigned a tier
        #headers = df.columns.values #Get each column values in Series
        num_row = 0 #tracks df location 
        for spaceVal in spaceVals: #invididual value
            for tierVal in tierVals: #unique values per column
               if spaceVal == tierVal: 
                   df.loc[num_row, str(column)] = "Tier {0}".format(str(tierVals.index(tierVal) + 1),"") #assigns tier value
                   num_row += 1
    df.insert(0, 'Store', storeVals, allow_duplicates=False)
    return df    