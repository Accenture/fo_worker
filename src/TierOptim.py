# -*- coding: utf-8 -*-
"""
Created on Thu July 26 2016
@Definition : Tier Determination Code
@author: hardik.sanchawat

Sample Output

Store	ADIDAS&PUMA	    ASICS	FILA USA
7	    Tier 2	        Tier 2	Tier 1
8	    Tier 1	        Tier 3	Tier 2
9	    Tier 1	        Tier 3	Tier 1S
10	    Tier 3	        Tier 3	Tier 1
"""
import pandas as pd
import numpy as np

df = pd.read_csv('C:\\Users\\kenneth.l.sylvain\\Documents\\Kohls\\Fixture Optimization\\Rpy2\\FOT\\FOT\\RecordTest_600.csv',header=0)

#print(df)



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
    #df.to_csv('C:\\Users\\tkmae0v\\Desktop\\FOT\\TierOptim\\RecordTest_output_new.csv', index=False)
                   
                   
    
    #print(df)
df=tierDef(df)
