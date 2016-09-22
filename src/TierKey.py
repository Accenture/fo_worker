# -*- coding: utf-8 -*-
"""
Created on Thu July 26 2016
@Definition : Tier Determination Code
@author: hardik.sanchawat

Sample Output

Category_Name	Tier_Value	Space_Value
ADIDAS&PUMA	    Tier_2	    2.5
"""

import pandas as pd
#import numpy as np

# df = pd.read_csv('C:\\Users\\kenneth.l.sylvain\\Documents\\Kohls\\Fixture Optimization\\Rpy2\\FOT\\FOT\\RecordTest_600.csv')
def tierKeyCreate(df):
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
    #dataFrame.to_csv('C:\\Users\\tkmae0v\\Desktop\\FOT\\TierKey\\RecordTest_output_latest.csv',index=None)
    #print(dataFrame)
    return dataFrame
# dataFrame=tierKeyCreate(df)