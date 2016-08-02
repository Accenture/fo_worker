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

df = pd.read_csv('C:\\Users\\hardik.sanchawat\\Documents\\Tier Determination Code\\RecordTest_600.csv')
def tierKeyCreate(df):
    iterdf = iter(df.columns.values)
    next(iterdf) #Skip first column
    df3 = df.drop(df.columns.values, axis=1)
    # Inserted new columns in dataframe
    df3.insert(0, 'Category_Name', '', allow_duplicates=False)
    df3.insert(1, 'Tier_Value', '', allow_duplicates=False)
    df3.insert(2, 'Space_Value', '', allow_duplicates=False)
    num_row = 0
    for d in iterdf:
        df1 = df.loc[:,d] #Get each column values in Series
        df2 = sorted(df.loc[:,d].unique().tolist()) #Get unique sorted value of that column
        for d1 in df1:
            for d2 in df2:
                if d1 == d2:
                    df3.loc[num_row, 'Category_Name'] = d
                    df3.loc[num_row, 'Tier_Value'] = "Tier {0}".format(str(df2.index(d2) + 1))
                    df3.loc[num_row, 'Space_Value'] = d1
                    # df3.to_csv('C:\\Users\\hardik.sanchawat\\Documents\\Tier Determination Code\\RecordTest_output_latest.csv',index=None)
                    num_row += 1
    return df3
Tier(df)