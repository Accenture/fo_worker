# -*- coding: utf-8 -*-
"""
Created on Thu July 26 2016
@Definition : Tier Determination Code
@author: hardik.sanchawat

Sample Output

Store	ADIDAS&PUMA	    ASICS	FILA USA
7	    Tier 2	        Tier 2	Tier 1
8	    Tier 1	        Tier 3	Tier 2
9	    Tier 1	        Tier 3	Tier 1
10	    Tier 3	        Tier 3	Tier 1
"""

import pandas as pd

df = pd.read_csv('C:\\Users\\kenneth.l.sylvain\\Documents\\Kohls\\Fixture Optimization\\Full_Test\RecordTest_600.csv',header=0)

def tierDef(df):
    df = df.drop(['Store'], axis=1) #Drop first column
    for d in df.columns.values:
        df1 = df.loc[:,d] #Get each column values in Series
        df2 = sorted(df.loc[:, d].unique().tolist()) #Get unique sorted value of that column
        num_row = 0
        for d1 in df1:
            for d2 in df2:
                if d1 == d2:
                    df.loc[num_row, str(d)] = "Tier {0}: {1}".format(str(df2.index(d2) + 1),str(d1))
                    df.to_csv('C:\\Users\\hardik.sanchawat\\Documents\\Tier Determination Code\\RecordTest_output_new.csv',index=False)
                    num_row += 1
Tier(df)


