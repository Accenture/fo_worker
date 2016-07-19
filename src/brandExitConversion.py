# -*- coding: utf-8 -*-
"""
Created on Thu July 4 2016

@author: hardik.sanchawat
"""

import pandas as pd
from pandas import DataFrame
import numpy as np

#Stores = np.unique([var for var in df.values.flatten() if var])
#Categories = df.columns.values
def brandExitConversion(df):
    brand_exit = pd.DataFrame(index=np.unique([var for var in df.values.flatten() if var]),columns=df.columns.values)
    for k in range(len(df)):
        for (j,Category) in enumerate(Categories):
            for (i,Store) in enumerate(Stores):
                if (df[Category][k] == Store):
                    brand_exit[Category].loc[Store] = 1
                else:
                    brand_exit[Category].loc[Store] = 0
    return brand_exit