# -*- coding: utf-8 -*-
"""
Created on Thu July 4 2016

@author: hardik.sanchawat
"""

import pandas as pd
from pandas import DataFrame
import numpy as np

def brandExitMung(df,Stores,Categories):
    # df.columns = df.iloc[0].values
    # df.drop(df.index[[0, 1]], axis=0, inplace=True)
    # df=df.reset_index(drop=True)
    brand_exit = pd.DataFrame(index=Stores,columns=Categories)
    for (i,Store) in enumerate(Stores):
        for (j,Category) in enumerate(Categories):
            if Store in pd.unique(df[Category].values):
                brand_exit[Category].iloc[i] = 1
            else:
                brand_exit[Category].iloc[i] = 0
    return brand_exit