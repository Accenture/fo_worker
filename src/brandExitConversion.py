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
# def brandExitMung(df,Stores,Categories):
#     print("I'm in brandExitMung'")
#     print("Old Brand Exit")
#     print(df)
#     # brand_exit = pd.DataFrame(index=np.unique([var for var in df.values.flatten() if var]),columns=df.columns.values)
#     brand_exit = pd.DataFrame(index=Stores,columns=Categories)    
#     for k in range(len(df)):
#         for (j,Category) in enumerate(Categories):
#             for (i,Store) in enumerate(Stores):
#                 if (df[Category][k] == Store):
#                     brand_exit[Category].loc[Store] = 1
#                 else:
#                     brand_exit[Category].loc[Store] = 0
#     print("newBrandExit")
#     print(brand_exit)
#     return brand_exit

# def brandExitMung(df,Stores,Categories):
#     df=df.drop(df.index[0])
#     df=df.reset_index(drop=True)
#     brand_exit = pd.DataFrame(index=Stores,columns=Categories)
#     for (i,Store) in enumerate(Stores):
#         for (j,Category) in enumerate(Categories):
#             if str(Store) in pd.unique(df[Category].values):
#                 brand_exit[Category].iloc[i] = 1
#             else:
#                 brand_exit[Category].iloc[i] = 0
#     return brand_exit

def brandExitMung(df,Stores,Categories):
    df.columns = df.iloc[0].values
    df.drop(df.index[[0, 1]], axis=0, inplace=True)
    df=df.reset_index(drop=True)
    brand_exit = pd.DataFrame(index=Stores,columns=Categories)
    for (i,Store) in enumerate(Stores):
        for (j,Category) in enumerate(Categories):
            if str(Store) in pd.unique(df[Category].values):
                brand_exit[Category].iloc[i] = 1
            else:
                brand_exit[Category].iloc[i] = 0
    return brand_exit