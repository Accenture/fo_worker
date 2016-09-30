# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 13:37:46 2016

@author: kenneth.l.sylvain
"""
import pandas as pd
df=pd.read_csv('C:\\Users\\kenneth.l.sylvain\\Documents\\Kohls\\Fixture Optimization\\Repos\\fo_worker\\rpy2_POC\\Curve Fitting\\Output_Data.csv',header=0)
df.set_index(['Store','Product']).to_dict()
testing=df.set_index('Store').to_dict()
testing2=testing['Product'].to_dict()

def recur_dictify(frame):
    if len(frame.columns) == 1:
        if frame.values.size == 1: return frame.values[0][0]
    return frame.values.squeeze()
    
idk=recur_dictify(df)

idk[10][Cold]