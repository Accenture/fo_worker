#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Thu Jun  2 11:33:51 2016

@author: kenneth.l.sylvain
"""

# from pulp import LpVariable, LpInteger, LpProblem, LpMinimize, lpSum, LpStatus,PULP_CBC_CMD, pulpTestAll, value, LpBinary
from outputFunctions import mergedPreOptCF, createLong, createWide, createTieredSummary, createDrillDownSummary
import numpy as np
import pandas as pd
# import os
import datetime as dt
from openpyxl import load_workbook
import time

    #pd.DataFrame(mergedPreOptCF).to_csv(filepath+"Test mergePreOptCF.csv") #for unit testing

    return mergedPreOptCF

def workerStandalone(fileName,cfFileName,type,methodology):
    startTime=dt.datetime.today()
    print("Job Started at: "+str(startTime)) #for unit testing

    #read inputs from excel
    fixtureArtifact=pd.read_excel(fileName,sheetname='Space_Data')
    transactionArtifact=pd.read_excel(fileName,sheetname='Transaction_Data')
    transactionArtifact=transactionArtifact.drop(transactionArtifact.index[[0]]).set_index("Store")
    fixtureArtifact=fixtureArtifact.drop(fixtureArtifact.index[[0]]).set_index("Store")
    tierCounts = pd.read_excel(fileName, sheetname='Tier_Counts').set_index('Tier_Value').to_dict()
    # spaceBounds=pd.read_excel(fileName,sheetname='Space_Bounds').set_index("Space").to_dict()
    inputs = pd.read_excel(fileName, sheetname='Values').set_index('Input').to_dict()
    metrics = pd.read_excel(fileName, sheetname='Weighted_Metrics').set_index('Metric').to_dict()
    Stores=fixtureArtifact.index.values.astype(int)
    print(Stores) #for unit testing
    Categories=fixtureArtifact.columns[2:].values
    print(Categories) #for unit testing

    try:
        futureSpace=pd.read_excel(fileName,sheetname="Future_Space").set_index("Store")
    except:
        futureSpace=None
    try:
        brandExitArtifact=pd.read_excel(fileName,sheetname="Brand_Exit")
        brandExitArtifact=brandExitMung(brandExitArtifact,Stores,Categories)
    except:
        brandExitArtifact=None

    # TODO: Call data merging R code in tool worker

    # TODO: Call curve-fitting/bound-setting R code in tool worker
    # TODO: R: use wide table bound input / refactor metric names
    # read output from csv for standalone worker for unit testing
    cfOutput = pd.read_csv(cfFileName, index_col=[0, 3])

    # Call preoptimize function to determine single store optimal space, skipping in the case of enhanced optimizations
    if methodology == "Traditional" :
        preoptimReturned = preoptimize(Stores=Stores,Categories=Categories,spaceData=fixtureArtifact,data=transactionArtifact,metricAdjustment=float(inputs['Value']["Metric_Adjustment"]),salesPenetrationThreshold=float(inputs['Value']["Sales_Penetration_Threshold"]),optimizedMetrics=metrics['Weight'],increment=inputs['Value']["Increment"],brandExitArtifact=brandExitArtifact,newSpace=futureSpace)
        penReturned=preoptimReturned[0]
        optReturned=preoptimReturned[1]
    else:  #since methodology == "Enhanced"
        preoptimReturned = None
        penReturned=pd.DataFrame(index=Stores,columns=Categories)
        optReturned = pd.DataFrame(index=Stores, columns=Categories)

    # Merge pre-optimize outputs with curve-fitting output
    mergedPreOptCFReturned = mergePreOptCF(cfOutput,preoptimReturned)

    #Call optimize function and retrieve results
    optimReturned=optimize(methodology,msg['meta']['name'],Stores, Categories,tierCounts,inputs['Value']["Increment"],metrics['Weight'],mergedPreOptCFReturned)
    optimStatus = optimReturned[0]
    optimResult = optimReturned[1]

    # TODO: If statement on infeasible solutions
    # Call functions to create output information
    longReturned = createLong(mergedPreOptCFReturned, optimResult)
    wideReturned = createWide(Stores,Categories,optimResult,optReturned,penReturned,fixtureArtifact)

    if type == "Tiered":
        summaryReturned = createTieredSummary(longReturned)
    else:  #since type == "Drill Down"
        summaryReturned = createDrillDownSummary(longReturned)
    
    # writeToExcel(optimJobInfo,longReturned,wideReturned,summaryReturned) # for unit testing

    endTime=dt.datetime.today()
    print("The status of the job was: "+str(optimStatus)) #for unit testing
    print("Total Time Taken for Job (Hours:Minutes::Seconds:Milliseconds): "+str(endTime-startTime)) #for unit testing
    print("This optimization had " + str(len(Stores)) + " stores and " + str(len(Categories)) + " categories.") #for unit testing

    return