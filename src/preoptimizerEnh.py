# -*- coding: utf-8 -*-
"""
Created on Thu Jun 30 09:55:06 2016

@author: kenneth.l.sylvain
"""
import numpy as np
import pandas as pd


# Need to change functions to allow for TFC as result of Future Space/ Brand Entry
# Need to create a max & min for category level informaiton to be passed as bounds in a long table format // Matching format to Bounding Info that is added to job context

# import os

def calcPen(metric):
    return metric.div(metric.sum(axis=1), axis='index')


def getColumns(df):
    # print(df[[ *np.arange(len(df.columns))[0::9] ]].drop(df.index[[0]]).convert_objects(convert_numeric=True).columns)
    return df[[*np.arange(len(df.columns))[0::9]]].drop(df.index[[0]]).convert_objects(convert_numeric=True).columns


def spreadCalc(sales, boh, receipt, mAdjustment):
    # storing input sales and inventory data in separate 2D arrays
    # finding sales penetration, GAFS and spread for each brand in given stores
    # calculate adjusted penetration
    # Not necessary -- sales.columns = master_columns
    inv = boh + receipt
    calcPen(sales)
    calcPen(inv)
    return calcPen(sales) + ((calcPen(sales) - calcPen(inv)) * float(mAdjustment))

def metric_per_fixture(metric1, metric2, mAdjustment):
    # storing input sales data in an array
    # finding penetration for each brand in given stores
    # calculate adjusted penetration
    # spacePen = metric2.div(newSpace, axis='index')
    return calcPen(metric1) + ((calcPen(metric1) - calcPen(metric2)) * float(mAdjustment))


def metric_per_metric(metric1, metric2, mAdjustment):
    # storing input sales data in an array
    # finding penetration for each brand in given stores
    # calculate adjusted penetration
    return calcPen(metric1) + ((calcPen(metric1) - calcPen(metric2)) * float(mAdjustment))


def invTurn_Calc(sold_units, boh_units, receipts_units):
    calcPen(sold_units)
    calcPen(boh_units + receipts_units)
    inv_turn = calcPen(sold_units).div(calcPen(boh_units + receipts_units), axis='index')
    inv_turn[np.isnan(inv_turn)] = 0
    inv_turn[np.isinf(inv_turn)] = 0
    return calcPen(inv_turn)


def roundArray(array, increment):
    rounded = np.copy(array)
    for i in range(len(array)):
        for j in range(len(list(array[0, :]))):
            if np.mod(np.around(array[i][j], 0), increment) > increment / 2:
                rounded[i][j] = np.around(array[i][j], 0) + (increment - (np.mod(np.around(array[i][j], 0), increment)))
            else:
                rounded[i][j] = np.around(array[i][j], 0) - np.mod(np.around(array[i][j], 0), increment)
    return rounded


def roundDF(array, increment):
    rounded = array.copy(True)
    for i in array.index:
        for j in array.columns:
            if np.mod(np.around(array[j].loc[i], 3), increment) > increment / 2:
                rounded[j].loc[i] = np.around(array[j].loc[i], 3) + (
                increment - (np.mod(np.around(array[j].loc[i], 3), increment)))
            else:
                rounded[j].loc[i] = np.around(array[j].loc[i], 3) - np.mod(np.around(array[j].loc[i], 3), increment)
    return rounded

def preoptimizeEnh(dataMunged, salesPenThreshold, mAdjustment, optimizedMetrics, increment):
    sales = dataMunged.pivot(index='Store',columns='Category',values='Sales $')
    print(sales.head())
    boh = dataMunged.pivot(index='Store',columns='Category',values='BOH $')
    receipt = dataMunged.pivot(index='Store',columns='Category',values='Receipts  $')
    sold_units = dataMunged.pivot(index='Store',columns='Category',values='Sales Units')
    boh_units = dataMunged.pivot(index='Store',columns='Category',values='BOH Units')
    receipts_units = dataMunged.pivot(index='Store',columns='Category',values='Receipts Units')
    profit = dataMunged.pivot(index='Store',columns='Category',values='Profit $')
    gm_perc = dataMunged.pivot(index='Store',columns='Category',values='Profit %')
    ccCount= dataMunged.pivot(index='Store',columns='Category',values='CC Count w/ BOH')
    newSpace= dataMunged.pivot(index='Store',columns='Category',values='New Space')
    bfc = dataMunged.pivot(index='Store',columns='Category',values='Current Space')



    mAdjustment = float(mAdjustment)
    adj_p = int(optimizedMetrics['spread']) * spreadCalc(sales, boh, receipt, mAdjustment) + int(
        optimizedMetrics['salesPenetration']) * calcPen(sales) + int(
        optimizedMetrics['salesPerSpaceUnit']) * metric_per_fixture(sales, bfc, mAdjustment) + int(
        optimizedMetrics['grossMargin']) * calcPen(gm_perc) + int(
        optimizedMetrics['inventoryTurns']) * invTurn_Calc(sold_units, boh_units, receipts_units)

    # adj_p.fillna(np.float(0))
    # adj_p[np.isnan(adj_p)] = 0
    # adj_p.where(adj_p < salesPenThreshold, 0, inplace=True)
    for i in adj_p.index:
        for j in adj_p.columns:
            if adj_p[j].loc[i] < salesPenThreshold:
                adj_p[j].loc[i] = 0
    adj_p = calcPen(adj_p)
    adj_p.fillna(0)
    # adj_p[np.isnan(adj_p)] = 0

    # Create Code to make adjustments to adj_p
    opt_amt = roundDF(adj_p.multiply(newSpace, axis='index'), increment)
    return (adj_p, opt_amt)


'''
#For Testing
salesPenThreshold=0
mAdjustment=0
metric=6
increment=.25
adj_p = metric_creation(transaction_data, bfc,salesPenThreshold,mAdjustment,metric,increment)
adj_p.head()
'''
# opt_amt=adj_p.multiply(bfc.sum(axis=1),axis='index').as_matrix()