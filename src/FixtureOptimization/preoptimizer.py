# -*- coding: utf-8 -*-
"""
Created on Thu Jun 30 09:55:06 2016

@author: kenneth.l.sylvain
"""
import numpy as np
import pandas as pd
import logging
import traceback

# Need to change functions to allow for TFC as result of Future Space/ Brand Entry
# Need to create a max & min for category level informaiton to be passed as bounds in a long table format // Matching format to Bounding Info that is added to job context

# import os

def preoptimize(jobType,optimizationType,dataMunged, salesPenThreshold, mAdjustment, optimizedMetrics, increment):
    print(dataMunged.columns)
    """
    Conducts the preoptimization based upon legacy R2 code to determine optimal space for traditional optimizations
    :param optimizationType: enhanced or traditional optimization
    :param dataMunged: output from data merging
    :param salesPenThreshold: sales penetration threshold to determine whether or not store-category information is eligible
    :param mAdjustment: metric adjustment
    :param optimizedMetrics:
    :param increment: increment size
    :return:
    """

    def calcPen(metric):
        """
        Calculates the penetration of a given metric
        :param metric: matrix of metric information
        :return: penetration matrix of a given metric
        """
        return metric.div(metric.sum(axis=1), axis=0)

    def spreadCalc(sales, boh, receipt, mAdjustment):
        """
        Calculates the Spread given sales and inventory
        :param sales: historical sales information
        :param boh: historical beginning on hand information
        :param receipt: historical receipt information
        :param mAdjustment: metric adjustment
        :return:
        """
        # storing input sales and inventory data in separate 2D arrays
        # finding sales penetration, GAFS and spread for each brand in given stores
        # calculate adjusted penetration
        inv = boh.add(receipt)
        return calcPen(sales) + (calcPen(sales).subtract(calcPen(inv))).multiply(mAdjustment)

    def metric_per_fixture(metric1, metric2, mAdjustment):
        """
        Calculates metric per fixture penetration
        :param metric1:
        :param metric2:
        :param mAdjustment: metric adjustment
        :return: array of metric per metric penetration with the metric adjustment
        """
        # storing input sales data in an array
        # finding penetration for each brand in given stores
        # calculate adjusted penetration
        # spacePen = metric2.div(newSpace, axis='index')
        return calcPen(metric1) + ((calcPen(metric1) - calcPen(metric2)) * mAdjustment)

    def metric_per_metric(metric1, metric2, mAdjustment):
        """
        Calculates metric per metric penetration
        :param metric1:
        :param metric2:
        :param mAdjustment: metric adjustment
        :return: returns array of metric per metric penetration with the metric adjustment
        """
        # storing input sales data in an array
        # finding penetration for each brand in given stores
        # calculate adjusted penetration
        return calcPen(metric1) + ((calcPen(metric1) - calcPen(metric2)) * mAdjustment)

    def invTurn_Calc(sold_units, boh_units, receipts_units):
        """
        Calculates an Inventory Turn array
        :param sold_units:
        :param boh_units:
        :param receipts_units:
        :return: returns array of inventory turn
        """
        calcPen(sold_units)
        calcPen(boh_units + receipts_units)
        inv_turn = calcPen(sold_units).div(calcPen(boh_units + receipts_units), axis='index')
        inv_turn[np.isnan(inv_turn)] = 0
        inv_turn[np.isinf(inv_turn)] = 0
        return calcPen(inv_turn)

    def roundArray(array, increment):
        """
        Rounds an array to values based on the increment size
        :param array: array to be rounded
        :param increment: increment size to be rounded to
        :return: returns rounded array
        """
        rounded = np.copy(array)
        for i in range(len(array)):
            for j in range(len(list(array[0, :]))):
                if np.mod(np.around(array[i][j], 0), increment) > increment / 2:
                    rounded[i][j] = np.around(array[i][j], 0) + (
                    increment - (np.mod(np.around(array[i][j], 0), increment)))
                else:
                    rounded[i][j] = np.around(array[i][j], 0) - np.mod(np.around(array[i][j], 0), increment)
        return rounded

    def roundColumn(array, increment):
        """

        :param array: array to be rounded
        :param increment: increment size to be rounded to
        :return: returns rounded column
        """
        for i in range(len(array)):
            if np.mod(np.around(array[i], 3), increment) > increment / 2:
                array[i] = np.around(array[i], 3) + (
                    increment - (np.mod(np.around(array[i], 3), increment)))
            else:
                array[i] = np.around(array[i], 3) - np.mod(np.around(array[i], 3), increment)

    def roundValue(cVal, increment):
        """

        :param cVal:
        :param increment: increment size to be rounded to
        :return:
        """
        if np.mod(round(cVal, 3), increment) > increment / 2:
            cVal = round(cVal, 3) + (increment - (np.mod(round(cVal, 3), increment)))
        else:
            cVal = round(cVal, 3) - np.mod(round(cVal, 3), increment)
        return cVal

    def roundDF(array, increment):
        """
        Rounds a dataframe to have values only as multiples of the increment size
        :param array:
        :param increment: increment size to be rounded to
        :return:
        """
        rounded = array.copy(True)
        for i in array.index:
            for j in array.columns:
                if np.mod(np.around(array[j].loc[i], 3), increment) > increment / 2:
                    rounded[j].loc[i] = np.around(array[j].loc[i], 3) + (
                        increment - (np.mod(np.around(array[j].loc[i], 3), increment)))
                else:
                    rounded[j].loc[i] = np.around(array[j].loc[i], 3) - np.mod(np.around(array[j].loc[i], 3), increment)
        return rounded

    try:
        sales = dataMunged.pivot(index='Store',columns='Category',values='Sales $')
        sold_units = dataMunged.pivot(index='Store',columns='Category',values='Sales Units')
        profit = dataMunged.pivot(index='Store',columns='Category',values='Profit $')

        if optimizationType=='traditional':
            boh = dataMunged.pivot(index='Store', columns='Category', values='BOH $')
            receipt = dataMunged.pivot(index='Store', columns='Category', values='Receipts  $')
            boh_units = dataMunged.pivot(index='Store', columns='Category', values='BOH Units')
            receipts_units = dataMunged.pivot(index='Store', columns='Category', values='Receipts Units')
            gm_perc = dataMunged.pivot(index='Store', columns='Category', values='Profit %')
            ccCount = dataMunged.pivot(index='Store', columns='Category', values='CC Count w/ BOH')
            if jobType == 'tiered' or jobType == 'unconstrained':
                bfc = dataMunged.pivot(index='Store', columns='Category', values='Current Space')
            else:
                bfc = calcPen(sales).multiply(dataMunged.pivot(index='Store',columns='Category',values='New Space'))
            adj_p = (optimizedMetrics['spread'] * spreadCalc(sales, boh, receipt, mAdjustment)) + (optimizedMetrics[
                'salesPenetration'] * calcPen(sales)) + (optimizedMetrics['salesPerSpaceUnit'] * metric_per_fixture(sales,
                                                                                                                   bfc,
                                                                                                                   mAdjustment)) + \
                    (optimizedMetrics['grossMargin'] * calcPen(gm_perc)) + (optimizedMetrics['inventoryTurns'] * invTurn_Calc(
                sold_units, boh_units, receipts_units))
        else:
            adj_p = (optimizedMetrics['sales'] * sales) + (optimizedMetrics['profits'] * profit) + (optimizedMetrics['units'] * sold_units)

        for i in adj_p.index:
            for j in adj_p.columns:
                if adj_p[j].loc[i] < salesPenThreshold:
                    adj_p[j].loc[i] = 0
        print('creating adj_p')
        adj_p = calcPen(adj_p)
        adj_p.fillna(0)
        information=pd.merge(dataMunged,pd.melt(adj_p.reset_index(), id_vars=['Store'], var_name='Category', value_name='Penetration'),on=['Store','Category'])
        information['Optimal Space'] = information['New Space'] * information['Penetration']
        if jobType == 'drilldown':
            information['Current Space'] = information['Optimal Space']
        print('attempting to keep sales pen')
        information = pd.merge(information,pd.melt(calcPen(sales).reset_index(),id_vars=['Store'], var_name='Category',value_name='Sales Penetration'),on=['Store','Category'])
        information = information.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        print(information.columns)
        return information
    except Exception as e:
        logging.exception('A thing')
        traceback.print_exception()
        return
    # return information