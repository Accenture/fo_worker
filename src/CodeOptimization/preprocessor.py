#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import logging
import traceback
from bcolors import Bcolors

class Preoptimizer():
    """
    Created on Thu Jan 05 16:08:06 2017

    @author: kenneth.l.sylvain
    @author: omkar.marathe
    """
 
    def __init__(self):
        pass

    def compute_penetration(data, metric):
        return data.groupby('Store')[metric].apply(lambda x: x / float(x.sum()))

    # data cleaning: sets any negative value for specified column in data to zero
    def set_negative_to_zero(data, column):
        data.loc[data[column] < 0, (column)] = 0

    def print_warning(title, data):
        logging.info(' ')
        logging.info(Bcolors.BOLD + Bcolors.UNDERLINE + Bcolors.FAIL + title + Bcolors.ENDC)
        logging.info(' ')
        logging.info(data)

    def validate_sales_data(sales_data):
        """
        Performns validation of sales data respective expected range of data:

        :param data: output from data merging
        :return: validated data with data imputed in case of invalid data
        """

        # Looks for negative sales data
        idx = (sales_data['Sales $']  < 0) | (sales_data['Sales Units'] < 0) | \
          (sales_data['BOH $']    < 0) | (sales_data['BOH Units']   < 0) | \
          (sales_data['Profit $'] < 0) | (sales_data['Profit %']    < 0)

        if np.sum(idx) > 0:
            print_warning('Negative sales data:', sales_data[idx])

            # sets Profit % to zero when any of the above metrics is negative,
            # since Profit % = Profit $ / Sales $ we can end up with high positive but wrong values (!)
            # despite both $ values being negative!
            sales_data.loc[idx, ('Profit %')] = 0

            # sales data columns for which data cleaning will be performed
            sales_columns = ['Sales $', 'Sales Units', 'BOH $', 'BOH Units', 'Profit $', 'Profit %']

            # for all sales data columns listed, set any negative value to zero
            for sales_column in sales_columns:
            set_negative_to_zero(sales_data, sales_column)

            logging.info(' ')
            logging.info('Data post cleaning:')
            logging.info(' ')
            logging.info(sales_data[idx])

        # Summary per category
        x = sales_data.groupby('Category')[['Sales $', 'Profit $']].sum()

        x['Profit %'] = x['Profit $'] / x['Sales $']

        logging.info(' ')
        logging.info('Calculates Profit in % of Sales for each category across all stores')
        logging.info(' ')
        logging.info(x)
        logging.info(' ')

        return sales_data, idx


    # Validation for space data
    #
    # Not fully implemented!!

    def validate_space_data(space_data, category_bounds):

        # looks for non-negative Current Space
        idx = space_data['Current Space'] <= 0

        if np.sum(idx) > 0:
            print_warning('Non-positive space data:', space_data[idx])

        for category in space_data['Category'].unique():
            # gets the record for this particular category
            bound = category_bounds[category_bounds['Category'] == category]

            # gets the unique space values for the category from Current Space across all stores
            space_values = space_data[space_data['Category'] == category]['Current Space'].unique()

            logging.info(category + ':' + \
              str(bound['Lower Space Bound'].values) + ' <= ' + \
              str(np.sort(space_values)) + ' <= ' + \
              str(bound['Upper Space Bound'].values))

        space_values = np.sort(space_data['Current Space'].unique())

        logging.info(space_data.groupby(['Category', 'Current Space'])['Store'].count())

        space_data.pivot(index='Current Space', columns='Category', values='Optimal Space')

        return space_data, idx

    def prepare_data(jobType, optimizationType, data, metricAdjustment, salesPenThreshold, bizmetrics):
        """
        Conducts the preoptimization based upon legacy R2 code to determine optimal space for traditional optimizations
        :param optimizationType: enhanced or traditional optimization
        :param data: output from data merging
        :param salesPenThreshold: sales penetration threshold to determine whether or not store-category information is eligible
        :param metricAdjustment: metric adjustment
        :param bizmetrics:
        :param increment: increment size
        :return: initial long table with optimal space & sales penetration
        """

        #########################################################
        # 2.6.1.: Sales $ penetration and Sales Units Penetration
        data['Sales %']       = data.groupby('Store')['Sales $'].apply(lambda x: x / float(x.sum()))
        data['Sales Units %'] = data.groupby('Store')['Sales Units'].apply(lambda x: x / float(x.sum()))

        #########################################################
        # GAFS: Goods Available for Sale in $ and Units and its penetration each
        data['GAFS $']        = data['BOH $'] + data['Receipts  $']
        data['GAFS Units']    = data['BOH Units'] + data['Receipts Units']

        data['GAFS %']        = data.groupby('Store')['GAFS $'].apply(lambda x: x / float(x.sum()))
        data['GAFS Units %']  = data.groupby('Store')['GAFS Units'].apply(lambda x: x / float(x.sum()))

        #########################################################
        # bfc Current Space for Tiered and Unconstrained
        data['BFC']           = data['Current Space']
        data['BFC %']         = data.groupby('Store')['BFC'].apply(lambda x: x / float(x.sum()))

        # Drill Down sets BFC = Sales % * New Space
        if jobType == 'drilldown':
            data['BFC'] = data['Sales %'] * data['New Space']

        #########################################################
        # 2.6.2.: Spread = Sales % + metricAdjustment * (Sales % - GAFS %)
        data['Spread'] = data['Sales %'] + metricAdjustment * (data['Sales %'] - data['GAFS %'])

        ################################################
        #
        #           Checks for negative Spread
        #
        # boolean index to rows with negative Spread
        idx_spread = data['Spread'] < 0

        # if there is any negative Spread print out entire row
        if np.sum(idx_spread) > 0:
            logging.info(' ')
            logging.info(Bcolors.BOLD + Bcolors.UNDERLINE + Bcolors.FAIL + 'Negative Spread:' + Bcolors.ENDC)
            logging.info(' ')
            logging.info(data[idx_spread])
            # sets a negative spread to 0
            data.loc[idx_spread, ('Spread')] = 0

        #########################################################
        # 2.6.3.: Sales per Space = sales % + metricAdjustment * (sales % - bfc %)
        data['Sales per Space'] = data['Sales %'] + metricAdjustment * (data['Sales %'] - data['BFC %'])

        # boolean index to rows with negative Spread
        idx_sales_per_space = data['Sales per Space'] < 0

        # if there is any negative Spread print out entire row
        if np.sum(idx_sales_per_space) > 0:
            logging.info(' ')
            logging.info(Bcolors.BOLD + Bcolors.UNDERLINE + Bcolors.FAIL + 'There are ' + str(np.sum(idx_sales_per_space)) + \
              ' negative Sales per Space. They will be set to 0!' + Bcolors.ENDC)
            # sets a negative sales per space to 0
            data.loc[idx_sales_per_space, ('Sales per Space')] = 0

        #########################################################
        # 2.6.4.: Inventory turnover rate
        data['Inventory Turnover'] = data['Sales Units %'] / data['GAFS Units %']
        # sets turnover to 0 when NaN such that other categories get proper sum for same store
        data['Inventory Turnover'].fillna(0, inplace=True)
        # computer turnover in percent of store
        data['Inventory Turnover %'] = data.groupby('Store')['Inventory Turnover'].apply(lambda x: x / float(x.sum()))

        # THIS NEEDS TO BE CHECKED IF A NAN or INF SHOULD BE REPORTED
        #inventory_turnover[np.isnan(inventory_turnover)] = 0
        #inventory_turnover[np.isinf(inventory_turnover)] = 0

        #########################################################
        # 2.6.5.: Gross margin = share of Profit % by store - why is that penetration of Profit %?
        data['Gross Margin %'] = data.groupby('Store')['Profit %'].apply(lambda x: x / float(x.sum()))


            ##############################################################
    #
    #             Computation of Penetration
    #
    # "Adjusted" penetration is weighted average of the 5 metrics
    # including Sales and Space penetration or variants thereof.
    # Should we make all components non-negative before averaging?
    # It simply depends what we assume:
    # is the error in one metric an outlier or a indicator of the entire record being wrong!!

    # Also the biz metrics weights are summing up to 100 and not to 1. We should change that since all other
    # percentages are represented that way! Especially since later its compared against the Threshold in %!!
        if optimizationType == 'traditional':
            penetration = bizmetrics['spread']              * data['Spread'] \
                      + bizmetrics['salesPenetration']  * data['Sales %'] \
                      + bizmetrics['salesPerSpaceUnit'] * data['Sales per Space'] \
                      + bizmetrics['grossMargin']       * data['Gross Margin %'] \
                      + bizmetrics['inventoryTurns']    * data['Inventory Turnover %']
        elif optimizationType == 'enhanced':
            # computes penetration as weighted average of the 3 metrics
            # I believ this should be done using the % metrics instead of the absolute ones since
            # the 3 have different scales
            penetration = bizmetrics['sales']   * data['Sales'] + \
                      bizmetrics['profits'] * data['Profit $'] + \
                      bizmetrics['units']   * data['Sales Units']
        else:
            logging.info('Unknown optimization type:')
            logging.info(optimizationType)
            penetration = 0

        # FOR NOW: CHANGED TO DIVIDION BY 100 (12/30/16 RL)
        if bizmetrics['spread'] + \
            bizmetrics['salesPenetration'] + \
            bizmetrics['salesPerSpaceUnit'] + \
            bizmetrics['grossMargin'] + \
            bizmetrics['inventoryTurns'] == 100:
            penetration = penetration / 100.0

        ###############################################################
        #
        # Business Overrides:
        #
        # resets any penetration to 0 if
        # A) its smaller than the sales penetration threshold OR
        # B) it has a brand exit planned
        idx_exit = (data['Sales %'] < salesPenThreshold) | (data['Exit Flag'] == 1)

        if np.sum(idx_exit) > 0:
            logging.info('Setting Penetration to ZERO due to Brand exit flag or too low threshold: ')
            logging.info(data[idx_exit])
            penetration[idx_exit] = 0

        #########################################################
        # tests for NaN in data[column_name] and prints entire record for those
        # but doesnt change the value yet. We need confirmation from business!
        def test_for_nan(data, column_name):
            if np.sum(np.isnan(data[column_name])) > 0:
                logging.info(' ')
                logging.info(Bcolors.BOLD + Bcolors.UNDERLINE + 'NaN in ' + column_name + Bcolors.ENDC)
                logging.info(' ')                
                logging.info(data[np.isnan(data[column_name])])

            # Wont fill NaN for now since we need more feedback from the customer regarding what should be done with them
            #return data[column_name].fillna(0)

        # copies metric into the dataframe
        data['Penetration'] = penetration

        # tests for NaN and prints corresponding records
        test_for_nan(data, 'Penetration')

        # sets NaN to zero
        data['Penetration'].fillna(0, inplace=True)

        # calcuates the Penetration normalized by total penetration for store
        data['Penetration %'] = data.groupby('Store')['Penetration'].apply(lambda x: x / float(x.sum()))

        # tests for NaN and prints corresponding records
        test_for_nan(data, 'Penetration %')

        # sets NaN to zero
        data['Penetration %'].fillna(0, inplace=True)


        ##################################################################################
        #
        #           Traditional Model for Sales to Space relationship
        #

        # adds a new column 'Optimal Space' as product of 'New Space' and 'Penetration'
        # New Space is total available space in store for allocating amongst all categories
        # in proportion of Penetration %
        data['Optimal Space'] = data['New Space'] * data['Penetration %']

        if jobType == 'drilldown':
            # adds new column 'Current Space' as copy of 'Optimal Space'
            data['Current Space'] = data['Optimal Space']

        return data

    def old_prepare_data(jobType, optimizationType, data, metricAdjustment, salesPenThreshold, bizmetrics):

        # creates wide format again such that categories are columns now again
        if 1:
            # Sales
            sales = data.pivot(index='Store', columns='Category', values='Sales $')
            sales_units = data.pivot(index='Store', columns='Category', values='Sales Units')

            # Profit
            profit = data.pivot(index='Store', columns='Category', values='Profit $')
            gross_margin = data.pivot(index='Store', columns='Category', values='Profit %')

            # Beginning on hand
            boh = data.pivot(index='Store', columns='Category', values='BOH $')
            boh_units = data.pivot(index='Store', columns='Category', values='BOH Units')

            # Receipts
            receipts = data.pivot(index='Store', columns='Category', values='Receipts  $')
            receipts_units = data.pivot(index='Store', columns='Category', values='Receipts Units')

            # BFC
            bfc = data.pivot(index='Store', columns='Category', values='Current Space')

            # GAFS (Goods available for sale) as sum of BOH and Receipts
            gafs = boh + receipts
            gafs_units = boh_units + receipts_units

            # 2.6.1.: Sales Penetration (Sales in % of total for category)
            sales_penetration = calc_penetration(sales)

            # 2.6.2.: Spread
            spread = calc_spread(sales, gafs, metricAdjustment)

            # 2.6.3.: Sales per Space
            sales_per_space = calc_sales_per_space(sales, bfc, metricAdjustment)

            # 2.6.4.: Inventory Turnover
            inventory_turnover = calc_penetration(sales_units).div(calc_penetration(gafs_units), axis='index')
            # THIS NEEDS TO BE CHECKED IF A NAN or INF SHOULD BE REPORTED
            inventory_turnover[np.isnan(inventory_turnover)] = 0
            inventory_turnover[np.isinf(inventory_turnover)] = 0
            inventory_turnover = calc_penetration(inventory_turnover)

            # 2.6.5.: Gross Margin Penetration
            gross_margin_penetration = calc_inventory_turnover(sales_units, gafs_units)

            if optimizationType == 'traditional':
                penetration2 = bizmetrics['spread']             * spread \
                          + bizmetrics['salesPenetration']  * sales_penetration \
                          + bizmetrics['salesPerSpaceUnit'] * sales_per_space \
                          + bizmetrics['grossMargin']       * gross_margin_penetration \
                          + bizmetrics['inventoryTurns']    * inventory_turnover
            elif optimizationType == 'enhanced':
                # computes penetration as weighted average of the 3 metrics
                penetration2 = bizmetrics['sales']  * sales + \
                          bizmetrics['profits'] * profit + \
                          bizmetrics['units']   * sales_units

            # resets any penetration to 0 if its smaller than the sales penetration threshold
            for i in penetration2.index:
                for j in penetration2.columns:
                    if penetration2[j].loc[i] < salesPenThreshold:
                        penetration2[j].loc[i] = 0

            # converts adjusted penetration into percent of total and fills NaN with 0
            penetration2 = calc_penetration(penetration2)
            # NaN should be investigated!!
            penetration2.fillna(0)

            