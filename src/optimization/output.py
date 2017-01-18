import pandas as pd
import numpy as np
from scipy.special import erf
import math
import traceback
import logging
from logging.handlers import RotatingFileHandler
from optimization.loggerManager import LoggerManager

class Outputs():
    def __init__(self):
        pass

    """
    Creates the Tier Columns
    :param df: Long Table to be given tiering information
    :return: tiered long table output
    """
    def tier_col_create(self,dfI):
        try:
            dfI.sort_values(by='Result Space', inplace=True)
            tierVals = dfI.groupby('Category')
            for (i, category) in tierVals:
                indices = category.index.values
                ranking = sorted(set(category['Result Space'].values.tolist()))
                dfI.loc[indices, 'Tier'] = category['Result Space'].apply(lambda x: 'Tier ' + str(ranking.index(x) + 1))
            dfI.reset_index(drop=True)
        except Exception:
            logging.exception('Exception in creating tier columns')
            traceback.print_exception()
        return dfI

    """
    Creates a long table output for user download
    :param jobType: Tiered, Unconstrained, or Drill Down
    :param optimizationType: Traditional or Enhanced
    :param lInput: Optimization Output
    :return: Creates a long table output for user and a version for internal testing
    """
    def create_long(self,job_type, optimization_type, linput):
        LoggerManager.getLogger().info('Creating a long table output')
        LoggerManager.getLogger().info(linput.columns)

        loutput = linput.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        # Merge the optimize output with the curve-fitting output (which was already merged with the preoptimize output)
        if optimization_type == 'enhanced':
            variables = ["Sales", "Profit", "Units"]
            for v in variables:
                loutput["Estimated " + v] = np.where(loutput["Result Space"] < loutput["Scaled_BP_" + v],
                                                 loutput["Result Space"] * (loutput["Scaled_Alpha_" + v] * (erf(
                                                     (loutput["Scaled_BP_" + v] - loutput["Scaled_Shift_" + v]).div(
                                                     math.sqrt(2) * loutput["Scaled_Beta_" + v]))) / loutput[
                                                                                "Scaled_BP_" + v]),
                                                 loutput["Scaled_Alpha_" + v] * erf(
                                                     (loutput["Result Space"] - loutput["Scaled_Shift_" + v]).div(
                                                     math.sqrt(2) * loutput["Scaled_Beta_" + v])))

            for v in variables:
                loutput["Current Estimated " + v] = np.where(loutput["Space"] < loutput["Scaled_BP_" + v],
                                                 loutput["Space"] * (loutput["Scaled_Alpha_" + v] * (erf(
                                                     (loutput["Scaled_BP_" + v] - loutput["Scaled_Shift_" + v]).div(
                                                     math.sqrt(2) * loutput["Scaled_Beta_" + v]))) / loutput[
                                                                                "Scaled_BP_" + v]),
                                                 loutput["Scaled_Alpha_" + v] * erf(
                                                     (loutput["Space"] - loutput["Scaled_Shift_" + v]).div(
                                                     math.sqrt(2) * loutput["Scaled_Beta_" + v])))
            loutput.rename(
            columns={'Sales': 'Current Sales $', 'Profit': 'Current Profit $', 'Units': 'Current Sales Units',
                     'Space': 'Current Space', 'Current Estimated Sales': 'Current Estimated Sales $',
                     'Current Estimated Profit': 'Current Estimated Profit $',
                     'Current Estimated Units': 'Current Estimated Sales Units',
                     'Estimated Sales': 'Result Estimated Sales $',
                     'Estimated Profit': 'Result Estimated Profit $', 'Estimated Units': 'Result Estimated Sales Units',
                     'Optimal Estimated Sales': 'Optimal Estimated Sales $',
                     'Optimal Estimated Profit': 'Optimal Estimated Profit $',
                     'Optimal Estimated Units': 'Optimal Estimated Sales Units', 'Space_to_Fill': 'Total Store Space'},
            inplace=True)
            loutput = self.tier_col_create(loutput)
            fullData = loutput.copy()
            loutput = loutput[
            ['Store', 'Category', 'Climate', 'VSG', 'Result Space', 'Current Space', 'Optimal Space',
             'Current Sales $', 'Current Profit $', 'Current Sales Units', 'Result Estimated Sales $',
             'Result Estimated Profit $', 'Result Estimated Sales Units', 'Optimal Estimated Sales $',
             'Optimal Estimated Profit $', 'Optimal Estimated Sales Units', 'Total Store Space', 'Sales Penetration',
             'Exit Flag', 'Tier']]
        else:
            loutput.drop('Current Space', axis=1, inplace=True)
            loutput.rename(columns={'New Space': 'Total Store Space','Historical Space': 'Current Space'},inplace=True)
            loutput = self.tier_col_create(loutput)
            fullData = loutput.copy()
            loutput = loutput[
            ['Store', 'Category', 'Climate', 'VSG', 'Result Space', 'Current Space',
             'Optimal Space', 'Sales %', 'Exit Flag', 'Total Store Space', 'Tier']]
        loutput.sort_values(by=['Store','Category'], axis=0, inplace=True)
        return (loutput, fullData)

    """
    Creates the wide table output from the long table output
    for user download
    :param long: Long table output
    :param jobType: unconstrained or tiered job
    :param optimizationType: enhanced or traditional
    :return: wide table output
    """
    def create_wide(self,long, job_type, optimization_type):
        # Set up for pivot by renaming metrics and converting blanks to 0's for Enhanced in long table
        adjusted_long = long.rename(columns={ "Result Space": "result", "Optimal Space": "optimal",'Current Space': 'current',
                 "Sales %": "penetration"})

        # Pivot to convert long table to wide, including Time in index for drill downs
        if job_type == "tiered" or 'unconstrained':
            wide = pd.pivot_table(adjusted_long, values=["result", "current", "optimal", "penetration"],
                              index=["Store", "Climate", "VSG"], columns="Category", aggfunc=np.sum, margins=True,
                              margins_name="Total")
        else:  # since type == Drill Down
            wide = pd.pivot_table(adjusted_long, values=["result", "current", "optimal", "penetration"],
                              index=["Store", "Time", "Climate", "VSG"], columns="Category", aggfunc=np.sum,
                              margins=True, margins_name="Total")

        # Generate concatenated column titles by swapping levels and merging category name with metric name
        wide = wide.swaplevel(axis=1)
        wide.columns = ['_'.join(col) for col in wide.columns.values]

        # Delete last row (which is a sum of column values)
        wide = wide.ix[:-1]  # drop last row

        # Set up for column reordering
        cols = wide.columns.tolist()
        num_categories = int((len(cols)) / 4 - 1)  # find number of categories, for use in finding total column numbers
        tot_col = {"C": num_categories, "O": 2 * num_categories + 1, "R": 3 * num_categories + 2}

        # Convert 0's back to blanks
        if optimization_type == "enhanced":
            for i in range(tot_col["R"] + 1, len(cols)):
                wide[[i]] = ""

    # Reorder columns and drop total penetration
    # cols = cols[:tot_col["C"]] + cols[tot_col["C"] + 1:tot_col["O"]] + cols[tot_col["O"] + 1:tot_col[
    #                                                                                                      "R"]] + [
    #            cols[tot_col["C"]]] + cols[tot_col["R"]:-1]
    #
        cols = cols[:tot_col["C"]] + cols[tot_col["C"] + 1:tot_col["O"]] + cols[tot_col["O"] + 1:tot_col[
                                                                                                         "R"]] + [
               cols[tot_col["C"]]] + [cols[tot_col["O"]]] + cols[tot_col["R"]:-1]
        wide = wide[cols]
        wide.drop('Total_optimal',axis=1,inplace=True)
        wide.reset_index(inplace=True)
        wide.sort(columns=['Store'],axis=0,inplace=True)
        return wide
    
    """
    Create summary for user download that applies to Tiered optimizations (type == "Tiered")
    Calculates store counts by tier and by climate
    """ 
    def create_tiered_summary(self,finalLong) :
        #pivot the long table to create a data frame providing the store count for each Category-ResultSpace by Climate along with the total for all climates
        tieredSummaryPivot = pd.pivot_table(finalLong, index=['Category', 'Result Space'], columns='Climate', values='Store', aggfunc=len, margins=True)
        #rename the total for all climates column
        tieredSummaryPivot.rename(columns = {'All':'Total Store Count'}, inplace = True)
        #delete the last row of the pivot, as it is a sum of all the values in the column and has no business value in this context
        tieredSummaryPivot = tieredSummaryPivot.ix[:-1]
        # tieredSummaryPivot.to_excel('outputs.xlsx',sheet_name='Summary_Table')
        tieredSummaryPivot.reset_index(inplace=True)
        return tieredSummaryPivot


    """
    Create summary for user download that applies to Drill Down optimizations
    Calculates space by tier-climate combination for user download
    """
    def create_drill_down_summary(self,finalLong):

        # pivot the long table to create a data frame providing the store count for each Time-Category-ResultSpace by Category along with the total for all categories.
        drilldownSummaryPivot = pd.pivot_table(finalLong, index=['Time', 'Climate', 'Optimal Space'], columns='Category', values='Store', aggfunc=len, margins=True)
        # rename the total for all Category column
        drilldownSummaryPivot.rename(columns={'All': 'Total Store'}, inplace=True)
        # Replace the total store value to particlar index column Total Store Count
        drilldownSummaryPivot.insert(0, 'Total Store Count', '', allow_duplicates=False)
        drilldownSummaryPivot['Total Store Count'] = drilldownSummaryPivot['Total Store']
        # Drop Total Store column
        drilldownSummaryPivot.drop('Total Store', axis=1, inplace=True)
        # delete the last row of the pivot, as it is a sum of all the values in the column and has no business value in this context
        drilldownSummaryPivot = drilldownSummaryPivot.ix[:-1]
        drilldownSummaryPivot.reset_index(inplace=True)
        return drilldownSummaryPivot

    def output_validation(self,df, job_type, tierCounts, increment):
        df.reset_index(inplace=True,drop=True)
        try:
            nullTest = 0
            tcValidation = 0
            exitValidation = 0
            spValidation = 0
            Categories = pd.unique(df['Category'])
            Stores = pd.unique(df['Store'])

            # Is Null
            if df.isnull().values.any():
                nullTest = 1
            if tierCounts is not None:
                # Tier Counts
                # Take a vector of numbers and append 0 or 1 for each Category
                tierCountValidation = pd.Series(data=0, index=Categories)
                for (j, Product) in enumerate(Categories):
                    if len(pd.unique(df[df.Category == Product]['Result Space'].dropna())) > tierCounts[Product][1]:
                        tierCountValidation[Product] = 1
                if sum(tierCountValidation) > 0:
                    tcValidation = 1
            else:
                tcValidation=0
            # TODO Might be able to make this faster with an apply function and an 'any'
            exitVector = pd.Series(index=df.index, name='ExitVector')
            spVector = pd.Series(index=df.index, name='SalesPenetrationVector')
            bbVector = pd.Series(index=df.index, name='BalanceBackVector')
            for i in df.index:
                # Brand Exit
                exitVector.iloc[i] = 1 if df['Exit Flag'].iloc[i] == 1 and df['Result Space'].iloc[i] > 0 else 0
                # Sales Penetration
                spVector.iloc[i] = 1 if df['Sales %'].iloc[i] == 1 and df['Result Space'].iloc[i] > 0 else 0
                # Balance Back

            for (i, Store) in enumerate(Stores):
                if df.groupby('Store')['Result Space'].sum().iloc[i] < max(df['Total Store Space'].iloc[i] * 1.1,
                                                                        df['Total Store Space'].iloc[i] + increment * 2) and \
                            df.groupby('Store')['Result Space'].sum().iloc[i] > min(df['Total Store Space'].iloc[i] * 0.9,
                                                                                    df['Total Store Space'].iloc[
                                                                                        i] - increment * 2):
                    bbVector.iloc[i] = 0
                else:
                    bbVector.iloc[i] = 1

            exitValidation = 1 if sum(exitVector) > 0 else 0
            spValidation = 1 if sum(spVector) > 0 else 0
            bbValidation = 1 if sum(bbVector) > 0 else 0

            # if df['Result Space'] > min(df['Future Space'] * 1.1, df['Future Space'] + increment * 2) and df[

            #     'Result Space'] < max(df['Future Space'] * 0, 9, df['Future Space'] - increment * 2):
            #     bbVector=0
            # else:
            #     bbVector=1


            return (nullTest, tcValidation, exitValidation, spValidation, bbValidation)
        except Exception:
            logging.exception('error in output validation')
