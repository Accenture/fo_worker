import pandas as pd
import numpy as np
from scipy.special import erf
import math

# Create long table for user download
def createLong(optimSpace, cfbs=None):
    print('Inside createLong')
    # Merge the optimize output with the curve-fitting output (which was already merged with the preoptimize output)
    if cfbs is not None:
        print('initial merge')
        lOutput = pd.merge(
            optimSpace[['Store', 'Climate', 'VSG', 'Category', 'Current Space', 'New Space', 'Result Space', 'Penetration', 'Optimal Space']], cfbs,
            on=['Store', 'Climate', 'Category'])
        # lOutput['Penetration']= ""
        # lOutput['Optimal Space']= ""
        print('Set Optimal & Penetration to 0')

        lOutput = lOutput.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        variables = ["Sales", "Profit", "Units"]
        for v in variables:
            lOutput["Estimated " + v] = np.where(lOutput["Result Space"] < lOutput["Scaled_BP_" + v],
                                                 lOutput["Result Space"] * (lOutput["Scaled_Alpha_" + v] * (erf(
                                                     (lOutput["Scaled_BP_" + v] - lOutput["Scaled_Shift_" + v]).div(
                                                     math.sqrt(2) * lOutput["Scaled_Beta_" + v]))) / lOutput[
                                                                                "Scaled_BP_" + v]),
                                                 lOutput["Scaled_Alpha_" + v] * erf(
                                                     (lOutput["Result Space"] - lOutput["Scaled_Shift_" + v]).div(
                                                     math.sqrt(2) * lOutput["Scaled_Beta_" + v])))
        print("Finished Forecasting")
        # Reset the index and name the columns
        # lOutput.rename(columns={'level_0': 'Store', 'level_1': 'Category', 'Space': 'Historical Space'}, inplace=True)

        # Either drop or rename space to fill, lower limit, and upper limit
        # lOutput.drop((['Space.to.Fill'],['Lower.Limit'],['Upper.Limit']), axis=1, inplace=True)
        # lOutput.rename(
        #     columns={'Space_to_Fill': 'Space to Fill', 'Lower_Limit': 'Lower Limit', 'Upper_Limit': 'Upper Limit','Exit_Flag': 'Exit Flag'},
        #     inplace=True)
        print('Dropped Columns')
        # Drop scaled coefficients, can be uncommented to test curve-fitting/forecasting
        cols = [c for c in lOutput.columns if c[:6] != 'Scaled']
        lOutput = lOutput[cols]
        print('Dropped more Columns')
        lOutput.drop(['Store_Group_Sales','Store_Group_Units','Store_Group_Profit'], axis=1, inplace=True)
        print('Dropped Group Columns')
        lOutput = lOutput.rename(
            columns={'Sales': 'Current Sales $', 'Profit': 'Current Profit $', 'Units': 'Current Sales Units',
                     'Estimated Sales': 'Estimated Sales $', 'Estimated Profit': 'Estimated Profit $',
                     'Estimated Units': 'Estimated Sales Units'})
        lOutput = lOutput[
            ['Store', 'Climate', 'VSG', 'Category', 'Result Space', 'Current Space', 'Optimal Space', 'Penetration',
             'Current Sales $', 'Current Profit $', 'Current Sales Units', 'Estimated Sales $', 'Estimated Profit $', 'Estimated Sales Units']]
    else:
        print('went to else')
        lOutput=optimSpace[['Store', 'Climate', 'VSG', 'Category', 'Result Space', 'Current Space','Optimal Space', 'Penetration']]
    lOutput.sort(columns=['Store'],axis=0,inplace=True)
    return lOutput

# Create wide table for user download
def createWide(long, jobType, optimizationType):

    # Set up for pivot by renaming metrics and converting blanks to 0's for Enhanced in long table
    adjusted_long = long.rename(
        columns={'Current Space': 'current', "Optimal Space": "optimal", "Result Space": "result",
                 "Penetration": "penetration"})

    print('renamed the columns')
    # Pivot to convert long table to wide, including Time in index for drill downs
    if jobType == "tiered":
        wide = pd.pivot_table(adjusted_long, values=["current", "optimal", "result", "penetration"],
                              index=["Store", "Climate", "VSG"], columns="Category", aggfunc=np.sum, margins=True,
                              margins_name="Total")
    else:  # since type == Drill Down
        wide = pd.pivot_table(adjusted_long, values=["current", "optimal", "result", "penetration"],
                              index=["Store", "Time", "Climate", "VSG"], columns="Category", aggfunc=np.sum,
                              margins=True, margins_name="Total")

    print('transpose the data')
    # Generate concatenated column titles by swapping levels and merging category name with metric name
    wide = wide.swaplevel(axis=1)
    wide.columns = ['_'.join(col) for col in wide.columns.values]

    print('delete the last row')
    # Delete last row (which is a sum of column values)
    wide = wide.ix[:-1]  # drop last row

    print('deleted last row')
    # Set up for column reordering
    cols = wide.columns.tolist()
    num_categories = int((len(cols)) / 4 - 1)  # find number of categories, for use in finding total column numbers
    tot_col = {"C": num_categories, "O": 2 * num_categories + 1, "R": 3 * num_categories + 2}

    print('reorder columns prep')
    # Convert 0's back to blanks
    if optimizationType == "enhanced":
        for i in range(tot_col["C"] + 1, tot_col["O"] + 1):
            wide[[i]] = ""
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
    print('reordered columns')
    wide = wide[cols]
    wide.drop('Total_optimal',axis=1,inplace=True)
    wide.reset_index(inplace=True)
    wide.sort(columns=['Store'],axis=0,inplace=True)
    return wide

# Create summary for user download that applies to Tiered optimizations (type == "Tiered")
# Calculates store counts by tier and by climate
def createTieredSummary(finalLong) :
    #pivot the long table to create a data frame providing the store count for each Category-ResultSpace by Climate along with the total for all climates
    tieredSummaryPivot = pd.pivot_table(finalLong, index=['Category', 'Result Space'], columns='Climate', values='Store', aggfunc=len, margins=True)
    #rename the total for all climates column
    tieredSummaryPivot.rename(columns = {'All':'Total Store Count'}, inplace = True)
    #delete the last row of the pivot, as it is a sum of all the values in the column and has no business value in this context
    tieredSummaryPivot = tieredSummaryPivot.ix[:-1]
    # tieredSummaryPivot.to_excel('outputs.xlsx',sheet_name='Summary_Table')
    tieredSummaryPivot.reset_index(inplace=True)
    return tieredSummaryPivot

# Create summary for user download that applies to Drill Down optimizations
# Calculates space by tier-climate combination for user download
def createDrillDownSummary(finalLong) :
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