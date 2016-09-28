# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 16:07:59 2016
Data Merging!!
@author: tkg3azc
"""

##################################
# 0. Setup
##################################
import pandas as pd
import os
import numpy as np
import string as str

# os.chdir('C:\\Users\\tkg3azc\\Downloads\\FO Data Merging')
#
# Hist_perf = pd.read_csv("Sales Data-WATH-ALLSTR.csv", header=None)
# Hist_perf.head()
#
# HSCI = pd.read_csv("Space Data-WATH-ALLSTR.csv", header=None)
# HSCI.head()
#
# Future_Space_Entry_Data = pd.read_csv("Future Space Data-WATH-ALLSTR.csv", header=0)
# # Future_Space_Entry_Data.head()
# Future_Space_Entry_Data = None
#
# Brand_Exit = pd.read_csv("Exit Data-WATH-ALLSTR.csv", header=None)
# Brand_Exit.head()
#
# optim_type = "Tiered"
#
# output = data_merging(optim_type, Hist_perf, HSCI, Future_Space_Entry_Data, Brand_Exit)


# big_master_data.to_csv("final_py.csv")


def kbMerge(optim_type, Hist_perf, HSCI, Future_Space_Entry_Data=None, Brand_Exit=None):
    ##################################
    # 1. Get base of final df
    ##################################

    ## 1.1 HIST PERF
    # Get base information

    # Get category list
    categories = pd.unique(Hist_perf.loc[0])
    categories = categories[pd.notnull(categories)]
    categories = categories[1:]

    # Get metric names
    metric_names = pd.unique(Hist_perf.loc[1])
    metric_names = metric_names[pd.notnull(metric_names)]

    # Get column names
    other_cols = ['Cat', 'Store']
    all_col_names = np.concatenate((other_cols, metric_names), 0)

    # Drop header rows
    Hist_perf = Hist_perf[2:]  # drop first two header rows
    Hist_perf = Hist_perf.reset_index(drop=True)  # reset index

    # Get list of stores
    store_list = Hist_perf[[0]]

    ## 1.2 HIST PERF
    # Assemble new dataframe by taking columns from Hist_perf

    for i in range(0, len(categories)):
        # Set stores and categories
        mini_df = pd.DataFrame()
        mini_df[[0]] = pd.DataFrame([categories[(i)]] * len(store_list))  # duplicate category name in column
        mini_df[[1]] = store_list

        # Get the category's corresponding data block
        start_col = 1 + (len(metric_names) * (i))  # determine start column for data block
        end_col = start_col + len(metric_names) - 1
        data_block = Hist_perf.ix[0:, start_col:end_col]  # get data block

        # Combine stores, categories, data block. Add col names.
        mini_df = pd.concat([mini_df, data_block], 1)  # concat by columns
        mini_df.columns = all_col_names

        # Concatenate current mini_df into existing data frame
        if i == 0:
            new_df = mini_df
        else:
            new_df = pd.concat([new_df, mini_df], axis=0)

            # Set column names
    new_df.columns = all_col_names
    new_df = new_df.reset_index(drop=True)  # reset index

    # Reorder columns based on desired output
    final_hist_perf = new_df[
        ['Cat', 'Store', 'BOH $', 'BOH Units', 'CC Count w/ BOH', 'Profit $', 'Profit %', 'Receipts  $',
         'Receipts Units', 'Sales $', 'Sales Units']]

    ## START HSCI PREP
    ## 1.3 HSCI
    # Get base information

    # Get category list and metric names
    first_row = pd.unique(HSCI.loc[0])
    first_row = first_row[pd.notnull(first_row)]
    metric_names = first_row[0:3]
    categories = first_row[3:]

    # Get column names
    other_cols = ['Cat', 'value']
    all_col_names = np.concatenate((metric_names, other_cols), 0)

    # Drop header rows
    HSCI = HSCI[2:]  # drop first two header rows
    HSCI = HSCI.reset_index(drop=True)  # reset index
    HSCI.columns = first_row

    # Get list of stores
    store_list = HSCI[[0]]

    ## 1.4 HSCI
    # Melt into new data format
    melted_HSCI = pd.melt(HSCI, id_vars=metric_names)
    melted_HSCI.columns = all_col_names

    # melted_HSCI.to_csv("melted_HSCI.csv")

    ## 1.5
    # Merging between Historical performance input and Historical space and climate info
    m = pd.merge(melted_HSCI, final_hist_perf, on=['Store', 'Cat'])
    big_master_data = m[
        ['Store', 'Climate', 'VSG ', 'Cat', 'value', 'Sales $', 'Sales Units', 'Profit $', 'BOH $', 'BOH Units',
         'CC Count w/ BOH', 'Profit %', 'Receipts  $', 'Receipts Units']]
    big_master_data.columns = ["Store", "Climate", "VSG", "Category", "Space", "Sales", "Units", "Profit", "BOH $",
                               "BOH Units", "CC Count w/ BOH", "Profit %", "Receipts  $", "Receipts Units"]
    # big_master_data.to_csv("big_master_data_py.csv")
    big_master_data[["Store", "Space", "Sales", "Units", "Profit", "BOH $", "BOH Units", "CC Count w/ BOH", "Profit %",
                     "Receipts  $", "Receipts Units"]] = big_master_data[
        ["Store", "Space", "Sales", "Units", "Profit", "BOH $", "BOH Units", "CC Count w/ BOH", "Profit %",
         "Receipts  $", "Receipts Units"]].apply(pd.to_numeric)

    ##################################
    # 2. Incorporate optional code
    ##################################


    if optim_type == "Tiered":
        # If user has FUTURE SPACE data, use it, else set future space = historical space
        if Future_Space_Entry_Data is None:
            # Future_Space_Entry_Data = big_master_data

            # Get the sum of the Space per Store
            f = big_master_data.groupby('Store')['Space'].sum()
            FS = pd.DataFrame()
            FS['Future_Space'] = f  # Future Space
            FS['Store'] = list(f.index)  # Stores
            FS['Entry_Space'] = 0
            FS['VSG'] = None
            FS['Climate'] = None
        else:
            Future_Space_Entry_Data = Future_Space_Entry_Data[1:]  # drop second header row
            col_names = Future_Space_Entry_Data.columns
            col_names = col_names.str.replace(" ", "_")
            Future_Space_Entry_Data.columns = col_names

        # If user has BRAND EXIT data, use it, else no brand/stores will have LB/UB = 0 due to brand exit
        if Brand_Exit is None:
            Brand_Exit = big_master_data
            Brand_Exit['Exit_Flag'] = 0
            Brand_Exit = Brand_Exit[['Store', 'Category', 'Exit_Flag']]
        else:
            categories = Brand_Exit.loc[0]
            Brand_Exit = Brand_Exit[2:]  # drop two header rows
            Brand_Exit.columns = categories
            Brand_Exit.head()
            brands_with_stores_counter = 0

            for i in range(0, len(categories)):  # Get list of stores and categories for Exit
                # Set stores and categories
                mini_df = pd.DataFrame(columns=['Category', 'Store'])

                small_store_list = Brand_Exit[categories[i]].unique()
                small_store_list = small_store_list[pd.notnull(small_store_list)]
                # print(i,categories[i],len(small_store_list))

                if len(small_store_list) > 0:  # if there are stores to exit for that brand
                    mini_df[['Category']] = pd.DataFrame(
                        [categories[(i)]] * len(small_store_list))  # duplicate category name in column
                    mini_df[['Store']] = pd.DataFrame(small_store_list)

                    # Concatenate current mini_df into existing data frame
                    if brands_with_stores_counter == 0:
                        new_df = mini_df
                        brands_with_stores_counter = brands_with_stores_counter + 1
                    else:
                        new_df = pd.concat([new_df, mini_df], axis=0)

            if brands_with_stores_counter == 0:
                # If BE data was provided but no stores were listed, flag no stores for exit
                Brand_Exit = big_master_data
                Brand_Exit['Exit_Flag'] = 0
                Brand_Exit = Brand_Exit[['Store', 'Category', 'Exit_Flag']]
            else:
                # Flag selected stores and categories for Exit
                new_df['Exit_Flag'] = 'Exit'
                new_df.head()
                Brand_Exit = new_df

    if optim_type == "Enhanced":  # target BA space is the same as historical BA space
        if Future_Space_Entry_Data is None:
            # Future_Space_Entry_Data = big_master_data

            # Get the sum of the Space per Store
            f = big_master_data.groupby('Store')['Space'].sum()
            FS = pd.DataFrame()
            FS['Future_Space'] = f  # Future Space
            FS['Store'] = list(f.index)  # Stores
            FS['Entry_Space'] = 0
            FS['VSG'] = None
            FS['Climate'] = None
        else:
            Future_Space_Entry_Data = Future_Space_Entry_Data[1:]  # drop second header row
            col_names = Future_Space_Entry_Data.columns
            col_names = col_names.str.replace(" ", "_")
            Future_Space_Entry_Data.columns = col_names
            Future_Space_Entry_Data['Entry_Space'] = 0

        # there will not be any product/stores with LB/UB = 0 due to brand exit
        Brand_Exit = big_master_data
        Brand_Exit['Exit_Flag'] = 0
        Brand_Exit = Brand_Exit[['Store', 'Category', 'Exit_Flag']] #What is she trying to do here?!

    ##################################
    # 3. Final data merging
    ##################################


    Brand_Exit[['Store']] = Brand_Exit[['Store']].apply(pd.to_numeric)

    big_master_data = pd.merge(big_master_data, Brand_Exit, on=['Store', 'Category'], how='left')
    print(type(big_master_data))
    print(big_master_data.head())

    big_master_data['Exit_Flag'].fillna(0, inplace=True)

    mini_Future_Space_Entry_Data = Future_Space_Entry_Data[["Store", "Future_Space", "Entry_Space"]]
    big_master_data = pd.merge(big_master_data, mini_Future_Space_Entry_Data, on=['Store'], how='left')

    big_master_data = big_master_data[
        ["Store", "Climate", "VSG", "Category", "Space", "Sales", "Units", "Profit", "Exit_Flag", "Future_Space",
         "Entry_Space", "BOH $", "BOH Units", "CC Count w/ BOH", "Profit %", "Receipts  $", "Receipts Units"]]
    print(big_master_data.head())

    return (big_master_data)


