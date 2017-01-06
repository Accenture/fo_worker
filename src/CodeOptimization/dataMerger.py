import pandas as pd
import numpy as np
import sys
import traceback
import logging
from itertools import product


class DataMerger():
    def __init__(self):
        pass

    """
    Assigns names for each financial metric in the Transaction Data Set and converts it to a long table
    :param wide_data: Individual Wide Table of Transaction Metric
    :param stores: List of stores
    :param categories: List of categories
    :return: Returns a long table for an individual metric
    """
    def convert_wide2long_sales(self,wide_data, stores, categories):

        wide_data.loc[0, :] = categories
        wide_data = pd.concat([stores, wide_data], axis=1)
        wide_data.columns = wide_data.loc[0,]

        long_data = pd.melt(wide_data[2::], id_vars=['Store'], var_name='Category',
                        value_name=pd.unique(wide_data.loc[1].dropna().values)[0])
        long_data = long_data.apply(lambda x: pd.to_numeric(x, errors='ignore'))

        return long_data


    # adds a category variable as 'Category' to a dataframe
    def add_category(self,data, category):
        data.insert(1, 'Category', category)
        return data


    """
    Reads Sales data from a csv file and places it into a dataframe
    and cleans it up for the merger with other data

    :param filename:
    :return:
    """    
    def read_sales_data(self,filename, jobType='tiered'):
       
        # reads in file, uses first row as headers
        data = pd.read_csv(filename, header=0, dtype={'Store': object})
        
        # extracts the categories from first row in sales data
        categories = data.columns[1::9].values

        # Note: data comes with 2 header rows!
        # copies the label in first row into second row
        data.values[0][0] = data.columns[0]

        # sets the header to the values from the first row
        data.columns = data.values[0]

        # drops the first row (includes just the category names)
        data.drop(0, inplace=True)

        # converts all space data to numeric (NOT THE CLEANEST WAY)
        data = data.apply(lambda x: pd.to_numeric(x, errors='ignore'))

    
        if  jobType in ('tiered', 'unconstrained'):       
            # extracts the metrics for each category and adds the Category column with category as its value
            frames  = [self.add_category(data.ix[:, np.r_[0, c*9+1:c*9+10]], category) for (c, category) in enumerate(categories)]

            # concatenates the individual dataframes to one
            sales = pd.concat(frames)
        # if jobType is drilldown
        elif jobType == 'drilldown':
            # NOT TESTED YET!!
            test = pd.DataFrame(list(product(stores, categories)), columns=['Store', 'Category'])
            # merging test (?) with long sales table
            # extracts business metrics one at a time
            for (m, metric) in enumerate(metrics):
                a = self.convert_wide2long_sales(data.loc[:, int(m + 1)::9], pd.DataFrame(sales[0]), categories)
                test = pd.merge(left=test, right=a, on=['Store', 'Category'])

            # merges data on store
            sales = pd.merge(left=data, right=test, on=['Store'], how='outer')
            
        return sales


    """
    Reads Space data from a csv file and places it into a dataframe
    and cleans it up for the merger with other data

    - renames 'VSG ' to 'VSG'
    - adds 'Current Space' as a copy of Historical Space
    - adds 'Store Space' as the total sum of space per store across all categories

    :param filename:
    :return:
        """    
    def read_space_data(self,filename):   
 
        #deals with Space data
        #reads in file, uses first row as headers
        data = pd.read_csv(filename, header=0, dtype={'Store': object})

        # removes the space in 'VSG '
        data.rename(columns={'VSG ': 'VSG'}, inplace=True)

        # drops the first value row which includes only 'Current Space' labels
        data = data.drop([0])

        # converts all space data to numeric
        data = data.apply(lambda x: pd.to_numeric(x, errors='ignore'))

        # un-pivoting: converts wide-format space data to long format
        # uses previous column names as levels for column 'Category'
        # and stores the data as 'Historical Space'
        data = pd.melt(data,
                    id_vars=['Store', 'Climate', 'VSG'],
                    var_name='Category',
                    value_name='Historical Space')

        # adds a copy of Historical space as Current Space
        data['Current Space'] = data['Historical Space']

        # adds total space by store across all categories
        store_space = data.groupby('Store')['Historical Space'].sum()

        # resets Index such that Store ID becomes normal column we can join next on
        store_space = pd.DataFrame(store_space).reset_index()

        # renames the total count column to 'Store Space' such that it doesnt conflict at merging
        store_space.columns = ['Store', 'Store Space']

        # add the store total to the space data
        data = data.merge(store_space, on='Store', how='inner')

        return data
    # Reads Future space data from a csv file
    def read_future_space_data(self,jobType, filename=None):

        # reads in file, uses first row as headers
        data = pd.read_csv(filename, header=0, dtype={'Store': object})

        # drops the first value row which includes only secondary labels
        data = data.drop([0])

        # converts all space data to numeric
        data = data.apply(lambda x: pd.to_numeric(x, errors='ignore'))

        return data


    # Reads Brand exit data from a csv file
    def read_brand_exit_data(self,filename=None):

        try:
            # reads in file, uses first row as headers
            data = pd.read_csv(filename, header=0)

            # un-pivotes the data to have now Category-Store pairs and drop those with Store=NaN
            data = pd.melt(data, var_name='Category', value_name='Store').dropna(axis=0)

            # converts Store ID to integer
            data['Store'] = data['Store'].astype(int)

            # orders columns to Store, Category
            data = data[['Store', 'Category']]

        except Exception as e:
            logging.error('error in read_brand_exit_data()')
            data = None

        return data

    # Merges Space data with requirements for future space and brand exits
    def merge_space_data(self,space, future_space, brand_exit):
        # if no future space set future space to total store space as calc in process space data step
        # if no future space set entry space to zero
        # set new space to future - entry space

        # incorporates Future Space data
        if future_space is None:

            # If no Future space is specified, use total current space
            space['Future Space'] = space['Store Space']

            # set entry space to zero
            space['Entry Space'] = 0
        else:
            # Future space data is available
            # merge future data into space data on store
            space = space.merge(future_space, on=['Store', 'Climate', 'VSG'], how='left')

        # 'New Space' is the amount of space available to be allocated in the optimization
        # it is the difference between total available store space and the space reserved for new entries
        space['New Space'] = space['Store Space'] - space['Entry Space']

        # incorporates Brand Exit data
        if brand_exit is None:
            space['Exit Flag'] = 0
        else:
            brand_exit['Exit Flag'] = 1

            # merges brand exit data with space
            space = pd.merge(space, brand_exit, on=['Store', 'Category'], how='left')

        space['Exit Flag'] = space['Exit Flag'].fillna(0).astype(int)

        return space


    # Merges Space and Sales data by Store and Category
    def merge_space_and_sales_data(self,sales, space):
        data = pd.merge(space, sales, on=['Store', 'Category'], how='inner')

        return data


    def extract_space_bound(self,space_bound, increment):

        # space_bound is holding the lower and upper space bounds for each category
        space_bound = pd.DataFrame.from_dict(space_bound).T.reset_index()

        # names the columns
        space_bound.columns = ['Category',
                          'Lower Space Bound', 'Upper Space Bound',
                          'Lower Space Limit %', 'Upper Space Limit %']
        # NOTE: Bounds in % dont seem to be needed here any further, maybe for other Optimizations?!

        # extracts only the absolute bounds and category from dataframe
        space_bound = space_bound[['Category', 'Lower Space Bound', 'Upper Space Bound']]

        # rounds the bounds according to the increment value given
        space_bound[['Lower Space Bound', 'Upper Space Bound']] = \
            increment * round(space_bound[['Lower Space Bound', 'Upper Space Bound']] / increment)       

        return space_bound


    def extract_tier_bound(self,tier_bound):
        tier_bound = pd.DataFrame.from_dict(tier_bound).T.reset_index()

        tier_bound.columns = ['Category', 'Lower Tier Bound', 'Upper Tier Bound']
        return tier_bound

    def prepare_bounds(self,space_bound, increment, tier_bound=None):

        # spaceBound is holding the lower and upper space bound for each category
        space_bound = self.extract_space_bound(space_bound, increment)

        logging.info(' ')
        logging.info('1a. Extracting Space bounds')
        logging.info(' ')
        logging.info(space_bound)

        if tier_bound is not None:
            tier_bound = self.extract_tier_bound(tier_bound)
            logging.info(' ')
            logging.info('1b. Extracting Tier bounds')
            logging.info(' ')
            logging.info(tier_bound)

            bounds = space_bound.merge(tier_bound, on=['Category'], how='outer')
        else:
            bounds = space_bound

        bounds.set_index(keys='Category', inplace=True)

        return bounds