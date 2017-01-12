#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pulp import *
import numpy as np
import pandas as pd
import logging
from optimization.baseOptimizer import BaseOptimizer
from optimization.solver import CbcSolver, GurobiSolver
from optimization.dataMerger import DataMerger
from optimization.preprocessor import PreProcessor

class TraditionalOptimizer(BaseOptimizer):
    """
    Created on Wed Jan 4 16:00:51 2017

    @author: omkar.marathe

    This is the class for
    Traditional Optimization
    """

    #def __init__(self,job_name,job_type,stores,categories,category_bounds,increment,data,sales_penetration_threshold):
    def __init__(self, sales, space, future_space, brand_exit, config):
        super(TraditionalOptimizer,self).__init__(sales, space, future_space, brand_exit, config)

        self.sales_penetration_threshold = config['salesPenetrationThreshold']
        self.solver = CbcSolver("CBC Solver")
        #self.solver = GurobiSolver("Gurobi SOlver")

    """
    Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
    :param space: Total Space Available in Store
    :param alpha: Percent Bounding for Balance Back
    :param increment: Increment Size Determined by the User in the UI
    :return: Returns an adjusted vector of percentages by which individual store space should be held
    """
    def adjust_for_twoincr(self,space, alpha, increment):
        return max(alpha, (2 * increment) / space)


    """
    Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
    :param row: Individual row of Total Space Available in Store
    :param bound: Percent Bounding for Balance Back
    :param increment: Increment Size Determined by the User in the UI
    :return: Returns an adjusted vector of percentages by which individual store space should be held
    """
    def adjust_for_oneincr(self,row, bound, increment):
        return max(bound, (1 * increment) / row)

    def myround(self,values, increment):
        # converting to integer
        number_set = values / increment
        # needs proper round before converting to int
        sum_of_numbers = np.int(np.round(np.sum(number_set)))

        unround_numbers = [x / float(sum(number_set)) * sum_of_numbers for x in number_set]

        decimal_part_with_index = sorted([(index, unround_numbers[index] % 1) for index in range(len(unround_numbers))], key=lambda y: y[1], reverse=True)
        remainder = sum_of_numbers - sum([int(x) for x in unround_numbers])
        index = 0
        while remainder > 0:
            unround_numbers[decimal_part_with_index[index][0]] += 1
            remainder -= 1
            index = (index + 1) % len(number_set)

        values = pd.Series(np.floor(np.asarray(unround_numbers)) * increment, index=values.index)

        return values

    """
        This function take a list of number and return a list of percentage, which represents the portion of each number in sum of all numbers
        Moreover, those percentages are adding up to 100%!!!
        Notice: the algorithm we are using here is 'Largest Remainder'
        The down-side is that the results won't be accurate, but they are never accurate anyway:)
    """
    def round_to_100_percent(self,number_set, digit_after_decimal=2):
        unround_numbers = [x / float(sum(number_set)) * 100 * 10 ** digit_after_decimal for x in number_set]
        decimal_part_with_index = sorted([(index, unround_numbers[index] % 1) for index in range(len(unround_numbers))], key=lambda y: y[1], reverse=True)
        remainder = 100 * 10 ** digit_after_decimal - sum([int(x) for x in unround_numbers])
        index = 0
        while remainder > 0:
            unround_numbers[decimal_part_with_index[index][0]] += 1
            remainder -= 1
            index = (index + 1) % len(number_set)
        return [int(x) / float(10 ** digit_after_decimal) for x in unround_numbers]

    def create_spacelevels(self,space_bound, increment):
        # determines min and max spacebound across all categories
        min_space_level = min(space_bound['Lower Space Bound'])
        max_space_level = max(space_bound['Upper Space Bound'])

        # creates Space levels as vector starting at min going to max in increment steps
        self.space_levels = list(np.arange(min_space_level, max_space_level + increment, increment))

        # if not existing already, adds a 0th level with value 0.0
        if 0.0 not in self.space_levels:
            self.space_levels.insert(0, 0.0)

        return self.space_levels

    def add_objective(self):
        self.solver.add_objective([(self.selected_tier[store][category][level] * self.error[i][j][k]) \
                             for (i, store) in enumerate(self.stores) \
                                for (j, category) in enumerate(self.categories) \
                                    for (k, level) in enumerate(self.space_levels)], \
                                        "Deviation from optimal space for store ")


    def create_variables(self):
        # Selected Tier(k) for store(i) and category(j) is Binary
        # selected_tier = LpVariable.dicts('Selected Tier', (self.stores, self.categories, space_levels), 0, upBound=1, cat='Binary')
        self.selected_tier = self.solver.add_variables('Selected Tier', self.stores, self.categories, self.space_levels, 0)

        # Created Tier(k) for category(j) is Binary
        if self.job_type == 'tiered':
            logging.info('Creating LP Variable created_tier')

            # created_tier = LpVariable.dicts('Created Tier', (self.categories, space_levels), 0, upBound=1, cat='Binary')
            self.created_tier = self.solver.create_variables('Created Tier', self.categories, self.space_levels, 0)

            # aux variable to track if a tier for a category has been to set to 0 already
            self.created_tier_zero_flag = np.zeros((len(self.categories), 1), dtype=bool)

        #what is the need for this return statement?
        return (self.selected_tier,self.created_tier)

    def create_error(self):
        num_stores = len(self.stores)
        num_categories = len(self.categories)
        num_levels = len(self.space_levels)
        # Calculates the error as abs difference from optimal space and space level
        # creates matrix of store x category to make the FOR loop easier for computing the error
        self.optimal_space = self.data.pivot(index='Store', columns='Category', values='Optimal Space')
        self.sales_penetration = self.data.pivot(index='Store', columns='Category', values='Sales %')

        logging.info('Calculates error to be minimized')
        self.error = np.zeros((num_stores, num_categories, num_levels))
        for (i, store) in enumerate(self.stores):
            for (j, category) in enumerate(self.categories):
                for (k, level) in enumerate(self.space_levels):
                    self.error[i][j][k] = np.absolute(self.optimal_space[category].iloc[i] - level)
        return self.error

    """
    Constraint 1
    At individual store level
    local balance constraint:
    NEW: No balance back, just total
    """
    def add_constraints_forlocalbalanceback(self):
        for (i, store) in enumerate(self.stores):
            self.solver.add_constraint(
                [(self.selected_tier[store][category][level]) * level \
                 for (j, category) in enumerate(self.categories) \
                 for (k, level) in enumerate(self.space_levels)], \
                'eq', self.local_space_to_fill[store], "Location Balance " + str(store))

    """
    Constraint 2.5.2.
    Exactly one Space level for each Store & Category
    """
    def add_constraints_forspaclevelstorecategory(self):
        for (i, store) in enumerate(self.stores):
            for (j, category) in enumerate(self.categories):
                self.solver.add_constraint([self.selected_tier[store][category][level] \
                                      for (k, level) in enumerate(self.space_levels)], 'eq', 1, \
                                     "Exactly one level for store " + str(store) + " and category " + str(category))

    """
    Constraint 2.5.4.
    Space Bound for each Store & Category
    """
    def add_constraints_forspaceboundstorecategory(self):
        for (i, store) in enumerate(self.stores):
            for (j, category) in enumerate(self.categories):
                self.solver.add_constraint([self.selected_tier[store][category][level] * level
                                  for (k, level) in enumerate(self.space_levels)], \
                                  'gte',self.category_bounds['Lower Space Bound'].loc[category], \
                                    "Space at Store "+ str(store) +" & Category "+category+" >= lower bound")

                self.solver.add_constraint([self.selected_tier[store][category][level] * level \
                                  for (k, level) in enumerate(self.space_levels)], \
                                  'lte',self.category_bounds['Upper Space Bound'].loc[category], \
                                    "Space at Store "+ str(store) +" & Category "+category+" <= upper bound")

    """
    Constraint 2.5.4.
    Space Bound for each Store & Category
        # Constraint 2
        #
        # Only For Tiered optimization
        #
        # At each category level
    """
    def add_constraintsfortiered(self):
            ################################
            # Constraint 2a) [Specification constraint 2.5.1.]
            # number of tiers >= lower tier count bound
            for (j, category) in enumerate(self.categories):
                self.solver.add_constraint([self.created_tier[category][level] \
                                  for (k, level) in enumerate(self.space_levels)], \
                           'gte',self.category_bounds['Lower Tier Bound'].loc[category], \
                           "Lower Tier Bound for Category" + category)

            ################################
            # Constraint 2b) [Specification constraint 2.5.1.]
            # number of tiers <= upper tier count bound
            for (j, category) in enumerate(self.categories):
                self.solver.add_constraint([self.created_tier[category][level] \
                                  for (k, level) in enumerate(self.space_levels)], \
                           'lte',self.category_bounds['Upper Tier Bound'].loc[category], \
                           "Upper Tier Bound for Category" + category)

            ################################
            # Constraint 2c) [Specification constraint 2.5.3.]

            # Relationship between Selected Tiers & Created Tiers:
            # Selected Tier can only be chosen if the corresponding Tier is a valid one for the category
            for (j, category) in enumerate(self.categories):
                for (k, level) in enumerate(self.space_levels):
                     self.solver.add_constraintdivision([self.selected_tier[store][category][level] \
                                      for (i, store) in enumerate(self.stores)],len(self.stores), \
                               'lte',self.created_tier[category][level], \
                               "Selected tier " + str(k) + "('" + str(level) + "') must be valid for category" + category)

    """

    """
    def add_constraints_forbrandexit(self):

        # loops through all stores and categories
        for (i, store) in enumerate(self.stores):
            for (j, category) in enumerate(self.categories):
                # [Specification constraint 2.5.10.]
                # checks if category has optimal space set to zero
                if self.sales_penetration[category].loc[store] < self.sales_penetration_threshold:

                    # creates the string to explain the constraint
                    constraint_str = " for category " + category + ": Sales Penetration too low or Brand exit planned."

                    # sets space to zero for category in store
                    self.solver.add_constraint(self.selected_tier[store][category][0.0], 'eq', 1, \
                               "No space in store " + str(store) + constraint_str)

                    # adds corresponding tier constraint for Tiered Optimization
                    if self.job_type == 'tiered':

                        if self.created_tier_zero_flag[j] == False:
                            self.solver.add_constraint(self.created_tier[category][0.0],'eq',1, \
                            "Tier 0 required " + constraint_str)

                            # Sets flag to indicate constraint has been set already
                            # We want to avoid repeating the same contraint multiple times
                            self.created_tier_zero_flag[j] = True

                    # ALSO DO WE NEED TO ADJUST THE TIER BOUNDS AND SHOULD WE ALLOW TO ADD AN EXTRA 0 TIER
                    # AND COUNT ONLY NON-ZERO TIERS FOR THE TIER COUNT BOUNDS?

    """
    removed to support 100% balance back
    """
    def add_globalback(self):
        pass
        # Global Balance back tolerance value for total chain space
        #beta = .05

        ###################################################################
        # Constraint 3

        ################################
        # Constraint 3a) - [Specification constraint 2.5.6.]

        # constraint = 'Constraint 2.5.6.a) Global allocated space >= total available space * (1-beta)'
        #
        # # Global Balance Back: Total allocated space >= Total available space*(1-beta)
        # problem += lpSum([selected_tier[store][category][level] * level \
        #                   for (i, store)    in enumerate(stores) \
        #                   for (j, category) in enumerate(categories) \
        #                   for (k, level)    in enumerate(space_levels)]) \
        #                 >= total_space * (1 - beta), constraint
        #
        # print(constraint)
        #
        # ################################
        # # Constraint 3b) - [Specification constraint 2.5.6.]
        #
        # constraint = 'Constraint 2.5.6.b) Global allocated space <= total available space * (1+beta)'
        #
        # # Global Balance Back: Total allocated space <= Total available space*(1+beta)
        # problem += lpSum([selected_tier[store][category][level] * level \
        #                   for (i, store)    in enumerate(stores) \
        #                   for (j, category) in enumerate(categories) \
        #                   for (k, level)    in enumerate(space_levels)]) \
        #                 <= total_space * (1 + beta), constraint
        #
        # print(constraint)

        ###################################################################
        # Constraint 3

    """
    todo: remove dependency of pulp api
    """
    def get_lpresults(self):
        self.lp_problem_status = LpStatus[self.problem.status]
        if LpStatus[self.problem.status] == 'Optimal':
            # determines the allocated space from the decision variable selected_tier per store and category
            allocated_space = pd.DataFrame(index=self.stores, columns=self.categories)
            for (i, store) in enumerate(self.stores):
                for (j, category) in enumerate(self.categories):
                    for (k, level) in enumerate(self.space_levels):
                        if value(self.selected_tier[store][category][level]) == 1:
                            allocated_space[category][store] = level

            # resets the index
            a = allocated_space.reset_index()

            # renames the first column to 'Store'
            a.rename(columns={'index': 'Store'}, inplace=True)
            # Ken's original code could be problematic
            # a.columns.values[0] = 'Store'

            b = pd.melt(a, id_vars=['Store'], var_name='Category', value_name='Result Space')

            # NOT SURE THIS IS NECESSARY
            # allocated_space = allocated_space.apply(lambda x: pd.to_numeric(x, errors='ignore'))

            self.data = self.data.merge(b, on=['Store', 'Category'], how='inner')

            return (LpStatus[self.problem.status], self.data, value(self.problem.objective), self.problem)  # (longOutput)#,wideOutput)
        else:
            self.data['Result Space'] = 0

            return (LpStatus[self.problem.status], self.data, 0, self.problem)

    """
    """
    def update_optimalspace(self):
        # saves the original Optimal Space for dev purposes
        self.data['Optimal Space unrounded'] = self.data['Optimal Space']

        # rounds 'Optimal Space' to a multiple of increment (SHOULD IT BE DONE IN PREPARE()??)
        self.data['Optimal Space'] = self.data.groupby('Store')['Optimal Space'].apply(
            lambda x: self.myround(x, self.increment))

        # Calculate Total space across all stores
        total_space = self.data['Optimal Space'].sum()

        logging.info('Total Optional Space to be filled:')
        logging.info(total_space)

        # Validation that optimal space = current space
        logging.info('Should match total Current Space:')
        logging.info(self.data['Current Space'].sum())
        logging.info(self.data[self.data['Store'] == 52])

    """
    """
    def update_categorybounds(self):
        # computes the minimum of Optimal Space for each category
        space_minimum = self.data.groupby('Category')['Optimal Space'].min()

        # determines zero's in the minimum for Optimal Space (at least one store is exiting this brand)
        categories_exit_idx = (space_minimum == 0)

        logging.info(self.category_bounds)

        # setting Lower Space Bound for these category to zero
        # THERE IS SOME BETTER WAY OF CODING THIS ACCORDING TO PYCHARM!
        if np.sum(categories_exit_idx) > 0:
            self.category_bounds['Lower Space Bound'][categories_exit_idx] = 0
            if self.job_type == 'tiered':
                # increments the Upper Space Bound by 1 to account for extra 0th tier
                self.category_bounds['Upper Tier Bound'][categories_exit_idx] = self.category_bounds['Upper Tier Bound'][categories_exit_idx] + 1

    """
    """
    def update_blancebackadjustments(self):

        # Local Balance back tolerance value for total store space
        alpha = .05


        # calculates the total space to be filled by grouping 'New Space' by store and averaging it !!??
        self.local_space_to_fill = self.data.groupby('Store')['New Space'].agg(np.mean)
        # Usually New Space (set in dataMerging.py) is already the total store space, so I dont understand why this is needed
        # maybe useful when New Space is depending on category as well

        # Note: local_space_to_fill usually = New Space (to be allocated for category in store)
        self.local_balance_back_adjustment = self.local_space_to_fill.apply(lambda row: self.adjust_for_twoincr(row, alpha, self.increment))


        logging.info('2. Creating the Local Balance Back values (using 2*increment modifier)')
        logging.info(self.local_balance_back_adjustment)

    """
    """
    def prepare_data(self):

        data_merger = DataMerger()

        # A preprocessor object
        pre_processor = PreProcessor()

        self.category_bounds = data_merger.prepare_bounds(self.config['spaceBounds'],
                                                          self.config['increment'],
                                                          self.config['tierCounts'])

        #print (self.sales)

        # Validates sales data
        sales, idx_sales_invalid = pre_processor.validate_sales_data(self.sales)

        # Validates space data
        # space, idx_space_invalid = validate_space_data(space, category_bounds)

        # extracts the categories from the sales data
        sales_categories = self.sales['Category'].unique()
        # extracts the categories from the space data
        space_categories = self.space['Category'].unique()

        # merges the space data with requirements for future space and brand exits by store and category
        space = data_merger.merge_space_data(self.space, self.future_space, self.brand_exit)

        # merges the space data with the sales data by store and category
        sales_space_data = data_merger.merge_space_and_sales_data(sales, space)


        self.data = pre_processor.prepare_data( self.config['jobType'],
                                                        self.config['optimizationType'],
                                                        sales_space_data,
                                                        float(self.config["metricAdjustment"]),
                                                        float(self.config["salesPenetrationThreshold"]),
                                                        self.config["optimizedMetrics"])


    """
    Run the Tiered Traditional LP-based optimization
    """
    def optimize(self):

        logging.info('==> optimizeTrad()')

        self.prepare_data()

        self.space_levels = self.create_spacelevels(self.category_bounds, self.increment)
        logging.info('1. Creates Tiers aka Space levels')        
        logging.info(self.space_levels)

        self.update_blancebackadjustments()
        self.update_optimalspace()
        self.update_categorybounds()

        logging.info(self.category_bounds)

        logging.info('Creating LP Variable selected_tier')

        self.create_variables()
        self.problem = self.solver.create_problem(self.job_name, 'MIN')
        self.create_error()
        logging.info("Adding objective function")
        self.add_objective()
        self.add_constraints_forlocalbalanceback()
        self.add_constraints_forspaclevelstorecategory()
        self.add_constraints_forspaceboundstorecategory()
        if self.job_type == 'tiered':
            self.add_constraintsfortiered()

        logging.info('Adding Brand Exit constraints & Sales Penetration Constraint')
        self.add_constraints_forbrandexit()

        logging.info("The problem has been formulated")

        #status = self.solver.solveProblem()
        self.solver.solveProblem()
        #logging.info(LpStatus[self.problem.status])
        logging.info(self.solver.status)

        return self.get_lpresults()

