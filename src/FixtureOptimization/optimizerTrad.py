#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Jan 3rd, 2017

@author: Ragnar Lesch
"""

from pulp import *
import numpy as np
import pandas as pd
import datetime as dt
#from tabulate import *
#from gurobipy import *

# i dont understand this rounding function!!
def roundValue(cVal, increment):
    if np.mod(round(cVal, 3), increment) > increment / 2:
        cVal = round(cVal, 3) + (increment - (np.mod(round(cVal, 3), increment)))
    else:
        cVal = round(cVal, 3) - np.mod(round(cVal, 3), increment)
    return cVal

# rounds a set of numbers to the closest multiple of increment while still adding up to the original total
def myround(values, increment):

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

def round_to_100_percent(number_set, digit_after_decimal=2):
    """
        This function take a list of number and return a list of percentage, which represents the portion of each number in sum of all numbers
        Moreover, those percentages are adding up to 100%!!!
        Notice: the algorithm we are using here is 'Largest Remainder'
        The down-side is that the results won't be accurate, but they are never accurate anyway:)
    """
    unround_numbers = [x / float(sum(number_set)) * 100 * 10 ** digit_after_decimal for x in number_set]
    decimal_part_with_index = sorted([(index, unround_numbers[index] % 1) for index in range(len(unround_numbers))], key=lambda y: y[1], reverse=True)
    remainder = 100 * 10 ** digit_after_decimal - sum([int(x) for x in unround_numbers])
    index = 0
    while remainder > 0:
        unround_numbers[decimal_part_with_index[index][0]] += 1
        remainder -= 1
        index = (index + 1) % len(number_set)
    return [int(x) / float(10 ** digit_after_decimal) for x in unround_numbers]


###############################################################################################################
def create_space_levels(space_bound, increment):
    # determines min and max spacebound across all categories
    min_space_level = min(space_bound['Lower Space Bound'])
    max_space_level = max(space_bound['Upper Space Bound'])

    # creates Space levels as vector starting at min going to max in increment steps
    space_levels = list(np.arange(min_space_level, max_space_level + increment, increment))

    # if not existing already, adds a 0th level with value 0.0
    if 0.0 not in space_levels:
        space_levels.insert(0, 0.0)

    return space_levels


###############################################################################################################
def optimizeTrad(job_name, job_type, stores, categories, category_bounds, increment, data, sales_penetration_threshold):
    """
    Run the Tiered Traditional LP-based optimization

    Side-effects: ?
        - creates file: Fixture_Optimization.lp (constraints)
        - creates file: solvedout.csv <= to be inserted into db
        - creates file: solvedout.text

    Synopsis:
        1. Creates space levels from space boundaries
        2. Creates Local Balance Back values
        3. Rounds Optimal Space to multiples of <increment> such that it totals up to total store space
        5. Validates data by comparing total Optimal Space to total Current Space
        6.
    """

    print('==> optimizeTrad()')

    ###############################################################################################################
    #
    #                               Creating Tiers aka Space levels
    #

    space_levels = create_space_levels(category_bounds, increment)

    print(' ')
    print('1. Creates Tiers aka Space levels')
    print(space_levels)

    # Local Balance Back

    # Local Balance back tolerance value for total store space
    alpha = .05

    # Global Balance back tolerance value for total chain space
    beta = .05

    # calculates the total space to be filled by grouping 'New Space' by store and averaging it !!??
    local_space_to_fill = data.groupby('Store')['New Space'].agg(np.mean)
    # Usually New Space (set in dataMerging.py) is already the total store space, so I dont understand why this is needed
    # maybe useful when New Space is depending on category as well

    # CHECK IT OUT
    # ITS NOT REALLY CLEAR WHAT THIS DOES
    def adjustForTwoIncr(space, alpha, increment):
        """
        Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
        :param space: Total Space Available in Store
        :param alpha: Percent Bounding for Balance Back
        :param increment: Increment Size Determined by the User in the UI
        :return: Returns an adjusted vector of percentages by which individual store space should be held
        """
        return max(alpha, (2 * increment) / space)

    # Note: local_space_to_fill usually = New Space (to be allocated for category in store)
    local_balance_back_adjustment = local_space_to_fill.apply(lambda row: adjustForTwoIncr(row, alpha, increment))

    # for example:
    # if space = 6 and increment = 0.5, alpha = 5%, 2*increment/space = 1/6 = 0.1667 = 17%

    print(' ')
    print('2. Creating the Local Balance Back values (using 2*increment modifier)')
    print(local_balance_back_adjustment)
    print(' ')


    # number of stores, categories and (space) levels
    num_stores     = len(stores)
    num_categories = len(categories)
    num_levels     = len(space_levels)

    # saves the original Optimal Space for dev purposes
    data['Optimal Space unrounded'] = data['Optimal Space']

    # Ken's original code:
    #print(data[data['Store'] == 18]['Optimal Space'].apply(lambda x: roundValue(x, increment)))

    # rounds 'Optimal Space' to a multiple of increment (SHOULD IT BE DONE IN PREPARE()??)
    data['Optimal Space'] = data.groupby('Store')['Optimal Space'].apply(lambda x: myround(x, increment))

    # Calculate Total space across all stores
    total_space = data['Optimal Space'].sum()

    print('Total Optional Space to be filled:')
    print(total_space)

    # Validation that optimal space = current space
    print('Should match total Current Space:')
    print(data['Current Space'].sum())

    #
    # DEBUG
    print(data[data['Store']==52])
    # END DEBUG

    # computes the minimum of Optimal Space for each category
    space_minimum = data.groupby('Category')['Optimal Space'].min()

    # determines zero's in the minimum for Optimal Space (at least one store is exiting this brand)
    categories_exit_idx = (space_minimum == 0)

    print(category_bounds)

    # setting Lower Space Bound for these category to zero
    # THERE IS SOME BETTER WAY OF CODING THIS ACCORDING TO PYCHARM!
    if np.sum(categories_exit_idx) > 0:
        category_bounds['Lower Space Bound'][categories_exit_idx] = 0
        if job_type == 'tiered':
            # increments the Upper Space Bound by 1 to account for extra 0th tier
            category_bounds['Upper Tier Bound'][categories_exit_idx] = category_bounds['Upper Tier Bound'][categories_exit_idx] + 1

    print(category_bounds)

    ###############################################################################################################
    #
    # Now we are using:
    #
    # category_bounds
    # stores
    # categories
    # space_levels

    ###############################################################################################################
    #
    #                   creates binary decision variables to be optimized
    #

    print('Creating LP Variable selected_tier')

    # Selected Tier(k) for store(i) and category(j) is Binary
    selected_tier = LpVariable.dicts('Selected Tier', (stores, categories, space_levels), 0, upBound=1, cat='Binary')

    # Created Tier(k) for category(j) is Binary
    if job_type == 'tiered':

        print('Creating LP Variable created_tier')

        created_tier = LpVariable.dicts('Created Tier', (categories, space_levels), 0, upBound=1, cat='Binary')

        # aux variable to track if a tier for a category has been to set to 0 already
        created_tier_zero_flag = np.zeros((num_categories,1), dtype=bool)


    ###############################################################################################################

    problem = LpProblem(job_name, LpMinimize)  # Define Optimization Problem/


    ###############################################################################################################
    #
    # Calculates the error as abs difference from optimal space and space level
    #

    # creates matrix of store x category to make the FOR loop easier for computing the error
    optimal_space = data.pivot(index='Store', columns='Category', values='Optimal Space')
    sales_penetration = data.pivot(index='Store', columns='Category', values='Sales %')

    print('Calculates error to be minimized')
    error = np.zeros((num_stores, num_categories, num_levels))
    for (i, store) in enumerate(stores):
        for (j, category) in enumerate(categories):
            for (k, level) in enumerate(space_levels):
                error[i][j][k] = np.absolute(optimal_space[category].iloc[i] - level)
    #
    # print('total error')
    # print(error.sum())


    # Cannot expand the optimal_space matrix to get the difference with space levels,
    # so staying for now with error
    #os = data['Optimal Space']
    #error2 = np.absolute(os[:, np.newaxis] - space_levels)
    #error3 = np.absolute(optimal_space[:, np.newaxis] - space_levels)

    ###############################################################################################################
    #
    #                                       Adds the Objective Function
    #

    print("Adding objective function")
    problem += lpSum([(selected_tier[store][category][level] * error[i][j][k]) \
                    for (i, store) in enumerate(stores) \
                        for (j, category) in enumerate(categories) \
                            for (k, level) in enumerate(space_levels)]), \
                            "Deviation from optimal space for store "+str(i)+" category "+str(j)+" tier "+str(k)


    ###############################################################################################################
    #
    #                                       Formulates the Constraints
    #

    ###############################################################################################################

    ###################################################################
    # Constraint 1
    #
    # At individual store level

    ################################
    # local balance constraint:
    # NEW: No balance back, just total
    for (i, store) in enumerate(stores):
        problem += lpSum(
            [(selected_tier[store][category][level]) * level \
             for (j, category) in enumerate(categories) \
                for (k, level) in enumerate(space_levels)]) \
                    == local_space_to_fill[store], \
                   "Location Balance " + str(store)

        # # Constraint 1a) - [Specification constraint 2.5.5.]
        #
        # #  the total space allocated to all products at location i must be at least the available store space - alpha(i)
        # problem += lpSum(
        #     [(selected_tier[store][category][level]) * level \
        #      for (j, category) in enumerate(categories) \
        #         for (k, level) in enumerate(space_levels)]) \
        #             >= local_space_to_fill[store] * (1 - local_balance_back_adjustment[store]), \
        #            "Location Balance Back Lower Bound - STR " + str(store)
        #
        # ################################
        # # Constraint 1b) - [Specification constraint 2.5.5.]
        #
        # #  the total space allocated to all products at location i must be at most the available store space + alpha(i)
        # problem += lpSum(
        #     [(selected_tier[store][category][level]) * level \
        #      for (j, category) in enumerate(categories) \
        #         for (k, level) in enumerate(space_levels)]) \
        #             <= local_space_to_fill[store] * (1 + local_balance_back_adjustment[store]), \
        #            "Location Balance Back Upper Bound - STR " + str(store)

    ################################
    # Constraint 2.5.2.
    # Exactly one Space level for each Store & Category
    for (i, store) in enumerate(stores):
        for (j, category) in enumerate(categories):
            problem += lpSum([selected_tier[store][category][level] \
                              for (k, level) in enumerate(space_levels)]) == 1, \
                              "Exactly one level for store " + str(store) + " and category " + str(category)

    ################################
    # Constraint 2.5.4.
    # Space Bound for each Store & Category
    for (i, store) in enumerate(stores):
        for (j, category) in enumerate(categories):
            # Lower Space Bound
            problem += lpSum([selected_tier[store][category][level] * level
                              for (k, level) in enumerate(space_levels)]) \
                              >= category_bounds['Lower Space Bound'].loc[category], \
                                "Space at Store "+ str(store) +" & Category "+category+" >= lower bound"
            # Upper Space Bound
            problem += lpSum([selected_tier[store][category][level] * level \
                              for (k, level) in enumerate(space_levels)]) \
                              <= category_bounds['Upper Space Bound'].loc[category], \
                                "Space at Store "+ str(store) +" & Category "+category+" <= upper bound"

    # End of Store level constraints
    ################################

    ###################################################################
    # Constraint 2
    #
    # Only For Tiered optimization
    #
    # At each category level

    if job_type == 'tiered':

        ################################
        # Constraint 2a) [Specification constraint 2.5.1.]
        # number of tiers >= lower tier count bound
        for (j, category) in enumerate(categories):
            problem += lpSum([created_tier[category][level] \
                              for (k, level) in enumerate(space_levels)]) \
                              >= category_bounds['Lower Tier Bound'].loc[category], \
                              "Lower Tier Bound for Category" + category

        ################################
        # Constraint 2b) [Specification constraint 2.5.1.]
        # number of tiers <= upper tier count bound
        for (j, category) in enumerate(categories):
            problem += lpSum([created_tier[category][level] \
                              for (k, level) in enumerate(space_levels)]) \
                              <= category_bounds['Upper Tier Bound'].loc[category], \
                              "Upper Tier Bound for Category" + category

        ################################
        # Constraint 2c) [Specification constraint 2.5.3.]

        # Relationship between Selected Tiers & Created Tiers:
        # Selected Tier can only be chosen if the corresponding Tier is a valid one for the category
        for (j, category) in enumerate(categories):
            for (k, level) in enumerate(space_levels):
                 problem += lpSum([selected_tier[store][category][level] \
                                   for (i, store) in enumerate(stores)])/len(stores) \
                                   <= created_tier[category][level], \
                                   "Selected tier "+str(k)+"('"+str(level)+"') must be valid for category"+category

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

    # Brand Exit Enhancement & Sales Penetration Constraint
    print('Adding Brand Exit constraints')

    # loops through all stores and categories
    for (i, store) in enumerate(stores):
        for (j, category) in enumerate(categories):

            # [Specification constraint 2.5.10.]

            # checks if category has optimal space set to zero
            if sales_penetration[category].loc[store] < sales_penetration_threshold:

                # creates the string to explain the constraint
                constraint_str = " for category " + category + ": Sales Penetration too low or Brand exit planned."

                # sets space to zero for category in store
                problem += selected_tier[store][category][0.0] == 1, \
                           "No space in store " + str(store) + constraint_str

                # adds corresponding tier constraint for Tiered Optimization
                if job_type == 'tiered':

                    if created_tier_zero_flag[j] == False:
                        problem += created_tier[category][0.0] == 1, \
                        "Tier 0 required " + constraint_str

                        # Sets flag to indicate constraint has been set already
                        # We want to avoid repeating the same contraint multiple times
                        created_tier_zero_flag[j] = True

                # ALSO DO WE NEED TO ADJUST THE TIER BOUNDS AND SHOULD WE ALLOW TO ADD AN EXTRA 0 TIER
                # AND COUNT ONLY NON-ZERO TIERS FOR THE TIER COUNT BOUNDS?

    ##################### end of loop over all stores
    # LpSolverDefault.msg = 1
    print("The problem has been formulated")

    ###############################################################################################################
    #S olving the Problem
    #problem.writeLP("Fixture_Optimization.lp")
    #problem.writeMPS(str(job_name)+".mps")

    # Solve the problem using Gurobi
    #status = problem.solve(pulp.GUROBI(mip=True, msg=True, MIPgap=.01, LogFile="/tmp/gurobi.log"))

    # local development uses CBC until
    status = problem.solve(pulp.PULP_CBC_CMD(msg=2))

    ###############################################################################################################
    # #Debugging
    print("#####################################################################")
    print(LpStatus[problem.status])
    print("#####################################################################")

    ###############################################################################################################
    if LpStatus[problem.status] == 'Optimal':

        # determines the allocated space from the decision variable selected_tier per store and category
        allocated_space = pd.DataFrame(index=stores, columns=categories)
        for (i, store) in enumerate(stores):
            for (j, category) in enumerate(categories):
                for (k, level) in enumerate(space_levels):
                    if value(selected_tier[store][category][level]) == 1:
                        allocated_space[category][store] = level

        # resets the index
        a = allocated_space.reset_index()

        # renames the first column to 'Store'
        a.rename(columns={'index': 'Store'}, inplace=True)
        # Ken's original code could be problematic
        #a.columns.values[0] = 'Store'

        b = pd.melt(a, id_vars=['Store'], var_name='Category', value_name='Result Space')

        # NOT SURE THIS IS NECESSARY
        #allocated_space = allocated_space.apply(lambda x: pd.to_numeric(x, errors='ignore'))

        data = data.merge(b, on=['Store', 'Category'], how='inner')

        return (LpStatus[problem.status], data, value(problem.objective), problem) #(longOutput)#,wideOutput)
    else:
        data['Result Space'] = 0

        return (LpStatus[problem.status], data, 0, problem)


