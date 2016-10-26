# from scipy.special import erf
# from gurobipy import *
import math
from pulp import *
import numpy as np
import pandas as pd


# Run tiered optimization algorithm
def optimize2():

    NewOptim = pulp.COIN_CMD.readsol_MPS(filename='/Users/tkmabjs/Downloads/23459-pulp.mps')
    # Solve the problem using open source solver
    NewOptim.solve(pulp.PULP_CBC_CMD(msg=2, threads=4, maxSeconds=115200))
    # solver = "CBC" #for unit testing

    # Solve the problem using Gurobi
    # NewOptim.solve(pulp.GUROBI())
    # solver = "Gurobi" #for unit testing

    # Time stamp for optimization solve time
    # solve_end_seconds = dt.datetime.today().hour*60*60 + dt.datetime.today().minute*60 + dt.datetime.today().second
    # solve_seconds = solve_end_seconds - start_seconds
    # print("Time taken to solve optimization was:" + str(solve_seconds)) #for unit testing

    # Debugging
    print("#####################################################################")
    print(LpStatus[NewOptim.status])
    print("#####################################################################")
    # Debugging
    NegativeCount = 0
    LowCount = 0
    TrueCount = 0
    OneCount = 0
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                if value(st[Store][Category][Level]) == 1:
                    # print(st[Store][Category][Level]) #These values should only be a one or a zero
                    OneCount += 1
                elif value(st[Store][Category][Level]) > 0:
                    # print(st[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                    TrueCount += 1
                elif value(st[Store][Category][Level]) == 0:
                    # print(value(st[Store][Category][Level])) #These values should only be a one or a zero
                    LowCount += 1
                elif value(st[Store][Category][Level]) < 0:
                    # print(st[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                    NegativeCount += 1

    ctNegativeCount = 0
    ctLowCount = 0
    ctTrueCount = 0
    ctOneCount = 0

    for (j, Category) in enumerate(Categories):
        for (k, Level) in enumerate(Levels):
            if value(ct[Category][Level]) == 1:
                # print(value(ct[Store][Category][Level])) #These values should only be a one or a zero
                ctOneCount += 1
            elif value(ct[Category][Level]) > 0:
                # print(ct[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                ctTrueCount += 1
            elif value(ct[Category][Level]) == 0:
                # print(value(ct[Category][Level])) #These values should only be a one or a zero
                ctLowCount += 1
            elif value(ct[Category][Level]) < 0:
                # print(ct[Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                ctNegativeCount += 1

    print("Status:", LpStatus[NewOptim.status])
    print("---------------------------------------------------")
    print("For Selected Tiers")
    print("Number of Negatives Count is: ", NegativeCount)
    print("Number of Zeroes Count is: ", LowCount)
    print("Number Above 0 and Below 1 Count is: ", TrueCount)
    print("Number of Selected Tiers: ", OneCount)
    print("---------------------------------------------------")
    print("For Created Tiers")
    print("Number of Negatives Count is: ", ctNegativeCount)
    print("Number of Zeroes Count is: ", ctLowCount)
    print("Number Above 0 and Below 1 Count is: ", ctTrueCount)
    print("Number of Created Tiers: ", ctOneCount)
    print("Creating Outputs")

    Results = pd.DataFrame(index=Stores, columns=Categories)
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                if value(st[Store][Category][Level]) == 1:
                    Results[Category][Store] = Level

    Results.reset_index(inplace=True)
    Results.columns.values[0] = 'Store'
    # Results.rename(
    #     columns={'level_0': 'Store'},
    #     inplace=True)
    Results = pd.melt(Results.reset_index(), id_vars=['Store'], var_name='Category', value_name='Result Space')
    Results = Results.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    mergedPreOptCF.reset_index(inplace=True)
    mergedPreOptCF.rename(columns={'level_0': 'Store', 'level_1': 'Category'}, inplace=True)
    mergedPreOptCF = pd.merge(mergedPreOptCF, Results, on=['Store', 'Category'])
    return (LpStatus[NewOptim.status], mergedPreOptCF)


if __name__ == '__main__':
    #Did this just happen...
    optimize2()

