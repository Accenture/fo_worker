import math
from pulp import *
import numpy as np
import pandas as pd
import logging as logging

def debugTiers(Stores, Categories, Levels, st, ct, tierCounts, status):
    NegativeCount = 0
    LowCount = 0
    TrueCount = 0
    OneCount = 0
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                if value(st[Store][Category][Level]) == 1:
                    # logging.info(st[Store][Category][Level]) #These values should only be a one or a zero
                    OneCount += 1
                elif value(st[Store][Category][Level]) > 0:
                    # logging.info(st[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                    TrueCount += 1
                elif value(st[Store][Category][Level]) == 0:
                    # logging.info(value(st[Store][Category][Level])) #These values should only be a one or a zero
                    LowCount += 1
                elif value(st[Store][Category][Level]) < 0:
                    # logging.info(st[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                    NegativeCount += 1
    if tierCounts is not None:
        ctNegativeCount = 0
        ctLowCount = 0
        ctTrueCount = 0
        ctOneCount = 0

        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                if value(ct[Category][Level]) == 1:
                    # logging.info(value(ct[Store][Category][Level])) #These values should only be a one or a zero
                    ctOneCount += 1
                elif value(ct[Category][Level]) > 0:
                    # logging.info(ct[Store][Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                    ctTrueCount += 1
                elif value(ct[Category][Level]) == 0:
                    # logging.info(value(ct[Category][Level])) #These values should only be a one or a zero
                    ctLowCount += 1
                elif value(ct[Category][Level]) < 0:
                    # logging.info(ct[Category][Level],"Value is: ",value(st[Store][Category][Level])) #These values should only be a one or a zero
                    ctNegativeCount += 1

    logging.info("#####################################################################")
    logging.info("Status:", status)
    logging.info("#####################################################################")

    logging.info("---------------------------------------------------")
    logging.info("For Selected Tiers")
    logging.info("Number of Negatives Count is: ", NegativeCount)
    logging.info("Number of Zeroes Count is: ", LowCount)
    logging.info("Number Above 0 and Below 1 Count is: ", TrueCount)
    logging.info("Number of Selected Tiers: ", OneCount)
    logging.info("---------------------------------------------------")
    if tierCounts is not None:
        logging.info("For Created Tiers")
        logging.info("Number of Negatives Count is: ", ctNegativeCount)
        logging.info("Number of Zeroes Count is: ", ctLowCount)
        logging.info("Number Above 0 and Below 1 Count is: ", ctTrueCount)
        logging.info("Number of Created Tiers: ", ctOneCount)
        logging.info("Creating Outputs")
    return




def roundValue(cVal, increment):
    if np.mod(round(cVal, 3), increment) > increment / 2:
        cVal = round(cVal, 3) + (increment - (np.mod(round(cVal, 3), increment)))
    else:
        cVal = round(cVal, 3) - np.mod(round(cVal, 3), increment)
    return cVal


def searchParam(search, jobName):
    if search in jobName:
        begin = jobName.find(search)
        length = 0
        for char in jobName[(len(search) + begin)::]:
            try:
                int(char)
                length = length + 1
            except:
                break
        try:
            sParam = int(jobName[(len(search) + begin):(len(search) + begin + length)]) / 100
            logging.info('{} has been changed to {}'.format(search, sParam))
            return sParam
        except:
            return True
    else:
        return None


def curveDataOutput(mergedPreOptCF, Stores, Categories, Levels, tierCounts, locBalBackBoundAdj, locSpaceToFill,
                    objective):
    """
    This was initially created late night with Mark so the code is horrid.
    :param mergedPreOptCF:
    :param Stores:
    :param Categories:
    :param Levels:
    :param tierCounts:
    :param locBalBackBoundAdj:
    :param locSpaceToFill:
    :param objective:
    :return:
    """

    def createIndexDict(df, k_max, Stores, Categories):
        kvals = dict()
        opal = 0
        for (j, Category) in enumerate(Categories):
            for (i, Store) in enumerate(Stores):
                # Get Parameters
                min_val = df['Lower_Limit'].loc[Store, Category]
                max_val = df['Upper_Limit'].loc[Store, Category]
                increment = df['Increment Size'].loc[Store, Category]

                c = min_val  # Initialize First Index Value

                for k in range(int(k_max)):
                    if c > max_val:
                        kvals[Category, Store, k] = 10000
                    else:
                        kvals[Category, Store, k] = c
                        c = c + increment
                    if opal % 10000 == 0:
                        print(opal)
                    opal = opal + 1
        return kvals

    mergedPreOptCF['Num Indeces'] = (mergedPreOptCF["Upper_Limit"] - mergedPreOptCF["Lower_Limit"]) / \
                                    mergedPreOptCF[
                                        "Increment Size"] + 1
    kMax = max(mergedPreOptCF['Num Indeces'])
    indexDict = createIndexDict(mergedPreOptCF, int(kMax), Stores, Categories)

    # def curveTableCreate (Stores,Categories,Levels,indexDict,locSpaceToFill,mergedPreOptCF,tierCounts,objective):
    out_df = pd.DataFrame(columns=["i", "j", "k", "f(i,j,k)", "x(j)", "y(j)", "l(i,j)", "u(i,j)", "a", "b", "w(i)"])
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                f_ijk = objective[i][j][k]
                x_j = tierCounts['tierLower'][Category]
                y_j = tierCounts['tierUpper'][Category]
                l_ij = mergedPreOptCF["Lower_Limit"].loc[Store, Category]
                u_ij = mergedPreOptCF["Upper_Limit"].loc[Store, Category]
                a_val = locSpaceToFill[Store] * (1 - locBalBackBoundAdj[Store])
                b_val = locSpaceToFill[Store] * (1 + locBalBackBoundAdj[Store])
                w_i = locSpaceToFill[Store]
                row_data = [Store, Category, indexDict[Category, Store, Level], f_ijk, x_j, y_j, l_ij, u_ij, a_val,
                            b_val,
                            w_i]
                out_df.loc[len(out_df)] = row_data
    return out_df


def createLevels(mergedPreOptCF, increment):
    """
    Creates all possible levels given the global maximum & global minimum for space with the increment size
    :param mergedPreOptCF: Contains all of the merged data from previous scripts
    :param increment: Determined by the user around
    :return: Returns a vector of all possible levels
    """

    minLevel = mergedPreOptCF.loc[:, 'Lower_Limit'].min()
    maxLevel = mergedPreOptCF.loc[:, 'Upper_Limit'].max()
    Levels = list(np.arange(minLevel, maxLevel + increment, increment))
    if 0.0 not in Levels:
        Levels.append(np.abs(0.0))

    # logging.info(Levels)  # for unit testing

    return Levels


# Helper function for createSPUByLevel function, to forecast weighted combination of sales, profit, and units
# str_cat is the row of the curve-fitting output for an individual store and category
# variable can take on the values of "Sales", "Profit", or "Units"
def forecast(str_cat, space, variable):
    """
    Forecasts estimated Sales, Profit, or Sales Units
    :param str_cat:
    :param space:
    :param variable:
    :return:
    """
    if space < str_cat["Scaled_BP_" + variable]:
        value = space * (str_cat["Scaled_Alpha_" + variable] * (math.erf(
            (str_cat["Scaled_BP_" + variable] - str_cat["Scaled_Shift_" + variable]) / ((
                math.sqrt(2) * str_cat["Scaled_Beta_" + variable])))) / str_cat["Scaled_BP_" + variable])
    else:
        value = str_cat["Scaled_Alpha_" + variable] * math.erf(
            (space - str_cat["Scaled_Shift_" + variable]) / (math.sqrt(2) * str_cat["Scaled_Beta_" + variable]))

    return round(value, 2)


# Helper function for optimize function, to create objective function of SPU by level for Enhanced optimizations
def createNegSPUByLevel(Stores, Categories, Levels, curveFittingOutput, enhMetrics):
    """
    Creates the objective for enhanced optimizations in which the goal to minimize the weighted combination of sales, profits, and units multiplied by -1
    :param Stores:
    :param Categories:
    :param Levels:
    :param curveFittingOutput:
    :param enhMetrics:
    :return:
    """
    # Create n-dimensional array to store Estimated SPU by level
    est_neg_spu_by_lev = np.zeros((len(Stores), len(Categories), len(Levels)))

    sU = "Sales"
    pU = "Profit"
    uU = "Units"
    sL = "sales"
    pL = "profits"
    uL = "units"

    logging.info('forecasting outputs')
    # Calculate SPU by level
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                str_cat = curveFittingOutput.loc[Store, Category]
                est_neg_spu_by_lev[i][j][k] = - (
                    (enhMetrics[sL] / 100) * forecast(str_cat, Level, sU) + (enhMetrics[pL] / 100) * forecast(str_cat,
                                                                                                              Level,
                                                                                                              pU) + (
                        enhMetrics[uL] / 100) * forecast(str_cat, Level, uU))
    logging.info('finished forecasting')
    return est_neg_spu_by_lev


# Helper function for optimize function, to create objective function of error by level for Traditional optimizations
def createErrorByLevel(Stores, Categories, Levels, mergedCurveFitting):
    """
    Creates the objective for traditional optimizations in which the goal to minimize the distance between the optimal & result space
    :param Stores: Vector of Stores within the transactions data
    :param Categories: Vector of Categories within the transactions data
    :param Levels:
    :param mergedCurveFitting:
    :return:
    """
    # Create n-dimensional array to store error by level
    error = np.zeros((len(Stores), len(Categories), len(Levels)))

    # Calculate error by level
    for (i, Store) in enumerate(Stores):
        for (j, Category) in enumerate(Categories):
            for (k, Level) in enumerate(Levels):
                error[i][j][k] = np.absolute(mergedCurveFitting.loc[Store, Category]["Optimal Space"] - Level)
    return error


# Adjust location balance back tolerance limit so that it's at least 2 increments
def adjustForOneIncr(row, bound, increment):
    """
    Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
    :param row: Individual row of Total Space Available in Store
    :param bound: Percent Bounding for Balance Back
    :param increment: Increment Size Determined by the User in the UI
    :return: Returns an adjusted vector of percentages by which individual store space should be held
    """
    return max(bound, increment / row)


# Adjust location balance back tolerance limit so that it's at least 2 increments
def adjustForTwoIncr(row, bound, increment):
    """
    Returns a vector with the maximum percent of the original total store space between two increment sizes and 10 percent of the store space
    :param row: Individual row of Total Space Available in Store
    :param bound: Percent Bounding for Balance Back
    :param increment: Increment Size Determined by the User in the UI
    :return: Returns an adjusted vector of percentages by which individual store space should be held
    """
    return max(bound, (2 * increment) / row)