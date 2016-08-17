import pandas as pd
import numpy as np

def tierKeyCreate(df):
    '''
    tierKeyCreate(dataframe)

    Takes in a dataframe and creates the key associated with the tiered results 
    ''''
    headers = iter(df.columns.values) #grabs all the brand headers from the csv
    next(headers) #Skip first column
    dataFrame = df.drop(df.columns.values, axis=1) #creates the empty dataframe
    # Inserted new columns in dataframe
    dataFrame.insert(0, 'Category_Name', '', allow_duplicates=False)
    dataFrame.insert(1, 'Tier_Value', '', allow_duplicates=False)
    dataFrame.insert(2, 'Space_Value', '', allow_duplicates=False)
    num_row = 0
    for head in headers:
        #df1 = df.loc[:,head].unique() #Get each column values in Series
        #df1 = np.sort(df1).tolist()
        spaceVals = sorted(df.loc[:,head].unique().tolist()) #Get unique sorted value of that column
        #print(df2) 
        #for d1 in df1:
        for val in spaceVals:
                #if d1 == d2:
            dataFrame.loc[num_row, 'Category_Name'] = head
            dataFrame.loc[num_row, 'Tier_Value'] = "Tier {0}".format(str(spaceVals.index(val) + 1))
            dataFrame.loc[num_row, 'Space_Value'] = val
            num_row += 1
    return dataFrame

def tierDef(df):
    storeVals = [] #Hold Store Values
    for storeVal in df['Store']: 
        storeVals.append(storeVal)
    df = df.drop(['Store'], axis=1) #Drop Store Column
    for (d,column) in enumerate(df.columns):
        tierVals = df[column].unique() #Get each unique spaceValue
        tierVals = np.sort(tierVals).tolist() #Sorts Unique Space values Ascending Order
        spaceVals = df.loc[:,column].tolist() #All Space Values in each column to be assigned a tier
        #headers = df.columns.values #Get each column values in Series
        num_row = 0 #tracks df location 
        for spaceVal in spaceVals: #invididual value
            for tierVal in tierVals: #unique values per column
               if spaceVal == tierVal: 
                   df.loc[num_row, str(column)] = "Tier {0}".format(str(tierVals.index(tierVal) + 1),"") #assigns tier value
                   num_row += 1
    df.insert(0, 'Store', storeVals, allow_duplicates=False)
    return df    