import pandas as pd
import numpy as np
import sys
import traceback
import logging

# transactions=pd.read_csv("transactions_data.csv",header=None)
# space=pd.read_csv('fixture_data.csv',header=0,dtype={'Store': object},skiprows=[1])
# futureSpace=pd.read_csv('futureSpace_data.csv',header=0,dtype={'Store': object},skiprows=[1])
# brandExit=pd.read_csv('exit_data.csv',header=0,skiprows=[1])

def dataMerge(jobName,jobType,optimizationType,transactions,space,brandExit=None,futureSpace=None):
    try:
        space.rename(columns={'VSG ': 'VSG'}, inplace=True)
        Categories = transactions[[*np.arange(len(transactions.columns))[1::9]]].loc[0].reset_index(
            drop=True).values.astype(str)
        Stores = space['Store']
        def transactionMerge(spaceData,transactions,Categories):
            Metrics = transactions.loc[1, 1:9].reset_index(drop=True)
            def longTransaction(df, storeList, categories):
                """
                Assigns names for each financial metric in the Transaction Data Set and converts it to a long table
                :param df: Individual Wide Table of Transaction Metric
                :param storeList: List of Stores
                :param categories: List of Categories
                :return: Returns a long table for an individual metric
                """
                df.loc[0, :] = categories
                df = pd.concat([storeList, df], axis=1)
                df.columns = df.loc[0,]
                lPiece = pd.melt(df[2::], id_vars=['Store'], var_name='Category',
                                 value_name=pd.unique(df.loc[1].dropna().values)[0])
                print(lPiece.columns)
                return lPiece

            masterData = spaceData.copy()
            # Loop to merge individual long tables into a single long table
            for (m, Metric) in enumerate(Metrics):
                masterData = pd.merge(left=masterData,
                                      right=longTransaction(transactions.loc[:, int(m + 1)::9], pd.DataFrame(transactions[0]),
                                                            Categories), on=['Store', 'Category'], how='outer')
            return masterData
        print(jobType)
        if jobType == 'tiered' or jobType == 'unconstrained':
            # Define the function to convert Brand Exit Information to Binary Values
            def brandExitMung(df, Stores, Categories):
                """
                Converts Brand Exit Table in to a Wide Table of Binary Values
                :param df: Initial Brand Exit Data Frame
                :param Stores: List of Stores
                :param Categories: List of Categories
                :return:
                """
                brand_exit = pd.DataFrame(index=Stores, columns=Categories)
                for (i, Store) in enumerate(Stores):
                    for (j, Category) in enumerate(Categories):
                        if int(Store) in df[Category].unique():
                            brand_exit[Category].iloc[i] = 1
                        else:
                            brand_exit[Category].iloc[i] = 0
                return brand_exit
            spaceData = pd.melt(space, id_vars=['Store', 'Climate', 'VSG'], var_name='Category', value_name='Historical Space')
            spaceData['Current Space'] = spaceData['Historical Space']
            masterData = transactionMerge(spaceData,transactions,Categories)

            # Create a Vector of Total Space by Store
            storeTotal = pd.DataFrame(masterData.groupby('Store')['Current Space'].sum()).reset_index()
            storeTotal.columns = ['Store', 'Store Space']
            storeTotal = storeTotal.sort_values(by='Store').reset_index(drop=True)

            # Code to handle the usage of Future Space Files
            if futureSpace is None:
                print("we don't have future space")
                storeTotal['Future Space']=storeTotal['Store Space']
                storeTotal['Entry Space']=0
                storeTotal['New Space'] = storeTotal['Store Space'] - storeTotal['Entry Space']
                masterData=pd.merge(masterData,storeTotal,on=['Store'])
            else:
                print('we have future space')
                futureSpace = futureSpace.sort_values(by='Store').reset_index(drop=True)
                futureSpace=pd.merge(storeTotal,futureSpace,on=['Store'],how='inner')
                print('in future space loop')
                futureSpace['Entry Space'].fillna(0,inplace=True)
                for (i,Store) in enumerate(Stores):
                    futureSpace['Future Space'].loc[i] = storeTotal['Store Space'].loc[i] if \
                    pd.to_numeric(futureSpace['Future Space']).loc[i] == 0 or \
                    pd.isnull(pd.to_numeric(futureSpace['Future Space'])).loc[i] else futureSpace['Future Space'].loc[i]
                futureSpace['New Space'] = futureSpace['Future Space'] - futureSpace['Entry Space']
                masterData=pd.merge(masterData,futureSpace,on=['Store','VSG','Climate'])

            masterData = masterData.sort_values(by=['Store', 'Category']).reset_index(drop=True)

            # Handling the upload of a Brand Exit
            if brandExit is None:
                masterData['Exit Flag'] = 0
                mergeTrad = masterData.copy()
            else:
                mergeTrad = masterData.copy()
                brandExit=pd.melt(brandExitMung(brandExit,Stores,Categories).reset_index(),id_vars=['Store'],var_name='Category',value_name='Exit Flag')
                brandExit=brandExit.sort_values(by=['Store','Category']).reset_index(drop=True)
                for i in range(0,len(mergeTrad)):
                    if brandExit['Exit Flag'].loc[i] == 1:
                        mergeTrad.loc[i,5::]=0
                masterData=pd.merge(masterData,brandExit,on=['Store','Category'],how='inner')
                mergeTrad=pd.merge(mergeTrad,brandExit,on=['Store','Category'],how='inner')
            print('There are ' + str(len(masterData[masterData['Exit Flag'] == 1])) + ' brand exits')

            # Make sure that all values that should be numeric are numeric
            masterData=masterData.apply(lambda x: pd.to_numeric(x, errors='ignore'))
            mergeTrad = mergeTrad.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        else:
            masterData = transactionMerge(space, transactions, Categories)
            masterData['Exit Flag']=0
            masterData.rename(columns={'Result Space': 'New Space'})
        print(masterData.columns)
    except Exception as e:
        logging.exception('A thing')
        traceback.print_exception() # syntactically incorrect since this function call is expecting 3 args.

    print('Finished Data Merging')
    return (masterData,mergeTrad)