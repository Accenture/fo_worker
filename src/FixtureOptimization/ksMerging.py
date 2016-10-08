import pandas as pd
import numpy as np
import sys

# transactions=pd.read_csv("transactions_data.csv",header=None)
# space=pd.read_csv('fixture_data.csv',header=0,dtype={'Store': object},skiprows=[1])
# futureSpace=pd.read_csv('futureSpace_data.csv',header=0,dtype={'Store': object},skiprows=[1])
# brandExit=pd.read_csv('exit_data.csv',header=0,skiprows=[1])

def ksMerge(optimizationType,transactions,space,brandExit=None,futureSpace=None):
    if optimizationType == 'tiered':
        def brandExitMung(df, Stores, Categories):
            brand_exit = pd.DataFrame(index=Stores, columns=Categories)
            for (i, Store) in enumerate(Stores):
                for (j, Category) in enumerate(Categories):
                    if int(Store) in df[Category].unique():
                        brand_exit[Category].iloc[i] = 1
                    else:
                        brand_exit[Category].iloc[i] = 0
            return brand_exit
        Stores = space['Store']
        Metrics = transactions.loc[1, 1:9].reset_index(drop=True)
        Categories = transactions[[*np.arange(len(transactions.columns))[1::9]]].loc[0].reset_index(drop=True).values.astype(
            str)
        spaceData = pd.melt(space, id_vars=['Store', 'Climate', 'VSG'], var_name='Category', value_name='Historical Space')
        spaceData['Current Space'] = spaceData['Historical Space']
        print(spaceData.columns)
        def longTransaction(df, storeList, categories):
            df.loc[0, :] = categories
            df = pd.concat([storeList, df], axis=1)
            df.columns = df.loc[0,]
            lPiece = pd.melt(df[2::], id_vars=['Store'], var_name='Category',
                             value_name=pd.unique(df.loc[1].dropna().values)[0])
            return lPiece

        masterData = spaceData
        for (m, Metric) in enumerate(Metrics):
            masterData = pd.merge(left=masterData,
                                  right=longTransaction(transactions.loc[:, int(m + 1)::9], pd.DataFrame(transactions[0]),
                                                        Categories), on=['Store', 'Category'], how='outer')
        storeTotal = pd.DataFrame(masterData.groupby('Store')['Current Space'].sum()).reset_index()
        storeTotal.columns = ['Store', 'Store Space']
        storeTotal = storeTotal.sort_values(by='Store').reset_index(drop=True)
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
                futureSpace['Future Space'].loc[i] = storeTotal['Store Space'].loc[i] if pd.to_numeric(futureSpace['Future Space']).loc[i] == 0 or pd.isnull(pd.to_numeric(futureSpace['Future Space'])).loc[i] else futureSpace['Future Space'].loc[i]
            futureSpace['New Space'] = futureSpace['Future Space'] - futureSpace['Entry Space']
            masterData=pd.merge(masterData,futureSpace,on=['Store','VSG','Climate'])

        masterData = masterData.sort_values(by=['Store', 'Category']).reset_index(drop=True)
        mergeTrad = masterData
        if brandExit is None:
            masterData['Exit Flag'] = 0
        else:
            brandExit=pd.melt(brandExitMung(brandExit,Stores,Categories).reset_index(),id_vars=['Store'],var_name='Category',value_name='Exit Flag')
            brandExit=brandExit.sort_values(by=['Store','Category']).reset_index(drop=True)
            for i in range(0,len(mergeTrad)):
                if brandExit['Exit Flag'].loc[i] == 1:
                    mergeTrad.loc[i,5::]=0
            masterData=pd.merge(masterData,brandExit,on=['Store','Category'],how='inner')
            mergeTrad=pd.merge(mergeTrad,brandExit,on=['Store','Category'],how='inner')
        print('There are ' + str(len(masterData[masterData['Exit Flag'] == 1])) + ' brand exits')
        # masterData.to_csv('mergedData.csv',sep=',',index=False)
        masterData=masterData.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        mergeTrad = mergeTrad.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        print('Finished Data Merging')
        # masterData.to_csv('MacroMerge.csv',sep=',')
            # input('Stop')
    return (masterData,mergeTrad)