import pandas as pd
import numpy as np

# transactions=pd.read_csv("transactions_data.csv",header=None)
# space=pd.read_csv('fixture_data.csv',header=0,dtype={'Store': object},skiprows=[1])
# futureSpace=pd.read_csv('futureSpace_data.csv',header=0,dtype={'Store': object},skiprows=[1])
# brandExit=pd.read_csv('exit_data.csv',header=0,skiprows=[1])

def ksMerge(optimizationType,transactions,space,brandExit,futureSpace):
    if optimizationType == 'tiered':
        Stores=space['Store'].astype(int)
        Metrics = transactions.loc[1, 1:9].reset_index(drop=True)
        Categories = transactions[[*np.arange(len(transactions.columns))[1::9]]].loc[0].reset_index(
            drop=True).values.astype(str)
        spaceData = pd.melt(space, id_vars=['Store', 'Climate', 'VSG'], var_name='Category', value_name='Current Space')

        def longTransaction(df, storeList, categories):
            df.loc[0, :] = categories
            df = pd.concat([storeList, df], axis=1)
            df.columns = df.loc[0,]
            lPiece = pd.melt(df[2::], id_vars=['Store'], var_name='Category',
                             value_name=pd.unique(df.loc[1].dropna().values)[0])
            return lPiece

        masterData = spaceData
        for (m, Metric) in enumerate(Metrics):
            masterData = pd.merge(left=masterData, right=longTransaction(transactions.loc[:, int(m + 1)::9],
                                                                         pd.DataFrame(transactions[0]), Categories),
                                  on=['Store', 'Category'], how='outer')
        storeTotal = pd.DataFrame(masterData.groupby('Store')['Current Space'].sum()).reset_index()
        storeTotal.columns = ['Store', 'Store Space']
        masterData = pd.merge(masterData, storeTotal, on='Store')
        masterData['Store']=pd.to_numeric(masterData['Store'])
        print(type(masterData['Store'].loc[0]))
        print(type(futureSpace['Store'].loc[0]))
        # masterData['Future Space'] = 0
        # print(masterData['Future Space'].loc[0])
        brandExit=brandExit.fillna(float(0))
        futureSpace=futureSpace.fillna(float(0))
        print('*****************************')
        if futureSpace is None:
            masterData['Future Space'] = masterData['Store Space']
            masterData['Entry Space'] = 0
        else:
            masterData = pd.merge(masterData, futureSpace, on=['Store','Climate','VSG'])
            # print(masterData.head())
            for (i, Store) in enumerate(Stores):
                masterData['Future Space'].loc[i] = masterData['Store Space'].loc[i] if \
                masterData['Future Space'].loc[i] == 0 or \
                pd.isnull(pd.to_numeric(masterData['Future Space'])).loc[i] else masterData['Future Space'].loc[i]
            masterData['New Space'] = masterData['Future Space'] - masterData['Entry Space']
        masterData['Brand Exit'] = 0

        if brandExit is None:
            print("No brand exit")
        else:
            masterData['Brand Exit']=0
            masterData.set_index(['Store','Category'],inplace=True)
        #     print(masterData.index)
            for (i,Store) in enumerate(Stores):
                for (j,Category) in enumerate(Categories):
        #             print(Category)
                    if str(Store) in pd.unique(brandExit[Category].values):
                        masterData['Brand Exit'].loc[int(Store),Category] = 1
                        masterData[4::]=0
                    else:
                        masterData['Brand Exit'].loc[int(Store),Category] = 0
    else:
        print("Stop It")
    return masterData