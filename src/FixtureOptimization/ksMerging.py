import pandas as pd
import numpy as np
import sys

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
        # masterData['Future Space'] = 0
        # print(masterData['Future Space'].loc[0])
        brandExit=brandExit.fillna(float(0))
        # futureSpace=futureSpace.fillna(float(0))
        print('*****************************')

        if futureSpace is None:
            masterData['Future Space'] = masterData['Store Space']
            masterData['Entry Space'] = 0
            masterData['New Space'] = masterData['Store Space']
        else:
            masterData = pd.merge(masterData, futureSpace, on=['Store','Climate','VSG'])
            for i in range(0,len(masterData)):
                # masterData['Future Space'].loc[i] = masterData['Store Space'].loc[i] if \
                # masterData['Future Space'].loc[i] == 0.0 or \
                # pd.isnull(masterData['Future Space'].loc[i]) is True else masterData['Future Space'].loc[i]
                if pd.to_numeric(masterData['Future Space'].iloc[i]) == 0 or pd.isnull(
                        pd.to_numeric(masterData['Future Space'].iloc[i])):         # Need to replace this with an apply
                    masterData['Future Space'].iloc[i] = masterData['Store Space'].loc[i]
            masterData['New Space'] = masterData['Future Space'] - masterData['Entry Space']
        if brandExit is None:
            print("No brand exit")
        else:
            def brandExitMung(df, Stores, Categories):
                brand_exit = pd.DataFrame(index=Stores, columns=Categories)
                for (i, Store) in enumerate(Stores):
                    for (j, Category) in enumerate(Categories):
                        brand_exit[Category].iloc[i] = 1 if float(Store) in df[Category].unique() else 0
                return brand_exit
            brandExit = pd.melt(brandExitMung(brandExit, Stores, Categories).reset_index(), id_vars=['Store'],
                                 var_name='Category', value_name='Exit Flag')
            masterData = pd.merge(masterData, brandExit, on=['Store', 'Category'], how='outer')
            print(type(masterData['Exit Flag'].loc[0]))
            print(pd.unique(masterData['Exit Flag']))
            for i in masterData.index:
                if masterData['Exit Flag'].loc[i]: masterData.loc[i, 4::] = 0
            print(masterData)
            input('Checkpoint')
    else:
        print("Stop It")
    return masterData