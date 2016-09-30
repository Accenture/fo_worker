import pandas as pd

def primaryMung(df):
    df.columns = df.iloc[0].values
    df.drop(df.index[[0, 1]], axis=0, inplace=True)
    df.set_index(df.Store.values.astype(int), inplace=True)
    return df

def brandExitMung(df,Stores,Categories):
    # df.columns = df.iloc[0].values
    # df.drop(df.index[[0, 1]], axis=0, inplace=True)
    # df=df.reset_index(drop=True)
    brand_exit = pd.DataFrame(index=Stores,columns=Categories)
    for (i,Store) in enumerate(Stores):
        for (j,Category) in enumerate(Categories):
            if str(Store) in pd.unique(df[Category].values):
                brand_exit[Category].iloc[i] = 1
            else:
                brand_exit[Category].iloc[i] = 0
    return brand_exit

def mergePreOptCF(cfOutput,preOpt):
    #For enhanced max financials optimizations, single store optimals are not calculated and these columns will be created in the long table but left blank
    if preOpt is None :
        cfOutput["Penetration"] = ""
        cfOutput["Optimal Space"] = ""
        mergedPreOptCF = cfOutput
    #For traditional min error optimizations, single store optimals are created in preoptimize and are merged here into the long table
    else :
        penetration=preOpt[0]
        opt_amt=preOpt[1]
        pen_long = pd.DataFrame(penetration.unstack()).swaplevel()
        pen_long.rename(columns = {pen_long.columns[-1]:"Penetration"},inplace = True)
        opt_long = pd.DataFrame(opt_amt.unstack()).swaplevel()
        opt_long.rename(columns = {opt_long.columns[-1]:"Optimal Space"},inplace = True)
        mergedPreOptCF = pd.concat([cfOutput,pen_long,opt_long],axis=1)
    return mergedPreOptCF