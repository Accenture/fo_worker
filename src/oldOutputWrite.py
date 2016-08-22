# -*- coding: utf-8 -*-
"""
Created on Sat Aug 20 10:51:57 2016

@author: kenneth.l.sylvain
"""
import pandas as pd

'''
Stores=sales.index.values
Categories=sales.columns.values#list(range(1,len(list(fixt_data[0,3::2]))+1,1))
minLevel= min(optAmt.min())
maxLevel= max(optAmt.max())
increment = .5
Levels=list(np.arange(minLevel,maxLevel+increment,increment))
'''

#Converting to Pandas Dataframe so that indexing functions can be used
opt_amt2=pd.DataFrame(data=opt_amt,index=Stores,columns=Categories)
adj_p2=pd.DataFrame(data=adj_p,index=Stores,columns=Categories)
optimal_space.columns=Categories
        
'''
#Create ideal Store Lookup dataframe
Historical=pd.read_csv("C:\\Users\\kenneth.l.sylvain\\Documents\\Kohls\\Fixture Optimization\\testData\\testSpace_FakeData.csv")
df=Historical[[0,1,2]].drop([0]).set_index('Store').T.to_dict()

Historical=optimal_space
#Dataframe to Dictionary
Historical =store_lookup.set_index('Store').T.to_dict()
'''
Optimal=opt_amt2
Penetration=adj_p2
df=df.drop([0])
Historical=pd.read_csv("C:\\Users\\kenneth.l.sylvain\\Documents\\Kohls\\Fixture Optimization\\testData\\testSpace_FakeData.csv")
#Long Data
#Write out climate & VSG information
def createLong(Stores,Categories,Optimal,Penetration,Historical):
    storeDict=Historical[[0,1,2]].drop([0]).set_index('Store').T.to_dict()
    Historical=Historical.drop([0]).drop(Historical.columns[[1,2]],axis=1).set_index('Store')
    l=0
    lOutput=pd.DataFrame(index=np.arange(len(Stores)*len(Categories)),columns=["Store","Climate","VSG","Category","Result Space","Optimal Space","Penetration","Historical Space"])
    for (i,Store) in enumerate(Stores):    
        for (j,Category) in enumerate(Categories):
            lOutput["Store"].iloc[l]=Store
            lOutput["Category"].iloc[l] = Category
            lOutput["Optimal Space"].iloc[l] = Optimal[Category].iloc[i]        
            lOutput["Penetration"].iloc[l] = Penetration[Category].iloc[i]
            #lOutput["Climate"].iloc[l] = 
            #lOutput["VSG"].iloc[l] = 
            #lOutput["Historical Space"].iloc[l] = Historical[Category].iloc[i]
            for (k,Level) in enumerate(Levels):        
                if value(st[Store][Category][Level])== 1:
                    lOutput["Result Space"].iloc[l] = Level
            l=l+1
    return lOutput

lOutput['Climate']=lOutput.Store.apply(lambda x: (storeDict.get(x)).get('VSG'))
            #lOutput["Climate"].iloc[l]=(storeDict.get(166)).get('Climate')
            #lOutput["VSG"].iloc[l]=storeDict.get([Store,1])


testOut=createLong(Stores,Categories,opt_amt2,adj_p2,Historical)

##################################################################################################
#Write out climate & VSG information
def createLongOld(Stores,Categories,lOutput,Optimal,Penetration,Historical):
    l=0
    lOutput=pd.DataFrame(index=np.arange(len(Stores)*len(Categories)),columns=["Store","Category","Result Space","Optimal Space","Penetration","Historical Space"])
    for (i,Store) in enumerate(Stores):    
        for (j,Category) in enumerate(Categories):
            lOutput["Store"].iloc[l]=Store
            lOutput["Category"].iloc[l] = Category
            lOutput["Optimal Space"].iloc[l] = opt_amt2[Category].iloc[i]        
            lOutput["Penetration"].iloc[l] = adj_p2[Category].iloc[i]
            lOutput["Historical Space"].iloc[l] = optimal_space[Category].iloc[i]
            for (k,Level) in enumerate(Levels):        
                if value(st[Store][Category][Level])== 1:
                    lOutput["Result Space"].iloc[l] = Level
            l=l+1
    return lOutput
    
#Wide Data
#Write out climate & VSG information from input file
#Re-order the columns
def createWide(Optimal,Penetration,Results,Historical):
    Optimal.columns = [str(col) + '_optimal' for col in Categories]
    Penetration.columns = [str(col) + '_penetration' for col in Categories]
    #result.columns = [str(col) + '_result' for col in Categories]
    Historical.columns = [str(col) + '_current' for col in Categories]
    wOutput=Results.append([Optimal,Penetration,Historical])
    return wOutput
    #                Output['Role'][j] = Position_Dict.get(roster['Position'][j],'null')                
