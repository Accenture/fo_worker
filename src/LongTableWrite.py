# -*- coding: utf-8 -*-
"""
Created on Sat Aug 20 21:36:14 2016

@author: kenneth.l.sylvain
"""
opt_amt2=pd.DataFrame(data=opt_amt,index=Stores,columns=Categories)
adj_p2=pd.DataFrame(data=adj_p,index=Stores,columns=Categories)
optimal_space.columns=Categories

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
        
def createWide(Optimal,Penetration,Results,Historical,Categories):
    opt_amt2.columns = [str(col) + '_optimal' for col in Categories]
    adj_p2.columns = [str(col) + '_penetration' for col in Categories]
    #result.columns = [str(col) + '_result' for col in Categories]
    optimal_space.columns = [str(col) + '_current' for col in Categories]
   # wOutput=opt_amt2.append([opt_amt2,adj_p2,optimal_space])
    #w2Output=pd.concat((opt_amt2,adj_p2,optimal_space))
    return pd.concat((opt_amt2,adj_p2,optimal_space))

createWide(1,1,1,1,Categories)
