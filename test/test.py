import pandas as pd

# reads in file, uses first row as headers
data = pd.read_csv('Test_Data_Trad/Children_Scn_Trdtnl_TC01/Brand_Exit.csv', header=0)

data = data.drop([0])

# un-pivotes the data to have now Category-Store pairs and drop those with Store=NaN
data = pd.melt(data, var_name='Category', value_name='Store').dropna(axis=0)

print (data)



# converts Store ID to integer
data['Store'] = data['Store'].astype(int)

# orders columns to Store, Category
data = data[['Store', 'Category']]

print (data)


