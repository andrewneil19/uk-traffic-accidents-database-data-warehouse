import pandas as pd
from tabulate import tabulate
import codecs

pd.options.display.max_columns =None
pd.options.display.max_rows =40

filename1 = 'Accident_Information.csv'
with codecs.open(filename1, 'r', encoding='ISO-8859-1') as f:
    df1 = pd.read_csv(f,low_memory=False)
#df1 = pd.read_csv(filename1,low_memory=False)

filename2 = 'Vehicle_Information.csv'
with codecs.open(filename2, 'r', encoding='ISO-8859-1') as f:
    df2 = pd.read_csv(f,low_memory=False)
#df2 = pd.read_csv(filename2,low_memory=False)

dfAcc = df1.sample(n=300000)
dfVeh = df2[df2['Accident_Index'].isin(dfAcc['Accident_Index'])]
dfAcc1 = dfAcc[dfAcc['Accident_Index'].isin(dfVeh['Accident_Index'])]

sorted_dfAcc = dfAcc1.sort_values(by='Accident_Index')
sorted_dfVeh = dfVeh.sort_values(by='Accident_Index')

sorted_dfAcc.to_csv('Accidents_extract.csv', index=False)
sorted_dfVeh.to_csv('Vehicles_extract.csv', index=False)

unique_count1 = dfAcc1['Accident_Index'].nunique()
print(unique_count1)

unique_count2 = dfVeh['Accident_Index'].nunique()
print(unique_count2)

'''
col_names1 = list(df1.columns)
print(col_names1)
'''


#print(tabulate(df, headers=col_names, tablefmt="psql", showindex="always"))
