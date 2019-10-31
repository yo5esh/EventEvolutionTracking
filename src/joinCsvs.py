from os import listdir
from os.path import isfile, join
import pandas as pd
mypath = '../Datasets/PreprocessedData/'
allCsvs = [f for f in listdir(mypath) if isfile(join(mypath, f))]
if 'AllEvents.csv' in allCsvs: allCsvs.remove('AllEvents.csv')
print(allCsvs)

df = pd.read_csv(mypath+allCsvs[0],sep='\t')
for fp in allCsvs[1:]:
    dft = pd.read_csv(mypath+fp,sep='\t')
    df = df.append(dft, ignore_index = True)

print('Appended all columns...')
print(df.dtypes)
print(pd.Series(df['id']).is_unique)
df.drop_duplicates(subset='id',inplace=True)
print(pd.Series(df['id']).is_unique)
print('Deleted duplicate tweets')
df['created_at'] = pd.to_datetime(df['created_at'],format='%a %b %d %H:%M:%S +0000 %Y')
print(df.dtypes)
df.sort_values(by='created_at', inplace=True)
print('Sorted by timestamps')
df.to_csv(f'../Datasets/PreprocessedData/AllEvents.csv', sep='\t', index=False)
print('Saved to csv file')