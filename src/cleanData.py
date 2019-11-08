from langdetect import detect
import csv
import pandas as pd
import string
# with open('../Datasets/PreprocessedData/AllEvents.csv') as inp, open('../Datasets/PreprocessedData/AllEventsNew.csv') as out:
#     writer = csv.writer(out)
#     count = 0
#     for row1 in csv.reader(inp):
#         count += 1
#         if (count % 100) == 0 :
#             print(count)
#         row = row1[0].split('\t')
#         if detect(row[4]) == 'en':
#             writer.writerow(row1[0])
count = 0
def fun(str1):
    global count
    count += 1
    print(count)
    try:
        s1 = detect(str1)
    except :
        s1 = str1
    return s1
count1 = 0
def removePunctuations(s):
    global count1
    count1 += 1
    print(count1)
    s = s.translate(str.maketrans('', '', string.punctuation))
    return s
removePunctuations("")
df = pd.read_csv('../Datasets/PreprocessedData/AllEvents.csv', error_bad_lines=False, sep='\t')
df = df.drop(['retweet_count', 'favorite_count', 'user_id', 'user_mentions', 'retweet_id', 'retweet_user_id'], axis = 1).dropna()
df = df[df['text'].map(fun) == 'en']
df.to_csv(f'../Datasets/PreprocessedData/AllEventsNew.csv', sep='\t', index=False)
df['filt_tweet_text'] = df['filt_tweet_text'].map(removePunctuations)
df.to_csv(f'../Datasets/PreprocessedData/AllEventsNew.csv', sep='\t', index=False)
