import pandas as pd
from subprocess import check_output
import numpy as np

noun_tags = ['NN', 'NNS', 'NNP', 'NNPS']
DATASET_NAME = '../Datasets/CrisisNLP_labeled_data_crowdflower/2014_ebola_cf/2014_ebola_CF_labeled_data.tsv'
FINAL_NAME = DATASET_NAME.split('/')[3]

def snowflake2utcms(sf):
    #print(sf," - ",sf[1:-1])
    sf = int(sf[1:-1])
    return ((sf >> 22) + 1288834974657)

def getNouns(tweet):
    proctweet = ''
    words = tweet.split(' ')
    for word1 in words:
        word = word1.split('_')
        if len(word) < 2:
            #print(tweet,'\n')
            continue
        elif word[0][0:3] == 'http':
            continue
        elif word[1] in noun_tags or word[0][0]=='#':
            proctweet += (word[0])
            proctweet += ' '
    return proctweet

df = pd.read_csv(DATASET_NAME, error_bad_lines=False, sep='\t')
df.drop(['label'],axis=1,inplace=True)
print('Removed labels')
df['tweet_timeStamp'] = df['tweet_id'].map(snowflake2utcms)
print('Added timestamps column')
print(df.head())

# storing in .txt for sending for tagging pos
np.savetxt("../untagged_tweets.txt", df.values, fmt='%s')

print('Started tagging all tweets')
taggerProcess = check_output(["java", "-mx300m", "-classpath", "../Taggers/stanford-postagger-2018-10-16/stanford-postagger.jar", "edu.stanford.nlp.tagger.maxent.MaxentTagger", "-model", "../Taggers/stanford-postagger-2018-10-16/models/gate-EN-twitter-fast.model", "-textFile", "../untagged_tweets.txt", "-l"])
all_tweets = (taggerProcess.decode('utf-8')).split('\n')
print('Tagged all tweets')

# # filtering for nouns
filtered_tweets = []
for tweet in all_tweets:
    if tweet != '':
        filtered_tweets.append(getNouns(tweet))

print(len(filtered_tweets))
print(df.tail(1))
df['filt_tweet_text'] = filtered_tweets
print('Added filtered tweets column')
#print(df.head())
df.to_csv(f'../Datasets/PreprocessedData/{FINAL_NAME}.csv', sep='\t', index=False)
print('Saved to csv file')