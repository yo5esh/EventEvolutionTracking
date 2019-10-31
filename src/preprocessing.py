import pandas as pd
from subprocess import check_output
import numpy as np

noun_tags = ['NN', 'NNS', 'NNP', 'NNPS']
DATASET_NAME = '../Datasets/Final Dataset/Final/Hangupit.csv'
FINAL_NAME = DATASET_NAME.split('/')[4]

def getNouns(tweet):
    proctweet = ''
    words = tweet.split(' ')
    for word1 in words:
        word = word1.split('_')
        if len(word[0]) < 2:
            #print(tweet,'\n')
            continue
        elif word[0][0:3] == 'http':
            continue
        elif word[1] in noun_tags or word[0][0]=='#':
            proctweet += (word[0])
            proctweet += ' '
    return proctweet

df = pd.read_csv(DATASET_NAME, sep='\t')
print(df.tail())

# storing in .txt for sending for tagging pos
f = open("../untagged_tweets.txt", 'w')
f.write("")
f.close()
df['text'] = df['text'].apply(lambda x: x.replace("\n"," "))
np.savetxt("../untagged_tweets.txt", df[['text']].values, fmt='%s')

print('Started tagging all tweets...')
taggerProcess = check_output(["java", "-mx500m", "-classpath", "../Taggers/stanford-postagger-2018-10-16/stanford-postagger.jar", "edu.stanford.nlp.tagger.maxent.MaxentTagger", "-model", "../Taggers/stanford-postagger-2018-10-16/models/gate-EN-twitter-fast.model", "-textFile", "../untagged_tweets.txt", "-l"])
all_tweets = (taggerProcess.decode('utf-8')).split('\n')
print('Tagged all tweets')
np.savetxt(f"../{FINAL_NAME}_tagged_tweets.txt", all_tweets, fmt='%s')
all_tweets = open(f'../{FINAL_NAME}_tagged_tweets.txt','r').read().split('\n')
print(f'Found1 {len(all_tweets)} tweets')
while all_tweets[-1] == '': del all_tweets[-1]
print(f'Found {len(all_tweets)} tweets')

oov = pd.read_csv('../OOV_Dict/OOV_Dictionary_V1.0.tsv', error_bad_lines=False, sep='\t', encoding='latin-1', header=None)
print(oov.head())
oov = oov.set_index(0).T.to_dict('list')
print('Loaded OOV...')

# # filtering for nouns
filtered_tweets = []
for i,tweet in enumerate(all_tweets):
    filtered_tweets.append(getNouns(tweet))


print(f'Found {len(filtered_tweets)} filtered tweets')
print(df.tail(1))
df['filt_tweet_text'] = filtered_tweets
print('Added filtered tweets column')
pd.to_datetime(df['created_at'])
df.sort_values(by='created_at', inplace=True)
print('Sorted by timestamps')
#print(df.head())
df.to_csv(f'../Datasets/PreprocessedData/{FINAL_NAME}', sep='\t', index=False)
print('Saved to csv file')
