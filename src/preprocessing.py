import pandas as pd

# df = pd.read_csv('../Datasets/CrisisNLP_labeled_data_crowdflower/2013_Pakistan_eq/2013_Pakistan_eq_CF_labeled_data.tsv', error_bad_lines=False, sep='\t')
# #print(df.head())

# df.drop(['label'],axis=1,inplace=True)
#print('Changed labels\n',df.head())

##################### storing in .txt for sending for tagging pos
# f = open("../untagged_tweets.txt", "w")
# tweets = df['tweet_text']
# for tweet in tweets:
#     f.write(tweet)
#     f.write('.\n')
# f.close()

'''
For pos tagging : run this cmnd from ./Datasets/stanford-postaer-2018-10-16/ folder
    java -mx300m -classpath stanford-postagger.jar edu.stanford.nlp.tagger.maxent.MaxentTagger -model models/gate-EN-twitter-fast.model -textFile ../../untagged_tweets.txt > ../../tagged_tweets2.txt
'''


##################### filtering for nouns
noun_tags = ['NN']
f = open("../tagged_tweets.txt", "r")
all_tweets = f.read().split('\n')
f.close()
f = open("../filtered_tweets.txt", "w")
for tweet in all_tweets:
    words = tweet.split(' ')
    for word in words:
        word = word.split('_')
        if len(word) < 2:
            print(tweet,'\n')
        elif word[1] in noun_tags:
            f.write(word[0])
            f.write(' ')
            
    f.write('\n')
f.close()
