# DM-Project-10

* For pos tagging : run this cmnd from ./Datasets/stanford-postaer-2018-10-16/ folder
    java -mx300m -classpath stanford-postagger.jar edu.stanford.nlp.tagger.maxent.MaxentTagger -model models/gate-EN-twitter-fast.model -textFile ../../untagged_tweets.txt > ../../tagged_tweets2.txt
