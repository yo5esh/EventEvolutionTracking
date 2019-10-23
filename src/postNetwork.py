from collections import defaultdict
import datetime
import math
from datetime import timedelta
import pandas as pd
import numpy as np

epsilon0 = 0.055
epsilon1 = 0.07
delta1 = 0.08
NEXT_POST_ID = 0
NEXT_CLUSTER_ID = 0
SLIDING_WINDOW = 1000000
TIME_STEP = 540000
LAMBDA = 200 # Without this we encounter overflow in fad_sim function

def fad_sim(a,b):
    '''datetimeFormat = '%S'
    a = str(a)
    b = str(b)
    diff = datetime.datetime.strptime(a, datetimeFormat)-datetime.datetime.strptime(b, datetimeFormat)
    return diff.seconds'''
    return np.exp(abs(a-b)/(1000.0*LAMBDA))

class Post:

    def __init__(self, entities, id=False, timeStamp="", author=""):
        global NEXT_POST_ID
        self.entities = entities
        self.timeStamp = timeStamp
        self.author = author
        self.weight = 0
        if id is False:
            self.id = NEXT_POST_ID
            NEXT_POST_ID += 1
        else:
            self.id = id
        self.state = None
        self.type = ''
        self.clusId = set()

class PostNetwork:
    
    def __init__ (self):
        self.posts = list()
        self.corePosts = list()
        self.borderPosts = list()
        self.noise = list()
        self.entityDict = defaultdict(list)
        self.graph = defaultdict(list)
        self.sketchGraph = defaultdict(list)
        self.clusters = defaultdict(list)
        self.S0 = list()
        self.Sn = list()
        self.currTime = 0


    def addPost(self, post):
        if not isinstance(post, Post):
            print('Undefined type passed to addPost method')
            return
        self.posts.append(post)
        self.updateConns(post)
        for noun in post.entities:
            if noun != '':
                self.entityDict[noun.lower()].append(post) # All nouns are stored in lower case

    def getPost(self, id):
        for post in self.posts:
            if post.id == id:
                return post
        return None

    def endTimeStep(self):
        global NEXT_CLUSTER_ID, TIME_STEP
        ########## S0 and Sn only have core posts
        S_, S_pl = list(),list()

        ## Checking for core->noncore posts
        for i,post in enumerate(self.corePosts):
            if post.weight/fad_sim(self.currTime,post.timeStamp) < delta1:
                post.type = 'Noise'
                del self.corePosts[i]
                self.noise.append(post)
                S_.append(post)

        # Check for new core posts
        for i,post in enumerate(self.borderPosts):
            if post.weight/fad_sim(self.currTime,post.timeStamp) >= delta1:
                post.type = 'Core'
                del self.borderPosts[i]
                self.corePosts.append(post)
                S_pl.append(post)

        for i,post in enumerate(self.noise):
            if post.weight/fad_sim(self.currTime,post.timeStamp) >= delta1:
                post.type = 'Core'
                del self.noise[i]
                self.corePosts.append(post)
                S_pl.append(post)

        ## Check for new border posts
        self.corePosts += self.Sn
        self.corePosts += S_pl
        for post in S_pl+self.Sn:
            for neiPost,_ in self.graph[post]:
                if post.type == 'Noise':
                    self.noise.remove(neiPost)
                    post.type = 'Border'
                    self.borderPosts.append(neiPost)
        for post in S_:
            for neiPost,_ in self.graph[post]:
                if neiPost.type == 'Border':
                    if not 'Core' in [x.type for x,_ in self.graph[neiPost]]:
                        # Shouldn't be a borderpost
                        self.borderPosts.remove(neiPost)
                        neiPost.type = 'Noise'
                        self.noise.append(neiPost)
        for post in S_pl+self.Sn:
            for neiPost,_ in self.graph[post]:
                if neiPost.type == 'Noise':
                    if 'Core' in [x.type for x,_ in self.graph[neiPost]]:
                        # Should be a borderpost
                        print('New n -> b ----- ',neiPost.id)
                        self.noise.remove(neiPost)
                        self.borderPosts.append(neiPost)
                        neiPost.type = 'Border'

        clus = self.S0+S_
        neg_C = set()
        for post in clus:
            for neiPost,we in self.graph[post]:
                if neiPost.type == 'Core' and we >= epsilon1 and len(neiPost.clusId):# Should we check if conn is core conn also?
                    neg_C.add(next(iter(neiPost.clusId)))# Gives element from a set
        '''if len(neg_C) == 0:
            return
        elif len(neg_C) == 1:
            return
        else:
            return'''

        pos_C = set()
        for post in self.Sn+S_pl:
            for neiPost,we in self.graph[post]:
                if neiPost.type == 'Core' and we >= epsilon1 and len(neiPost.clusId):# Should we check if conn is core conn also?
                    pos_C.add(next(iter(neiPost.clusId)))

        if len(pos_C) == 0:
            newClus = self.Sn + S_pl
            for post in self.Sn+S_pl:
                for neiPost,we in self.graph[post]:
                    newClus.append(neiPost)
                    neiPost.clusId.add(NEXT_CLUSTER_ID)
                post.clusId = set([NEXT_CLUSTER_ID])
            self.clusters[NEXT_CLUSTER_ID] = newClus
            NEXT_CLUSTER_ID += 1
        elif len(pos_C) == 1:
            cid = pos_C.pop()
            for post in self.Sn+S_pl:
                for neiPost,we in self.graph[post]:
                    self.clusters[cid].append(post)
                    neiPost.clusId.add(cid)
                post.clusId = set([NEXT_CLUSTER_ID])
        else:
            cid = pos_C.pop()
            for post in self.Sn+S_pl:
                for neiPost,we in self.graph[post]:
                    self.clusters[cid].append(neiPost)
                    neiPost.clusId.add(cid)
                post.clusId = set([cid])
            for oldCid in pos_C:
                for post in self.clusters[oldCid]:
                    post.clusId.remove(oldCid)
                    post.clusId.add(cid)
        self.Sn.clear()
        self.S0.clear()
        #self.printStats()
        self.currTime += TIME_STEP

    def startTimeStep(self):
        # Delete old posts from self.posts and update weights, store in other array
        for i,post in enumerate(self.posts):
            if fad_sim(self.currTime,post.timeStamp) > SLIDING_WINDOW:
                print('Removing ',post.id)
                for neiPost,we in self.graph[post]:
                    neiPost.weight -= we
                    self.graph[neiPost].remove((post,we))
                del self.graph[post]
                if(post.type == 'Core'): 
                    self.corePosts.remove(post)
                    self.S0.append(post)
                elif(post.type == 'Border'): self.borderPosts.remove(post)
                elif(post.type == 'Noise'): self.noise.remove(post)
                self.posts.remove(post)
            else:
                break
        return
        
    
    def updateConns(self, newPost):
        similarity = defaultdict(lambda : 0)
        for word in newPost.entities:
            for posts in self.entityDict[word.lower()]:
                similarity[posts] += 1
        for prevPost in similarity.keys():
            '''try:
                sim = similarity[prevPost]/(len(newPost.entities)+len(prevPost.entities)-similarity[prevPost])
            except:
                print('error')
                print(newPost.entities)
                print(prevPost.entities)
                print(similarity[prevPost])
                sim = '''
            sim = similarity[prevPost]/(len(newPost.entities)+len(prevPost.entities)-similarity[prevPost])
            print('We bw ',newPost.id,' ',prevPost.id, ' is ',sim)
            newPost.weight += sim
            prevPost.weight += sim
            if sim/fad_sim(newPost.timeStamp,prevPost.timeStamp) > epsilon0:
                print('Conn bw ',newPost.id,' ',prevPost.id)
                self.graph[newPost].append((prevPost,sim))
                self.graph[prevPost].append((newPost,sim))
        if newPost.weight/fad_sim(self.currTime,newPost.timeStamp) >= delta1:
            self.Sn.append(newPost)
            newPost.type = 'Core'
        else:
            newPost.type = 'Noise'
            self.noise.append(newPost)
    def nc_p0(self,delPost):
        ans = 0
        clus_posts = list()
        q = queue.Queue()
        explore = dict()
        for post,we in self.graph[delPost]:
            if post.type == 'Core' :
                clus_posts.add(post)
                q.put(post)
                explore[post] = False
        while (not q.empty()) :
            post = q.get()
            for posta,we in self.graph[post]:
                if posta == 'Core' and not(posta in explore.keys()) :
                    explore[posta] = False
                    q.put(posta)
                    clus_posts.add(posta)
        while (not(len(clus_posts) == 0)) :
            ans+=1
            q.put(clus_posts[0])
            explore[clus_posts[0]] = True
            clus_posts.pop(0)
            while (not q.empty()) :
                post = q.get()
                for posta,we in self.graph[post]:
                    if not (posta == delPost) and posta.type == 'Core' and explore[posta] == False :
                        q.put(posta)
                        explore[posta] = True
                        clus_posts.remove(posta)
        return ans
    
    def printStats(self):
        print('********************************************************')
        print(self.currTime)
        print('No. of clusters: ',len(self.clusters))
        print('Cores: ',[x.id for x in self.corePosts])
        print('B: ',[x.id for x in self.borderPosts])
        print('N: ',[x.id for x in self.noise])
        print('********************************************************')


postGraph = PostNetwork()
df = pd.read_csv('../Datasets/PreprocessedData/2014_ebola_cf.csv', error_bad_lines=False, sep='\t')

for index, row in df.iterrows():
    print(index,row['filt_tweet_text'].split(' '))
    if index > 20:
        break
    if index == 0:
        postGraph.currTime = int(row['tweet_timeStamp'])
    if row['tweet_timeStamp'] <= postGraph.currTime + TIME_STEP:
        postGraph.addPost(Post(entities=row['filt_tweet_text'].split(' '), timeStamp=row['tweet_timeStamp']))
    else:
        postGraph.endTimeStep() # Process new posts till now
        postGraph.startTimeStep() # Start adding new posts
        postGraph.addPost(Post(entities=row['filt_tweet_text'].split(' '), timeStamp=row['tweet_timeStamp']))
    if NEXT_POST_ID%50 == 0:
        print(f'Processed {NEXT_POST_ID} posts')
        print(row['tweet_timeStamp'], postGraph.currTime + TIME_STEP, row['tweet_timeStamp'] <= postGraph.currTime + TIME_STEP, sep='\n')

    postGraph.random_Walk(Post(entities=row['filt_tweet_text'].split(' '), timeStamp=row['tweet_timeStamp']))

