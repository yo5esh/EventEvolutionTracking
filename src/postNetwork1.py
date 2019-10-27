from collections import defaultdict
import datetime
import math
from datetime import timedelta
import pandas as pd
import numpy as np
import queue

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
        self.clusters = defaultdict(set)
        self.S0 = list()
        self.Sn = list()
        self.S_, self.S_pl = list(),list()
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
        global NEXT_CLUSTER_ID, TIME_STEP, delta1
        ########## S0 and Sn only have core posts
        S_time = list()
        ## core->noncore posts due to change in time
        for post in self.corePosts :
            if post.weight/fad_sim(self.currTime,post.timeStamp) < delta1 :
                S_time.append(post)
        for post in S_time :
            self.corePosts.remove(post)
            for neighbours,we in self.graph[post] :
                if (neighbours.type == 'Border') and (not 'Core' in [x.type for x,_ in self.graph[neighbours]]):
                    neighbours.type = 'Noise'
                    self.borderPosts.remove(neighbours)
                    self.noise.append(neighbours)
            if 'Core' in [x.type for x,_ in self.graph[post]] :
                post.type = 'Border'
                borderPosts.append(post)
            else :
                post.type = 'Noise'
                noise.append(post)
        self.corePosts += self.Sn
        self.corePosts += self.S_pl
        ## Check for new border posts
        for post in self.S_pl+self.Sn:
            for neiPost,_ in self.graph[post]:
                if post.type == 'Noise':
                    self.noise.remove(neiPost)
                    post.type = 'Border'
                    self.borderPosts.append(neiPost)
        for post in self.S_:
            for neiPost,_ in self.graph[post]:
                if neiPost.type == 'Border':
                    if not 'Core' in [x.type for x,_ in self.graph[neiPost]]:
                        # Shouldn't be a borderpost
                        self.borderPosts.remove(neiPost)
                        neiPost.type = 'Noise'
                        self.noise.append(neiPost)
        for post in self.S_pl+self.Sn :
            for neiPost,_ in self.graph[post] :
                if neiPost.type == 'Noise' :
                    # Should be a borderpost
                    print('New n -> b ----- ',neiPost.id)
                    self.noise.remove(neiPost)
                    self.borderPosts.append(neiPost)
                    neiPost.type = 'Border'

        clus = self.S0+self.S_
        neg_C = set()
        for post in clus :
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
        S_temp = set(self.Sn+self.S_pl)
        explore = dict()
        for post in S_temp :
            explore[post] = True
        while len(S_temp) :
            connected = list()
            post = S_temp.pop()
            connected.append(post)
            q = queue.Queue()
            q.add(post)
            explore[post] = False
            while (not(q.empty())) :
                post = q.get()
                for neiPost,we in self.graph[post] :
                    if neiPost.type == 'Core' :
                        if len(neiPost.clusId) :
                            pos_C.add(next(iter(neiPost.clusId)))
                        elif explore[neiPost] :
                            explore[neiPost] = False
                            q.add(neiPost)
                            connected.append(neiPost)
            S_temp = S_temp - set(connected)
            if len(pos_C) == 0:
                newClus = set(connected)
                for post in connected :
                    for neiPost,we in self.graph[post] :
                        newClus.add(neiPost)
                        neiPost.clusId.add(NEXT_CLUSTER_ID)
                    post.clusId = set([NEXT_CLUSTER_ID])
                self.clusters[NEXT_CLUSTER_ID] = newClus
                NEXT_CLUSTER_ID += 1
            elif len(pos_C) == 1 :
                cid = pos_C.pop()
                for post in connected :
                    for neiPost,we in self.graph[post]:
                        self.clusters[cid].add(post)
                        neiPost.clusId.add(cid)
                    post.clusId.add(cid)
            else:
                cid = pos_C.pop()
                for post in connected :
                    for neiPost,we in self.graph[post] :
                        self.clusters[cid].add(neiPost)
                        neiPost.clusId.add(cid)
                    post.clusId = set([cid])
                for oldCid in pos_C :
                    for post in self.clusters[oldCid] :
                        post.clusId.remove(oldCid)
                        post.clusId.add(cid)
                    clusters[cid] = clusters[cid].union(clusters[oldCid])
                    clusters[oldCid].clear()
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
                    neiPost.weight -= we                            ## core to non core can be checked here itself
                    if neiPost.type == 'Core' and neiPost.weight/fad_sim(self.currTime,neiPost.timeStamp) < delta1 :
                        if not 'Core' in [x.type for x,_ in self.graph[neiPost]] :
                            neiPost.type = 'Noise'
                            self.noise.append(neiPost)
                        else :
                            neiPost.type = 'Border'
                            self.borderPosts.append(neiPost)
                        corePosts.remove(neiPost)
                        self.S_.append(neiPost)
                    self.graph[neiPost].remove((post,we))
                del self.graph[post]
                if(post.type == 'Core'): 
                    self.corePosts.remove(post)
                    self.S0.append(post)
                elif(post.type == 'Border'): self.borderPosts.remove(post)
                else : self.noise.remove(post)
                self.posts.remove(post)
            else:
                break
        return
        
    
    def updateConns(self, newPost):
        similarity = defaultdict(lambda : 0)
        for word in newPost.entities:
            for posts in self.entityDict[word.lower()]:
                similarity[posts] += 1/len(self.entityDict[word.lower()])
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
            prevPost.weight += sim
            newPost.weight += sim                                           ## non core to core can be checked here itself
            if not(prevPost.type == 'Core') and prevPost.weight/fad_sim(self.currTime,prevPost.timeStamp) >= delta1:
                if prevPost.type == 'Border' :
                    borderPosts.remove(prevPost)
                else :
                    noise.remove(prevPost)
                prevPost.type = 'Core'
                self.S_pl.append(newPost)
            if sim/fad_sim(newPost.timeStamp,prevPost.timeStamp) > epsilon0:
                print('Conn bw ',newPost.id,' ',prevPost.id)
                self.graph[newPost].append((prevPost,sim))
                self.graph[prevPost].append((newPost,sim))
        if newPost.weight/fad_sim(self.currTime,newPost.timeStamp) >= delta1:
            self.Sn.append(newPost)
            newPost.type = 'Core'
        else:
            if not 'Core' in [x.type for x,_ in self.graph[newPost]] :
                newPost.type = 'Noise'
                self.noise.append(newPost)
            else :
                newPost.type = 'Border'
                self.borderPosts.append(newPost)
    def nc_p0(self,delPost):
        ans = 0
        clus_posts = list()
        q = queue.Queue()
        explore = dict()
        for post,we in self.graph[delPost] :
            if post.type == 'Core' :
                clus_posts.add(post)
                q.put(post)
                explore[post] = True
        while (not q.empty()) :
            post = q.get()
            for neighbour,we in self.graph[post] :
                if neighbour.type == 'Core' and not(neighbour in explore.keys()) :
                    explore[neighbour] = True
                    q.put(neighbour)
                    clus_posts.add(neighbour)
        explore[delPost] = False
        while (len(clus_posts)) :
            ans+=1
            q.put(clus_posts[0])
            explore[clus_posts[0]] = False
            clus_posts.pop(0)
            while (not q.empty()) :
                post = q.get()
                for neighbour,we in self.graph[post]:
                    if neighbour.type == 'Core' and explore[neighbour] :
                        q.put(neighbour)
                        explore[neighbour] = False
                        clus_posts.remove(neighbour)
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

