from collections import defaultdict
import datetime
import math
from datetime import timedelta
import pandas as pd
import numpy as np
import queue

epsilon0 = 0.9
epsilon1 = 0.95
delta1 = 0.08
NEXT_POST_ID = 0
NEXT_CLUSTER_ID = 0
SLIDING_WINDOW = 86400
TIME_STEP = 30
LAMBDA = 200 # Without this we encounter overflow in fad_sim function
datetimeFormat = '%Y-%m-%d %H:%M:%S'

potential_neigh_thres = 0.05

def fad_sim(a,b):
    a = str(a)
    b = str(b)
    diff = datetime.datetime.strptime(a, datetimeFormat)-datetime.datetime.strptime(b, datetimeFormat)
    return diff.seconds+1 # Shldn't be zero

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
        self.trend = defaultdict(int)
        
        
    def addToTrend(self,noun):
        if len(self.trend) < 10:
            self.trend[noun.lower()] = len(self.entityDict[noun.lower])
        else:
            if noun.lower in self.trend.keys():
                self.trend[noun.lower()] = len(self.entityDict[noun.lower])
            else:
                key_min = min(self.trend.keys(), key=(lambda k: self.trend[k]))
                if self.trend[key_min] < len(self.entityDict[noun.lower]):
                    del self.trend[key_min]
                    self.trend[noun.lower()] = len(self.entityDict[noun.lower])


    def addPost(self, post):
        if not isinstance(post, Post):
            print('Undefined type passed to addPost method')
            return
        self.posts.append(post)
        self.updateConns(post)
        l = []
        l = set(post.entities)
        for noun in l:
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
        ## core->noncore posts due to change in time
        for post in self.S_ :
            if post.type == 'Core' :
                self.S_.remove(post)
        for post in self.corePosts :
            if post.weight/fad_sim(self.currTime,post.timeStamp) < delta1 :
                self.S_.append(post)
        ## Check for new border posts
        for post in self.S_:
            for neiPost,_ in self.graph[post]:
                if neiPost.type == 'Border':
                    if not 'Core' in [x.type for x,_ in self.graph[neiPost]]:
                        # Shouldn't be a borderpost
                        self.borderPosts.remove(neiPost)
                        neiPost.type = 'Noise'
                        self.noise.append(neiPost)
                        for clus in neiPost.clusId :
                            self.clusters[clus].remove(neiPost)
                        neiPost.clusId.clear()
            if post.type == 'Noise' and 'Core' in [x.type for x,_ in self.graph[post]] :
                post.type = 'Border'
                self.borderPosts.append(post)
                self.noise.remove(post)

        for post in self.S_pl+self.Sn :
            for neiPost,_ in self.graph[post] :
                if neiPost.type == 'Noise' :
                    # Should be a borderpost
                    print('New n -> b ----- ',neiPost.id)
                    self.noise.remove(neiPost)
                    self.borderPosts.append(neiPost)
                    neiPost.type = 'Border'
        self.corePosts += self.Sn
        self.corePosts += self.S_pl
        clus = set(self.S0+self.S_)
        # neg_C = set()
        # for post in clus :
        #     for neiPost,we in self.graph[post]:
        #         if neiPost.type == 'Core' and we >= epsilon1 and len(neiPost.clusId):# Should we check if conn is core conn also?
        #             neg_C.add(next(iter(neiPost.clusId)))# Gives element from a set
        # '''if len(neg_C) == 0:
        #     return
        # elif len(neg_C) == 1:
        #     return
        # else:
        #     return'''
        for post in clus:
            self.nc_p0(post)
        for post in self.S0 :
            for neighbour,_ in self.graph[post] :
                if neighbour.type == 'Border' and not 'Core' in [x.type for x,_ in self.graph[neighbour]]:
                    # Shouldn't be a borderpost
                    self.borderPosts.remove(neighbour)
                    neighbour.type = 'Noise'
                    self.noise.append(neighbour)
            del post
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
            q.put(post)
            explore[post] = False
            while (not(q.empty())) :
                post = q.get()
                for neiPost,we in self.graph[post] :
                    if neiPost.type == 'Core' :
                        if len(neiPost.clusId) :
                            a = neiPost.clusId.pop()
                            pos_C.add(a)
                            neiPost.clusId.add(a)
                        elif explore[neiPost] :
                            explore[neiPost] = False
                            q.put(neiPost)
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
                        self.clusters[cid].add(neiPost)
                        neiPost.clusId.add(cid)
                    self.clusters[cid].add(post)
                    post.clusId=set([cid])
            else:
                cid = pos_C.pop()
                for post in connected :
                    for neiPost,we in self.graph[post] :
                        self.clusters[cid].add(neiPost)
                        neiPost.clusId.add(cid)
                    self.clusters[cid].add(post)
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
        self.currTime += timedelta(seconds=TIME_STEP)

    def startTimeStep(self):
        # Delete old posts from self.posts and update weights, store in other array
        for i,post in enumerate(self.posts):
            if fad_sim(self.currTime,post.timeStamp) > SLIDING_WINDOW :
                print('Removing ',post.id)
                for neiPost,we in self.graph[post]:
                    self.graph[neiPost].remove((post,we))
                for neiPost,we in self.graph[post] :
                    neiPost.weight -= we                            ## core to non core can be checked here itself
                    if neiPost.type == 'Core' and neiPost.weight/fad_sim(self.currTime,neiPost.timeStamp) < delta1 :
                        neiPost.type = 'Noise'
                        self.noise.append(neiPost)
                        self.corePosts.remove(neiPost)
                        self.S_.append(neiPost)
                del self.graph[post]
                if(post.type == 'Core'): 
                    self.corePosts.remove(post)
                    self.S0.append(post)
                    post.type = 'Noise'
                    self.noise.append(post)
                elif(post.type == 'Border'): 
                    for clus in post.clusId :
                        self.clusters[clus].remove(post)
                    self.borderPosts.remove(post)
                else : 
                    self.noise.remove(post)
                self.posts.remove(post)
                for clus in post.clusId :
                    self.clusters[clus].remove(post)
                for word in post.entities :
                    self.entityDict[word.lower()].remove(post)
                if not (post.type == 'Core') :
                    del post
            else:
                break
        return
    
    
    def updateConns(self, newPost):
        similarity_for_jac = defaultdict(lambda : 0)
        similarity_for_pot = defaultdict(lambda : 0)
        for word in newPost.entities:
            for posts in self.entityDict[word.lower()]:
                similarity_for_pot[posts] += 1/(len(self.entityDict[word.lower()])+1)
                similarity_for_jac[posts] += 1
        for prevPost in similarity_for_pot.keys():
            sim = 0
            if(similarity_for_pot[prevPost] > potential_neigh_thres):
                #sim = similarity_for_jac[prevPost]/(len(newPost.entities)+len(prevPost.entities)-similarity_for_jac[prevPost])
                tfidf1 = []
                tfidf2 = []
                for entity in prevPost.entities:
                    if entity in newPost.entities:
                        tfidf1.append((prevPost.entities.count(entity)/len(prevPost.entities))*(math.log(len(self.posts)/(len(self.entityDict[entity.lower()])+1))))
                        tfidf2.append((newPost.entities.count(entity)/len(newPost.entities))*(math.log(len(self.posts)/(len(self.entityDict[entity.lower()])+1))))
                mag1=0
                mag2=0
                for tfidf in tfidf1:
                    mag1 += tfidf*tfidf
                for tfidf in tfidf2:
                    mag2 += tfidf*tfidf
                mag1 = math.sqrt(mag1)
                mag2 = math.sqrt(mag2)
                count = 0
                for i in range(len(tfidf1)):
                    count += tfidf1[i]*tfidf2[i]
                sim = count/(mag1*mag2)
                print('We bw ',newPost.id,' ',prevPost.id, ' is ',sim)
            newPost.weight += sim
            prevPost.weight += sim
            if not(prevPost.type == 'Core') and prevPost.weight/fad_sim(self.currTime,prevPost.timeStamp) >= delta1:
                prevPost.type = 'Core'
                self.S_pl.append(prevPost)
                for neighbour,we in self.graph[prevPost] :
                    if neighbour.type == 'Noise':
                        self.noise.remove(neighbour)
                        neighbour.type = 'Border'
                        self.borderPosts.append(neighbour)
            if sim/fad_sim(newPost.timeStamp,prevPost.timeStamp) > epsilon0 :
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

    def nc_p0(self, delPost):
        global NEXT_CLUSTER_ID
        delClustId = delPost.clusId.pop()
        delPost.clusId.add(delClustId)
        postsInCluster = self.clusters[delClustId]
        clus_posts = []
        for post in postsInCluster:
            if post.type == 'Core' :
                clus_posts.append(post)
            post.clusId.remove(delClustId)
        if delPost.type == 'Core' :
            clus_posts.remove(delPost)
        q = queue.Queue()
        explore = dict()
        for post in clus_posts:
            explore[post] = True
        explore[delPost] = False
        while (len(clus_posts)) :
            cluster = set()
            cluster.add(clus_posts[0])
            q.put(clus_posts[0])
            explore[clus_posts[0]] = False
            clus_posts.pop(0)
            while (not q.empty()) :
                post = q.get()
                cluster.add(post)
                post.clusId.add(NEXT_CLUSTER_ID)
                for neighbour,we in self.graph[post]:
                    if neighbour.type == 'Core' and explore[neighbour] :
                        q.put(neighbour)
                        explore[neighbour] = False
                        clus_posts.remove(neighbour)
            self.clusters[NEXT_CLUSTER_ID] = cluster
            NEXT_CLUSTER_ID += 1
        self.clusters.pop(delClustId, None)
        
    
    def printStats(self):
        print('********************************************************')
        print(self.currTime)
        print('No. of clusters: ',len(self.clusters))
        print('Cores: ',[x.id for x in self.corePosts])
        print('B: ',[x.id for x in self.borderPosts])
        print('N: ',[x.id for x in self.noise])
        k = Counter(self.entityDict)
        high = k.most_common(10)
        for i in high: 
            print(i[0]," :",i[1]," ")
        top_most = defaultdict(int)
        avg_for_all_clus = 0
        for clus in self.clusters.values():
            for post in clus:
                for entity in post.entities():
                    if entity in top_most.keys():
                        top_most[entity] += 1
                    else:
                        top_most[entity] = 1
            tot = 0        
            k = Counter(top_most)
            high = k.most_common(5)
            for i in high:
                tot += i[1]
            tot = tot/(len(clus)*5)
            avg_for_all_clus += tot
        avg_for_all_clus = avg_for_all_clus/len(self.clusters)
        print('Purity for all clusters ',avg_for_all_clus)    
        print('********************************************************')


postGraph = PostNetwork()
df = pd.read_csv('../Datasets/PreprocessedData/AllEvents.csv', error_bad_lines=False, sep='\t')

for index, row in df.iterrows():
    print(index,row['filt_tweet_text'].split(' '))
    if index > 2000:
        break
    if index == 0:
        postGraph.currTime = datetime.datetime.strptime(row['created_at'], datetimeFormat) + timedelta(seconds=1)
    if datetime.datetime.strptime(row['created_at'], datetimeFormat) <= postGraph.currTime + timedelta(seconds=TIME_STEP):
        postGraph.addPost(Post(entities=row['filt_tweet_text'].split(' '), timeStamp=row['created_at']))
    else:
        postGraph.endTimeStep() # Process new posts till now
        postGraph.startTimeStep() # Start adding new posts
        postGraph.addPost(Post(entities=row['filt_tweet_text'].split(' '), timeStamp=row['created_at']))
    if NEXT_POST_ID%50 == 0:
        print(f'Processed {NEXT_POST_ID} posts')
        print(row['created_at'], postGraph.currTime + timedelta(seconds=TIME_STEP), datetime.datetime.strptime(row['created_at'], datetimeFormat) <= postGraph.currTime + timedelta(seconds=TIME_STEP), sep='\n')


