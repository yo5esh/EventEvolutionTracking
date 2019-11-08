from collections import defaultdict
import datetime
import math
from datetime import timedelta
import pandas as pd
import numpy as np
import queue
from collections import Counter
import time

epsilon0 = 0.09
epsilon1 = 0.95
delta1 = 0.4
NEXT_POST_ID = 0
NEXT_CLUSTER_ID = 0
SLIDING_WINDOW = 60
TIME_STEP = 10
LAMBDA = 200 # Without this we encounter overflow in fad_sim function
datetimeFormat = '%Y-%m-%d %H:%M:%S'

potential_neigh_thres = 0.05
event_thres = 1
def fad_sim(a,b):
    a = str(a)
    b = str(b)
    diff = datetime.datetime.strptime(a, datetimeFormat)-datetime.datetime.strptime(b, datetimeFormat)
    return abs(diff.seconds)+1 # Shldn't be zero

class Post:

    def __init__(self, entities, id=False, timeStamp="", author=""):
        global NEXT_POST_ID
        self.entities = entities
        self.entities.remove('')
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
        self.clusters = dict()
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

    def check_clus(self):
        for post in self.corePosts:
            if len(post.clusId) != 1:
                print('################## Core post not in one cluster')
            cid = post.clusId.pop()
            post.clusId.add(cid)
            if post not in self.clusters[cid]:
                print('################## core post didnt get added in cluster')
        for post in self.borderPosts:
            for neipost,_ in self.graph[post]:
                if neipost.type == 'Core':
                    if next(iter(neipost.clusId)) not in post.clusId:
                        print('########################### Border 1')
            for cid in post.clusId:
                if post not in self.clusters[cid]:
                    print('###################################################  Border2')

        for post in self.noise:
            if len(post.clusId) != 0:
                print('############################################################# Noise')    

        for clus in self.clusters.values():
            for post in clus:
                if datetime.datetime.strptime(post.timeStamp, datetimeFormat) <= self.currTime - timedelta(seconds=SLIDING_WINDOW):
                    print('####################################################################### Error3 ',post.id, post.type) 
                if post.type == 'Noise':
                    print('##################################################  Noise in clus')



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
        for i,post in enumerate(self.corePosts):
            if post.weight/fad_sim(self.currTime,post.timeStamp) < delta1:
                self.corePosts.remove(post)
                self.S_.append(post)
                if not 'Core' in [x.type for x,_ in self.graph[post]] :
                    post.type = 'Noise'
                    self.noise.append(post)
                else :
                    post.type = 'Border'
                    self.borderPosts.append(post)

        # Check for new core posts
        for i,post in enumerate(self.borderPosts):
            if post.weight/fad_sim(self.currTime,post.timeStamp) >= delta1:
                post.type = 'Core'
                self.borderPosts.remove(post)
                self.corePosts.append(post)
                self.S_pl.append(post)

        for i,post in enumerate(self.noise):
            if post.weight/fad_sim(self.currTime,post.timeStamp) >= delta1:
                post.type = 'Core'
                self.noise.remove(post)
                self.corePosts.append(post)
                self.S_pl.append(post)

        for post in self.S_pl+self.Sn:
            for neiPost,_ in self.graph[post]:
                if post.type == 'Noise':
                    self.noise.remove(neiPost)
                    post.type = 'Border'
                    self.borderPosts.append(neiPost)
        for post in self.S_:
            for neiPost,_ in self.graph[post]:
                if neiPost.type == 'Border' and neiPost not in self.S_:
                    if not 'Core' in [x.type for x,_ in self.graph[neiPost]]:
                        # Shouldn't be a borderpost
                        self.borderPosts.remove(neiPost)
                        neiPost.type = 'Noise'
                        self.noise.append(neiPost)
                        for cid in neiPost.clusId:
                            self.clusters[cid].remove(neiPost)
                        neiPost.clusId.clear()
            if post.type == 'Noise' and 'Core' in [x.type for x,_ in self.graph[post]] :
                post.type = 'Border'
                self.borderPosts.append(post)
                self.noise.remove(post)
        for post in self.S_pl+self.Sn :
            for neiPost,_ in self.graph[post] :
                if neiPost.type == 'Noise' :
                    self.noise.remove(neiPost)
                    self.borderPosts.append(neiPost)
                    neiPost.type = 'Border'
        
        print('Len of S_pl and Sn ', len(self.S_pl),' set ', len(set(self.S_pl)),' ',len(self.Sn),' set ', len(set(self.Sn)))
        print('len of S_ ', len(self.S_),' set ', len(set(self.S_)))
        delclus = set(self.S_)
        explore = dict()
        inTemp = defaultdict(lambda: False)
        for post in self.S_ :
            explore[post] = False
            inTemp[post] = True
            print('s-ids ',post.id)
        while len(delclus) != 0:
            q = queue.Queue()
            post1 = delclus.pop()
            q.put(post1)
            explore[post1] = True
            while not q.empty():
                presPost = q.get()
                for neigh,_ in self.graph[presPost]:
                    if inTemp[neigh] and not explore[neigh]:
                        explore[neigh] = True
                        q.put(neigh)
                        print('got id in bfs ',neigh.id)
                        delclus.remove(neigh)
            self.nc_p0(post1)


        S_temp = set(self.Sn+self.S_pl)
        explore = dict()
        inTemp = defaultdict(lambda: False)
        for post in S_temp :
            explore[post.id] = True
            inTemp[post] = True
        while len(S_temp) :
            pos_C = set()
            connected = list()
            post = S_temp.pop()
            connected.append(post)
            q = queue.Queue()
            q.put(post)
            explore[post.id] = False
            while (not(q.empty())) :
                post = q.get()
                for neiPost,we in self.graph[post] :
                    if neiPost.type == 'Core' :
                        if not inTemp[neiPost]:
                            a = neiPost.clusId.pop()
                            pos_C.add(a)
                            neiPost.clusId.add(a)
                        elif explore[neiPost.id] :
                            explore[neiPost.id] = False
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
                    self.clusters[cid] = self.clusters[cid].union(self.clusters[oldCid])
                    self.clusters[oldCid].clear()
                    del self.clusters[oldCid]
                    #print('#################################################################################')
        self.printStats()
        self.Sn.clear()
        self.S_.clear()
        self.S_pl.clear()
        self.currTime += timedelta(seconds=TIME_STEP)

    def startTimeStep(self):
        # Delete old posts from self.posts and update weights, store in other array
        for i,post in enumerate(self.posts):
            if datetime.datetime.strptime(post.timeStamp, datetimeFormat) <= self.currTime - timedelta(seconds=SLIDING_WINDOW):
                print('Removing ',post.id)
                for neiPost,we in self.graph[post]:
                    self.graph[neiPost].remove((post,we))
                for neiPost,we in self.graph[post] :
                    neiPost.weight -= we
                    if neiPost.type == 'Border':
                        if not 'Core' in [x.type for x,_ in self.graph[neiPost]]:
                            # Shouldn't be a borderpost
                            self.borderPosts.remove(neiPost)
                            neiPost.type = 'Noise'
                            self.noise.append(neiPost)
                if(post.type == 'Core'): 
                    self.corePosts.remove(post)
                    post.type = 'Noise'
                    self.nc_p0(post)
                    for neighbour,_ in self.graph[post] :
                        if neighbour.type == 'Border' and not 'Core' in [x.type for x,_ in self.graph[neighbour]]:
                            self.borderPosts.remove(neighbour)
                            neighbour.type = 'Noise'
                            self.noise.append(neighbour)
                elif(post.type == 'Border'): 
                    self.borderPosts.remove(post)
                    for id in post.clusId:
                        self.clusters[id].remove(post)
                    # post.clusId.clear()
                else : 
                    self.noise.remove(post)
                self.posts.remove(post)
                for word in set(post.entities) :
                    if word != '' :
                        self.entityDict[word.lower()].remove(post)
                del self.graph[post]
                del post
            else:
                break
        return
    
    
    def updateConns(self, newPost):
        similarity_for_jac = defaultdict(lambda : 0)
        #similarity_for_pot = defaultdict(lambda : 0)
        for word in newPost.entities:
            for posts in self.entityDict[word.lower()]:
                #similarity_for_pot[posts] += 1/(len(self.entityDict[word.lower()])+1)
                similarity_for_jac[posts] += 1
        for prevPost in similarity_for_jac.keys():
            sim = similarity_for_jac[prevPost]/(len(prevPost.entities) + len(newPost.entities) - similarity_for_jac[prevPost])
            sim /= fad_sim(newPost.timeStamp,prevPost.timeStamp)
            #print('We bw ',newPost.id,' ',prevPost.id, ' is ',sim)
            if sim > epsilon0 :
                print('Conn bw wei ',newPost.id,' ',prevPost.id,' ',sim)
                newPost.weight += sim
                prevPost.weight += sim
                self.graph[newPost].append((prevPost,sim))
                self.graph[prevPost].append((newPost,sim))
        print('New post weight ', newPost.weight/fad_sim(self.currTime,newPost.timeStamp))
        if newPost.weight/fad_sim(self.currTime,newPost.timeStamp) >= delta1:
            self.Sn.append(newPost)
            newPost.type = 'Core'
            self.corePosts.append(newPost)
        else:
            if not 'Core' in [x.type for x,_ in self.graph[newPost]] :
                newPost.type = 'Noise'
                self.noise.append(newPost)
            else :
                newPost.type = 'Border'
                self.borderPosts.append(newPost)

    def nc_p0(self, delPost):
        global NEXT_CLUSTER_ID
        print('ncp0 on ',delPost.id, ' type ',delPost.type)
        delClustId = delPost.clusId.pop()
        delPost.clusId.add(delClustId)
        postsInCluster = self.clusters[delClustId]
        clus_posts = []
        for post in postsInCluster :
            if post.type == 'Core' :
                clus_posts.append(post)
            post.clusId.remove(delClustId)
        q = queue.Queue()
        explore = dict()
        for post in clus_posts:
            explore[post.id] = True
        while (len(clus_posts)) :
            cluster = set()
            cluster.add(clus_posts[0])
            q.put(clus_posts[0])
            explore[clus_posts[0].id] = False
            clus_posts.pop(0)
            while (not q.empty()) :
                post = q.get()
                cluster.add(post)
                post.clusId.add(NEXT_CLUSTER_ID)
                for neighbour,we in self.graph[post] :
                    if neighbour.type == 'Core' :
                        if neighbour in postsInCluster and explore[neighbour.id] :
                            q.put(neighbour)
                            explore[neighbour.id] = False
                            clus_posts.remove(neighbour)
                    else :
                        cluster.add(neighbour)
                        neighbour.clusId.add(NEXT_CLUSTER_ID)
            if len(cluster) > 0:
                self.clusters[NEXT_CLUSTER_ID] = cluster
                NEXT_CLUSTER_ID += 1
        #self.clusters.pop(delClustId, None)
        del self.clusters[delClustId]
    
    def printStats(self):
        print('********************************************************')
        self.check_clus()
        print(self.currTime)
        print('No. of clusters: ',len(self.clusters.keys()))
        print('Len of posts ',len(self.posts))
        print('Cores: ',len(self.corePosts),' set ', len(set(self.corePosts)))
        print('B: ',len(self.borderPosts),' set ', len(set(self.borderPosts)))
        print('N: ',len(self.noise),' set ', len(set(self.noise)))
        
        #k = Counter(self.entityDict)
        #high = k.most_common(10)
        #for i in high: 
        #    print(i[0]," :",i[1]," ")
        #top_most = defaultdict(int)
        avg_for_all_clus = 0
        for clus in self.clusters.values():
            top_most = defaultdict(int)
            for post in clus:
                for entity in set(post.entities):
                    if entity in top_most.keys():
                        top_most[entity] += 1
                    else:
                        top_most[entity] = 1
                    
            tot = 0        
            k = Counter(top_most)
            high = k.most_common(5)
            wei = 0
            for post in clus:
                wei += post.weight
            if wei > event_thres:
                for i in high:
                    print(i[0])
            print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
            for i in high:
                tot += i[1]
            tot = tot/(len(clus)*5)
            avg_for_all_clus += tot
            del top_most
        if len(self.clusters) != 0 :   
            avg_for_all_clus = avg_for_all_clus/len(self.clusters)
        print('Purity for all clusters ',avg_for_all_clus)
        #print (self.clusters) 
        print('********************************************************')        
        time.sleep(5)


postGraph = PostNetwork()
df = pd.read_csv('../Datasets/PreprocessedData/AllEvents.csv', error_bad_lines=False, sep='\t')

for index, row in df.iterrows():
    #print(index,row['filt_tweet_text'].split(' '))
    if index > 20000:
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


