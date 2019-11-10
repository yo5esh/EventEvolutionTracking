from collections import defaultdict
import datetime
import math
from datetime import timedelta
import pandas as pd
import numpy as np
import queue
from collections import Counter
import time

epsilon0 = 0.03
epsilon1 = 0.95
delta1 = 0.00005
NEXT_POST_ID = 0
NEXT_CLUSTER_ID = 0
SLIDING_WINDOW = 24000
TIME_STEP = 3000
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
        self.entities = [x.lower() for x in entities if x != '']
        self.timeStamp = timeStamp
        self.author = author
        self.weight = 0
        if id is False:
            self.id = NEXT_POST_ID
            NEXT_POST_ID += 1
        else:
            self.id = id
        self.state = None
        self.type = ''          # Type of post (Core, Border, Noise)
        self.clusId = set()     # Set of cluster ids to which a post belongs

class PostNetwork:
    
    def __init__ (self):
        self.posts = list()     # Contains list of posts in current time window.
        self.corePosts = list()
        self.borderPosts = list()
        self.noise = list()
        self.entityDict = defaultdict(list)     # Bipartite graph between entities and posts.
        self.graph = defaultdict(list)          # Adjacency list of Post Network.
        self.clusters = dict()                  # Map from Clusted id to posts in the cluster
        self.Sn = list()                        # List of new core posts added
        self.S_, self.S_pl = list(),list()      # List of non-core to core and core to non-core posts in present timestep.
        self.currTime = 0
        self.trend = defaultdict(int)           # List of trending entities in present timestep
        self.potconns = 0
        self.connformed= 0
        self.fileptr = open("Information.csv", 'w')
        self.fileptr.write("TIMESTAMP\tCLUSTER ID\tFREQUENT ENTITIES\tCHILD CLUSTER ID\n")
        self.purityptr = open("Purity.txt", 'w')
        self.time = 0

    def __del__(self):
        self.fileptr.close()
        self.purityptr.close()

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
                print('################## Core post not in one cluster ', post.id, post.type)
            cid = post.clusId.pop()
            post.clusId.add(cid)
            if post not in self.clusters[cid]:
                print('################## core post didnt get added in cluster ', post.id, post.type)
        for post in self.borderPosts:
            for neipost,_ in self.graph[post]:
                if neipost.type == 'Core':
                    if next(iter(neipost.clusId)) not in post.clusId:
                        print('########################### Border 1 ', post.id, post.type)
            for cid in post.clusId:
                if post not in self.clusters[cid]:
                    print('###################################################  Border2', post.id, post.type)

        for post in self.noise:
            if len(post.clusId) != 0:
                print('############################################################# Noise ', post.id, post.type)    

        for clus in self.clusters.values():
            for post in clus:
                if datetime.datetime.strptime(post.timeStamp, datetimeFormat) <= self.currTime - timedelta(seconds=SLIDING_WINDOW):
                    print('####################################################################### Error3 ', post.id, post.type) 
                if post.type == 'Noise':
                    print('##################################################  Noise in clus ', post.id, post.type)



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
                print('=======================================================================================================================================================================================================')
        for post in self.corePosts :
            if post.weight/fad_sim(self.currTime,post.timeStamp) < delta1 :
                self.S_.append(post)
                self.corePosts.remove(post)
                post.type = 'Noise'
                self.noise.append(post)
        ## Check for new border posts
        
        print('Len of S_pl and Sn ', len(self.S_pl),' set ', len(set(self.S_pl)),' ',len(self.Sn),' set ', len(set(self.Sn)))
        print('len of S_ ', len(self.S_),' set ', len(set(self.S_)))
        delclus = set(self.S_)
        for post in delclus:
            for neiPost,_ in self.graph[post]:
                if neiPost.type == 'Border' and neiPost not in self.S_:
                    if not 'Core' in [x.type for x,_ in self.graph[neiPost]]:
                        # Shouldn't be a borderpost
                        self.borderPosts.remove(neiPost)
                        neiPost.type = 'Noise'
                        self.noise.append(neiPost)
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
        for post in self.S_pl:
            for id in post.clusId :
                self.clusters[id].remove(post)
            post.clusId.clear()
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
            print('bfs at ',post1.id,post1.type)
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
        for post in S_temp :
            explore[post.id] = True
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
                        if len(neiPost.clusId) :
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
                self.fileptr.write(f"{self.currTime}\t-\t{self.getFrequentEntities(NEXT_CLUSTER_ID)}\t{NEXT_CLUSTER_ID}\n")
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
                        if post.type == 'Core':
                            for neiPost,we in self.graph[post]:
                                if not cid in neiPost.clusId:
                                    self.clusters[cid].add(neiPost)
                                    neiPost.clusId.add(cid)
                    self.clusters[cid] = self.clusters[cid].union(self.clusters[oldCid])
                    self.fileptr.write(f"{self.currTime}\t{oldCid}\t{self.getFrequentEntities(oldCid)}\t{cid}\n")
                    self.clusters[oldCid].clear()
                    del self.clusters[oldCid]
                    #print('#################################################################################')
        self.printStats()
        self.Sn.clear()
        self.S_.clear()
        self.S_pl.clear()
        self.currTime += timedelta(seconds=TIME_STEP)
        self.time += 1
        self.potconns = 0
        self.connformed= 0

    def startTimeStep(self):
        # Delete old posts from self.posts and update weights, store in other array
        while len(self.posts):
            post = self.posts.pop(0)
            print('checking starttime ',post.id, post.type)
            if datetime.datetime.strptime(post.timeStamp, datetimeFormat) <= self.currTime - timedelta(seconds=SLIDING_WINDOW):
                print('Removing ',post.id)
                for neiPost,we in self.graph[post]:
                    self.graph[neiPost].remove((post,we))
                for neiPost,we in self.graph[post] :
                    neiPost.weight -= we                            ## core to non core can be checked here itself
                    if datetime.datetime.strptime(post.timeStamp, datetimeFormat) > self.currTime - timedelta(seconds=SLIDING_WINDOW) and neiPost.type == 'Core' and neiPost.weight/fad_sim(self.currTime,neiPost.timeStamp) < delta1 :
                        neiPost.type = 'Noise'
                        self.noise.append(neiPost)
                        self.corePosts.remove(neiPost)
                        self.S_.append(neiPost)
                if(post.type == 'Core'): 
                    self.corePosts.remove(post)
                    post.type = 'Noise'
                    self.nc_p0(post)
                    for neighbour,_ in self.graph[post] :
                        if neighbour.type == 'Border' and not 'Core' in [x.type for x,_ in self.graph[neighbour]]:
                            # Shouldn't be a borderpost
                            self.borderPosts.remove(neighbour)
                            neighbour.type = 'Noise'
                            self.noise.append(neighbour)
                            for clus in neighbour.clusId :
                                self.clusters[clus].remove(neighbour)
                            neighbour.clusId.clear()
                elif(post.type == 'Border'): 
                    self.borderPosts.remove(post)
                    for id in post.clusId:
                        self.clusters[id].remove(post)
                    post.clusId.clear()
                else : 
                    self.noise.remove(post)
                del self.graph[post]
                #self.posts.remove(post)
                for word in set(post.entities) :
                    if word != '' :
                        self.entityDict[word.lower()].remove(post)
                del post
            else:
                self.posts.insert(0,post)
                break
    
    def updateConns(self, newPost):
        similarity_for_jac = defaultdict(lambda : 0)
        #similarity_for_pot = defaultdict(lambda : 0)
        for word in newPost.entities:
            for posts in self.entityDict[word.lower()]:
                #similarity_for_pot[posts] += 1/(len(self.entityDict[word.lower()])+1)
                similarity_for_jac[posts] += 1
        for prevPost in similarity_for_jac.keys():
            sim = similarity_for_jac[prevPost]/(len(prevPost.entities) + len(newPost.entities) - similarity_for_jac[prevPost])

        # for prevPost in similarity_for_pot.keys():
        #     #sim = 0
        #     if(similarity_for_pot[prevPost] > potential_neigh_thres):
        #         #sim = similarity_for_jac[prevPost]/(len(newPost.entities)+len(prevPost.entities)-similarity_for_jac[prevPost])
        #         tfidf1 = []
        #         tfidf2 = []
        #         for entity in prevPost.entities:
        #             if entity in newPost.entities:
        #                 tfidf1.append((prevPost.entities.count(entity)/len(prevPost.entities))*(math.log(len(self.posts)/(len(self.entityDict[entity.lower()])+1))))
        #                 tfidf2.append((newPost.entities.count(entity)/len(newPost.entities))*(math.log(len(self.posts)/(len(self.entityDict[entity.lower()])+1))))
        #         mag1=0
        #         mag2=0
        #         for tfidf in tfidf1:
        #             mag1 += tfidf*tfidf
        #         for tfidf in tfidf2:
        #             mag2 += tfidf*tfidf
        #         mag1 = math.sqrt(mag1)
        #         mag2 = math.sqrt(mag2)
        #         count = 0
        #         for i in range(len(tfidf1)):
        #             count += tfidf1[i]*tfidf2[i]
        #         sim = count/(mag1*mag2)
            sim /= fad_sim(newPost.timeStamp,prevPost.timeStamp)
            #print('We bw ',newPost.id,' ',prevPost.id, ' is ',sim)
            self.potconns += 1
            if sim > epsilon0 :
                self.connformed+= 1
                print('Conn bw wei ',newPost.id,' ',prevPost.id,' ',sim)
                newPost.weight += sim
                prevPost.weight += sim
                self.graph[newPost].append((prevPost,sim))
                self.graph[prevPost].append((newPost,sim))
                if not(prevPost.type == 'Core') and prevPost.weight/fad_sim(self.currTime,prevPost.timeStamp) >= delta1:
                    if prevPost.type == 'Border': self.borderPosts.remove(prevPost)
                    if prevPost.type == 'Noise': self.noise.remove(prevPost)
                    prevPost.type = 'Core'
                    self.corePosts.append(prevPost)
                    if prevPost not in self.S_:
                        self.S_pl.append(prevPost)
                    else:
                        self.S_.remove(prevPost)
                    for neighbour,we in self.graph[prevPost] :
                        if neighbour.type == 'Noise':
                            self.noise.remove(neighbour)
                            neighbour.type = 'Border'
                            self.borderPosts.append(neighbour)
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
                self.fileptr.write(f"{self.currTime}\t{delClustId}\t{self.getFrequentEntities(delClustId)}\t{NEXT_CLUSTER_ID}\n")
                NEXT_CLUSTER_ID += 1
        #self.clusters.pop(delClustId, None)
        del self.clusters[delClustId]

    def getFrequentEntities(self, cid):
        result = ""
        top_most = defaultdict(int)
        for post in self.clusters[cid]:
            for entity in set(post.entities):
                if entity in top_most.keys():
                    top_most[entity] += 1
                else:
                    top_most[entity] = 1
        tot = 0        
        k = Counter(top_most)
        high = k.most_common(5)
        for i in high:
            result += " "
            result += str(i[0])
        return result
    
    def printStats(self):
        print('************************************************************************************************************************************************************************')
        if self.posts[0].id < 80000: self.check_clus()
        print(self.currTime)
        print('No. of clusters: ',len(self.clusters.keys()))
        print('Len of posts ',len(self.posts))
        print('POten neighs: ',self.potconns)
        print('Conns formed : ',self.connnformed)
        print('Cores: ',len(self.corePosts),' set ', len(set(self.corePosts)))
        print('B: ',len(self.borderPosts),' set ', len(set(self.borderPosts)))
        print('N: ',len(self.noise),' set ', len(set(self.noise)))
        for post in self.posts[:10]:
            print('postid in posts', post.id)
        if len(self.noise)>10:
            for post in self.noise[:10]:
                print('noise weight', post.weight)
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
        self.purityptr.write(f'{avg_for_all_clus}\n')
        #print (self.clusters) 
        print('************************************************************************************************************************************************************************')        
        # time.sleep(5)


postGraph = PostNetwork()
df = pd.read_csv('AllEventsNew.csv', error_bad_lines=False, sep='\t').dropna()
print(df.tail())
start_time = time.time()
for index, row in df.iterrows():
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

print("--- %s seconds ---" % (time.time() - start_time))