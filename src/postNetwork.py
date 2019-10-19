from collections import defaultdict
import datetime
import math
from datetime import timedelta

epsilon0 = 0.25
epsilon1 = 0.5
delta1 = 0.5
NEXT_POST_ID = 0
NEXT_CLUSTER_ID = 0
SLIDING_WINDOW = 500 #not implemented
TIME_STEP = 10


def fad_sim(a,b):
    datetimeFormat = '%S'
    diff = datetime.datetime.strptime(a, datetimeFormat)-datetime.datetime.strptime(b, datetimeFormat)
    return diff.seconds

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
        self.posts.append(post)
        for noun in post.entities:
            self.entityDict[noun].append(post)
        self.updateConns(post)

    def getPost(self, id):
        for post in self.posts:
            if post.id == id:
                return post
        return None

    def endTimeStep(self, currTime):
        global NEXT_CLUSTER_ID, TIME_STEP
        ########## S0 and Sn only have core posts
        S_, S_pl = list(),list()

        ## Checking for core->noncore posts
        for i,post in enumerate(self.corePosts):
            if post.weight/fad_sim(currTime,post.timeStamp) < delta1:
                post.type = 'Noise'
                del self.corePosts[i]
                S_.append(post)

        # Check for new core posts
        for i,post in enumerate(self.borderPosts):
            if post.weight/fad_sim(currTime,post.timeStamp) >= delta1:
                post.type = 'Core'
                del self.borderPosts[i]
                S_pl.append(post)

        for i,post in enumerate(self.noise):
            if post.weight/fad_sim(currTime,post.timeStamp) >= delta1:
                post.type = 'Core'
                del self.noise[i]
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
                if neiPost.type != 'Core':
                    if not 'Core' in [x.type for x,_ in self.graph[neiPost]]:
                        # Shouldn't be a borderpost
                        if neiPost.type == 'Border':
                            self.borderPosts.remove(neiPost)
                        neiPost.type = 'Noise'
                        self.noise.append(neiPost)
                    else:
                        # Should be a borderpost
                        if neiPost.type != 'Border':
                            self.noise.remove(neiPost)
                            self.borderPosts.append(neiPost)
                            neiPost.type = 'Border'

        neg_C = set()
        for post in self.S0+S_:
            for neiPost,we in self.graph[post]:
                if neiPost.type == 'Core' and we >= epsilon1:# Should we check if conn is core conn also?
                    neg_C.add(next(iter(neiPost.clusId)))# Gives element from a set
        if len(neg_C) == 0:
            return
        elif len(neg_C) == 1:
            return
        else:
            return

        pos_C = set()
        for post in self.Sn+S_pl:
            for neiPost,we in self.graph[post]:
                if neiPost.type == 'Core' and we >= epsilon1:# Should we check if conn is core conn also?
                    pos_C.add(next(iter(neiPost.clusId)))

        if len(pos_C) == 0:
            newClus = self.Sn + S_pl
            for post in self.Sn+S_pl:
                for neiPost,we in self.graph[post]:
                    newClus.append(neiPost)
                    neiPost.clusId.add(NEXT_CLUSTER_ID)
                post.clusId.add(NEXT_CLUSTER_ID)
            self.clusters[NEXT_CLUSTER_ID] = newClus
            NEXT_CLUSTER_ID += 1
        elif len(pos_C) == 1:
            cid = pos_C.pop()
            for post in self.Sn+S_pl:
                for neiPost,we in self.graph[post]:
                    if we >= epsilon0 and (not neiPost in self.Sn+S_pl):
                        newClus.add(neiPost)
                        neiPost.clusId.add(cid)
                post.clusId.add(cid)
        else:
            cid = pos_C.pop()
            for post in self.Sn+S_pl:
                for neiPost,we in self.graph[post]:
                    if we >= epsilon0 and (not neiPost in self.Sn+S_pl):
                        newClus.add(neiPost)
                        neiPost.clusId = cid
                post.clusId = cid
            for oldCid in pos_C:
                for post in self.clusters[oldCid]:
                    post.clusId.add(cid)
                    post.clusId.remove(oldCid)
        self.Sn.clear()
        self.S0.clear()
        self.currTime += TIME_STEP

    def startTimeStep(self, curr_time):
        # Delete old posts from self.posts and update weights, store in other array
        for post in self.posts:
            if curr_time - post.timeStamp > SLIDING_WINDOW:
                for neiPost,we in self.graph[post]:
                    neiPost.weight -= we
                if(post.type == 'Core'): 
                    self.corePosts.remove(post)
                    self.S0.append(post)
                elif(post.type == 'Border'): self.borderPosts.remove(post)
                elif(post.type == 'Noise'): self.noise.remove(post)
            else:
                break
        return

    def updateConns(self, newPost):
        postId = newPost.id
        similarity = defaultdict(lambda : 0)
        for word in self.posts[postId].entities:
            for posts in self.entityDict[word]:
                similarity[posts] += 1
        for prevPost in similarity.keys():
            sim = similarity[posts]/(len(newPost.entities)+len(prevPost.entities)-similarity[posts])
            newPost.weight += sim
            prevPost.weight += sim
            if sim/fad_sim(newPost.timeStamp,prevPost.timeStamp) > epsilon0:
                self.graph[newPost].append((prevPost,sim))
                self.graph[prevPost].append((newPost,sim))
        if newPost.weight/fad_sim(self.currTime,newPost.timeStamp) >= delta1:
            self.Sn.append(newPost)
            newPost.type = 'Core'
        else:
            newPost.type = 'Noise'
            self.noise.append(newPost)


postGraph = PostNetwork()
file = open('../filtered_tweets.txt', 'r').read().split('\n')
for line in file:
    line = line.strip()
    if NEXT_POST_ID % 100 == 0:
        print(f"Loaded {NEXT_POST_ID} tweets")
    entities = line.split(' ')
    newPost = Post(entities)
    postGraph.addPost(newPost)
