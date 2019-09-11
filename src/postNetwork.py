from collections import defaultdict

epsilon0 = 0.25
nextPostId = 0

class Post:

    def __init__(self, entities, id=False, timeStamp="", author=""):
        global nextPostId
        self.entities = entities
        self.timeStamp = timeStamp
        self.author = author
        self.weight = 0
        if id is False:
            self.id = nextPostId
            nextPostId += 1
        else:
            self.id = id
    
        

class PostNetwork:
    
    def __init__ (self):
        self.posts = []
        self.entityDict = defaultdict(list)
        self.graph = defaultdict(list)
        self.sketchGraph = defaultdict(list)

    def addPost(self, post):
        self.posts.append(post)
        for noun in post.entities:
            self.entityDict[noun].append(post)

    def getPost(self, id):
        for post in self.posts:
            if post.id == id:
                return post
        return None
    
    def updateSimilarity(self, newPost):
        postId = newPost.id
        similarity = [0]*(postId+1)
        for word in self.posts[postId].entities:
            for posts in self.entityDict[word]:
                similarity[posts.id] += 1
        for posts in range(postId-1):
            prevPost = self.getPost(posts)
            sim = similarity[posts]/(len(newPost.entities)+len(prevPost.entities)-similarity[posts])
            if sim >= epsilon0:
                self.graph[postId].append([posts+1, sim])
                self.graph[posts+1].append([postId, sim])
                newPost.weight += sim
                prevPost.weight += sim
        


postGraph = PostNetwork()
file = open('../filtered_tweets.txt', 'r').read().split('\n')
for line in file:
    line = line.strip()
    if nextPostId % 100 == 0:
        print(f"Loaded {nextPostId} tweets")
    entities = line.split(' ')
    newPost = Post(entities)
    postGraph.addPost(newPost)
    postGraph.updateSimilarity(newPost)
