from collections import defaultdict

epsilon0 = 0.25

class postNetwork:
    
    def __init__ (self):
        self.postDict = defaultdict(list)
        self.entityDict = defaultdict(list)
        self.graph = defaultdict(list)
        self.hittingCount = []

    def addEdge (self, post, noun):
        self.postDict[post].append(noun)
        self.entityDict[noun].append(post)

    def updateSimilarity(self, post):
        similarity = [0]*post
        for word in self.postDict[post]:
            for posts in self.entityDict[word]:
                similarity[posts-1] += 1
        self.hittingCount.append(similarity)
        entityPost = postGraph.hittingCount[post-1][post-1]
        for posts in range(post-1):
            entityPost2 = postGraph.hittingCount[posts][posts]
            common = postGraph.hittingCount[post-1][posts]
            sim = common/(entityPost+entityPost2-common)
            if sim >= epsilon0:
                self.graph[post].append([posts+1, sim])
                self.graph[posts+1].append([post, sim])
        


postGraph = postNetwork()
file = open("filtered_tags.txt", 'r').read().split('\n')
postCount = 0
for line in file:
    line = line.strip()
    postCount += 1
    entities = line.split(' ')
    for entity in entities:
        postGraph.addEdge(postCount, entity)
    postGraph.updateSimilarity(postCount)

# print(postGraph.graph)