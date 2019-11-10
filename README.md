# Event Evolution Tracking

* We used Stanford POS tagger. Model used is Twittie tagger fast model (which is trained for tagging tweets specifically considering that a tweet need not be grammatically correct) and  extracted nouns (NNP, NN, NNPS and NNS).
* We used two classes, Post (to store details of a post) and PostNetwork (to store the current state of graph). Respective attributes are described by the comments beside them in the code.
* Using these entities we built a bipartite graph (dictionary) between entities and posts which contain that particular entity.
* startTimeStep() function deletes posts outside the sliding window and reclusters the post graph. We parallely checked for posts turning from core to non-core and border to noise.
* In updateConns(), using Jaccard similarity, we find similarity between posts and if that exceeds a certain threshold we form an edge between the posts. updateConns() is called when we add a new post to the graph. We form connections with the posts having atleast one common entity with new post. We check for new border and new core posts here.
* endTimeStep() function is called after collecting all new posts in this time step. Here we check for posts changing from core to non-core due to progress of time. We, then, do bulk updating of clusters (addition and deletion).
* nc_p0() function is used to break a cluster into multiple clusters when a core post is deleted.
