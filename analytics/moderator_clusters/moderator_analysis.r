library(dplyr)
library(ape)
library(RColorBrewer)
library(igraph)

setwd('/hddl/workspace/TreeGrabForReddit/analytics/moderator_clusters/')

moderator_data = read.csv('mod_subreddit.csv')

#filter out automod
moderator_data = filter(moderator_data, username != 'AutoModerator')

jaccard_index = function(data){
  #get list of subreddits
  subreddits = levels(data$subreddit)
  N = length(subreddits)
  jaccard_matrix = matrix(1, nrow=N, ncol=N)
  rownames(jaccard_matrix) = subreddits
  colnames(jaccard_matrix) = subreddits
  for (i in 1:(N-1)){
    for (j in (i+1):N){
      di = filter(data, subreddit==subreddits[i])
      dj = filter(data, subreddit==subreddits[j])
      jaccard_matrix[i,j] = jaccard_matrix[j,i] = length(base::intersect(di$username, dj$username))/
						   length(base::union(di$username, dj$username))
    }
  }
  return(jaccard_matrix)
}

adjacency_matrix = function(data){
  #get list of subreddits
  subreddits = levels(data$subreddit)
  N = length(subreddits)
  adj_matrix = matrix(0, nrow=N, ncol=N)
  rownames(adj_matrix) = subreddits
  colnames(adj_matrix) = subreddits
  for (i in 1:(N-1)){
    for (j in (i+1):N){
      di = filter(data, subreddit==subreddits[i])
      dj = filter(data, subreddit==subreddits[j])
      adj_matrix[i,j] = adj_matrix[j,i] = length(base::intersect(di$username, dj$username))
    }
  }
  return(adj_matrix)
}

moderator_distances = as.dist(1-jaccard_index(moderator_data))
moderator_adjacency = adjacency_matrix(moderator_data)

##first do clustering

mod_cluster = hclust(moderator_distances, method="ward.D")
##I don't really like how this looks, despite it being somewhat useful; let's use Gephi
plot(as.phylo(mod_cluster), type='fan')

#export adjacency matrix to edgelist
g <- graph.adjacency(moderator_adjacency, weighted=TRUE) 
moderator_edgelist <- get.data.frame(g)
colnames(moderator_edgelist) = c("Source", "Target",'Weight')
moderator_edgelist$Type = 'undirected'

write.table(file='moderator_edgelist.csv', moderator_edgelist, row.names=FALSE, sep=',', col.names=TRUE)

#let's get # of moderators per subreddit go get size of nodes
subreddit_sizes = moderator_data %>% group_by(subreddit) %>% summarise(count=length(subreddit)) 
colnames(subreddit_sizes)[1] = 'Id'
write.csv(file='subreddit_sizes.csv', subreddit_sizes, row.names=FALSE)