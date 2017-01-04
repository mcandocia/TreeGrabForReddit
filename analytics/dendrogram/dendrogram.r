library(ape)
library(RColorBrewer)
library(dplyr)

setwd('/hddl/workspace/RedditTreeGrab2/analytics/dendrogram')

df1 = read.csv('data/politics_summary.csv')

#let's see the difference in dendrograms if count > 0 is used vs. pos_count > 1 and neg_count > 1 are used

df_signed = rbind(
	      df1 %>% filter(pos_count > 0) %>% 
		select(username, subreddit) %>% 
		mutate(subreddit=factor(paste('+',as.character(subreddit))))
	      , 
	      df1 %>% filter(neg_count > 0) %>% 
		select(username, subreddit) %>% 
		mutate(subreddit=factor(paste('-',as.character(subreddit))))
	      )

df_unsigned = df1 %>% filter(count > 0) %>% 
		select(username, subreddit) 
##jaccard index is reliable, though biased towards clustering subreddits of similar sizes

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

unsigned_dissimilarity = 1 - jaccard_index(df_unsigned)
signed_dissimilarity = 1 - jaccard_index(df_signed)

unsigned_dist = as.dist(unsigned_dissimilarity)
signed_dist = as.dist(signed_dissimilarity)

signed_clust = hclust(signed_dist, method='ward.D')
unsigned_clust = hclust(unsigned_dist, method='ward.D')

#note that all signed elements are close to each other, indicating that differentiation via sign is unimportant
#exception in the case of communism and debatecommunism, where sign separates those two rather than subreddit
plot(as.phylo(signed_clust), type='fan', main='Signed Clustering of Subreddits',cex=0.8)

clusts = cutree(unsigned_clust, 14)
ccolors = brewer.pal(8,'Dark2')
plot(as.phylo(unsigned_clust), type='fan', main='Similarity Clustering of Political Subreddits', cex=0.9,
tip.color=ccolors[clusts %% 8 + 1])

##try different clustering algorithm

signed_clust2 = hclust(signed_dist, method='single')
unsigned_clust2 = hclust(unsigned_dist, method='single')

#results are much worse...
plot(as.phylo(unsigned_clust2), type='fan', main='Similarity Clustering of Political Subreddits', cex=0.9)

##let's try the Rand Index
rand_index = function(data){
  #get list of subreddits
  subreddits = levels(data$subreddit)
  all_users = levels(data$username)
  n_users = length(all_users)
  n_combinations = choose(n_users,2)
  N = length(subreddits)
  rand_matrix = matrix(1, nrow=N, ncol=N)
  rownames(rand_matrix) = subreddits
  colnames(rand_matrix) = subreddits
  for (i in 1:(N-1)){
    di = filter(data, subreddit==subreddits[i])
    i_total = nrow(di)
    for (j in (i+1):N){
      dj = filter(data, subreddit==subreddits[j])
      j_total = nrow(dj)
      n_common = length(base::intersect(di$username, dj$username))
      n_union = length(base::union(di$username, dj$username))
      a = choose(n_common, 2)
      b = choose(n_users - n_union, 2)
      rand_matrix[i,j] = rand_matrix[j,i] = (a + b)/n_combinations
    }
  }
  return(rand_matrix)
}


##rand appears to be a bad choice since the dominating effect is the sample size rather than similarity
unsigned_dissimilarity_rand = 1 - rand_index(df_unsigned)
signed_dissimilarity_rand = 1 - rand_index(df_signed)

unsigned_dist_rand = as.dist(unsigned_dissimilarity_rand)
signed_dist_rand = as.dist(signed_dissimilarity_rand)

signed_clust_rand = hclust(signed_dist_rand, method='ward.D2')
unsigned_clust_rand = hclust(unsigned_dist_rand, method='ward.D2')

plot(as.phylo(signed_clust), type='fan', main='Signed Clustering of Subreddits')

clusts_rand = cutree(unsigned_clust_rand, 14)
plot(as.phylo(unsigned_clust_rand), type='fan', main='Similarity Clustering of Political Subreddits', cex=0.9,
tip.color=ccolors[clusts_rand %% 8 + 1])


##let's try the adjusted Rand Index, which may possibly yield distances > 1

adjusted_rand_index = function(data){
  #get list of subreddits
  subreddits = levels(data$subreddit)
  all_users = levels(data$username)
  n_users = length(all_users)
  n_combinations = choose(n_users,2)
  N = length(subreddits)
  rand_matrix = matrix(1, nrow=N, ncol=N)
  rownames(rand_matrix) = subreddits
  colnames(rand_matrix) = subreddits
  for (i in 1:(N-1)){
    di = filter(data, subreddit==subreddits[i])
    i_total = nrow(di)
    for (j in (i+1):N){
      dj = filter(data, subreddit==subreddits[j])
      j_total = nrow(dj)
      n_common = length(base::intersect(di$username, dj$username))
      n_union = length(base::union(di$username, dj$username))
      a = choose(n_common, 2)
      b = choose(n_users - n_union, 2)
      rand_matrix[i,j] = rand_matrix[j,i] = (a - choose(i_total,2)*choose(j_total,2)/n_combinations
	)/(1/2*(choose(i_total, 2) + choose(j_total, 2)) - (choose(i_total, 2)*choose(j_total, 2)/n_combinations))
    }
  }
  return(rand_matrix)
}

##adjusted rand index may work better
##it looks better, but jaccard seems to work best, despite size differences
##perhaps clustering by generative model loadings would work better
unsigned_dissimilarity_arand = 1 - adjusted_rand_index(df_unsigned)
signed_dissimilarity_arand = 1 - adjusted_rand_index(df_signed)

unsigned_dist_arand = as.dist(unsigned_dissimilarity_arand)
signed_dist_arand = as.dist(signed_dissimilarity_arand)

signed_clust_arand = hclust(signed_dist_arand, method='ward.D2')
unsigned_clust_arand = hclust(unsigned_dist_arand, method='ward.D')

plot(as.phylo(signed_clust_arand), type='fan', main='Signed Clustering of Subreddits')

clusts_arand = cutree(unsigned_clust_arand, 13)
plot(as.phylo(unsigned_clust_arand), type='fan', main='Similarity Clustering of Political Subreddits', cex=0.9,
tip.color=ccolors[clusts_arand %% 8 + 1])