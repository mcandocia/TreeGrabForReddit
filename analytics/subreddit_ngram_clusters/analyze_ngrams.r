setwd('/hddl/workspace/TreeGrabForReddit/analytics/subreddit_ngram_clusters/')
library(dplyr)
library(ape)
library(RColorBrewer)

subreddit_ngrams = read.csv('ngram_data.csv')

subreddit_tfidf = subreddit_ngrams
for (i in 2:ncol(subreddit_tfidf)){
  subreddit_tfidf[,i] = subreddit_tfidf[,i]/sum(subreddit_tfidf[,i])
}

cosine_similarity_matrix <- function(data){
  subreddits = data$SUBREDDIT 
  n_subreddits = nrow(data)
  sub_matrix = matrix(1, nrow = n_subreddits, ncol = n_subreddits)
  rownames(sub_matrix) = as.character(subreddits)
  colnames(sub_matrix) = as.character(subreddits)
  for (i in 1:(n_subreddits-1)){
    row_i = as.numeric(data[i,-1])
    for (j in (i+1):n_subreddits){
    row_j = as.numeric(data[j,-1])
      sub_matrix[i, j] = sub_matrix[j, i] = 2 * row_i %*% row_j / (sum(row_i^2) + sum(row_j^2))
    }
  }
  return(sub_matrix)
}

similarity_matrix = cosine_similarity_matrix(subreddit_tfidf)
similarity_matrix = ifelse(is.na(similarity_matrix), 0, similarity_matrix)

subreddit_dist = as.dist(1-similarity_matrix)

ngram_clusts = hclust(subreddit_dist, method='ward.D')

tree = cutree(ngram_clusts, 12)
pal = brewer.pal(8,'Dark2')

plot(as.phylo(ngram_clusts), type='fan', tip.color=pal[tree %% 8 + 1], cex=0.8,
  main='Clustering of Subreddits by Word & Phrase Similarity of Comments')
  
  plot(as.phylo(ngram_clusts), tip.color=pal[tree %% 8 + 1], cex=0.8,
  main='Clustering of Subreddits by Word/Phrase Similarity')
  
#try a compound graph to see what happens

compound_dist = subreddit_dist + unsigned_dist