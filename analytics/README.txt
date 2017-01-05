These are some files I used for some analytics of Reddit data. They aren't particularly refined, but you may find them useful.

Description:

* dendrogram: The python script allows you to summarize scores of post or comment history of users across subreddits of your choosing. The R files allow you to transform and cluster that data.

* moderator_clusters: This is a simple SQL script that can be run to export data to a CSV, and then a Reddit script that can convert the data to an edgelist (I did so after realizing that a dendrogram does not do a good job of showing how the data is related)

* subreddit_ngram_clusters: A python script to parse ngrams (default N = 1, 2, and 3) from comments from subreddits of your choosing. The R script uses cosine similarity and then clusters in a manner similar to those in dendrogram.r

