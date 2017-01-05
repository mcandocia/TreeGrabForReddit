--dump moderator data to CSV files

COPY (SELECT DISTINCT subreddit, username FROM politics.moderators) TO 
 '/hddl/workspace/TreeGrabForReddit/analytics/moderator_clusters/mod_subreddit.csv' 
 DELIMITER ',' CSV HEADER;

