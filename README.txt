The goal of this script is to provide a way of storing Reddit data into a tree-like data structure. The previous iteration, RedditTreeGrab, relied on the shelve module and an earlier API version. This version should be much more efficient and easier to use for general querying.

For an example of previous work done using this type of data, see a course project I wrote a couple years ago: https://www.scribd.com/document/250044180/Analyzing-Political-Discourse-on-Reddit 

The script is currently new and being debugged; here's how you can run it:

1. Set up PostgreSQL on your computer/server. Make a user to access the database.
2. Get a dev account with Reddit so you have a username, client ID, and client secret key
3. Edit dbinfo.py to reflect the PostgreSQL settings
4. Edit praw_user.py to reflect your Reddit developer account information.
5. Use the command line to run the code.

Right now the scraper (if run from scraper.py) can run through files if you manually give the post IDs and/or you supply a list of subreddits to cyclically collect data from. You can also supply a list of user names to scrape through beforehand.

The basic command format is

    python scraper.py SCHEMA_NAME --ids id1 id2 id3...

You can use --help if you want more details, but not all of the functions/features are implemented yet.

+--------------------+
|----REQUIREMENTS----|
+--------------------+

1. PostgreSQL (anything >=9.1 will work)
2. Python >= 2.7.6
   * psycopg2 (install after PostgreSQL)
   * praw
   * pytz
3. A Reddit account with API permissions. You will need to make your own project page (most likely GitHub). See https://www.reddit.com/wiki/api .


+--------------------+
|-GENERAL GUIDELINES-|
+--------------------+

* Reddit returns query results in groups of 100 (i.e., per API call). Unless you specifically want less data, it's recommended to have --user_thread_limit, --user_comment_limit, and --limit be integer multiples of 100.

* It takes roughly a second for each API call request. Gathering longer post and comment history of users takes up a very long time.

* You can use "random" or "randnsfw" as subreddit name arguments. However, post histories are not reused for subreddits selected this way. Larger --limit arguments will noticeably increase the amount of time it takes for the code to run, especially if less data is gathered from users via thread and comment limits.

* If the first value of --pattern is large, it may take a while for a larger thread to begin navigating, since it needs to weed out more non-top-level comments and expand potentially lower-level comment trees and get rid of those if they are lower-level.

* If you want to scrape some subreddits more frequently than others, you can enter the subreddit name more than once either in the --subreddits argument or in the file referenced by the -f_subreddits argument.

* IDs are scraped first, then subreddits. 

* --constants is not very well implemented yet, and if you want to automate a particular scrape command, you should save the command as text and run it in a python file with an os.system() call.

+---------------------+
|-----OTHER NOTES-----|
+---------------------+

* The number of comments navigated per thread variable is currently broken, but it should be possible via (possibly expensive) SQL calls to use comment IDs to rederive those values more accurately.

* If you discover any other bugs (unrelated to stdout output prettiness), please open a new issue.
