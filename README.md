# TreeGrabForReddit

The goal of this script is to provide a way of storing Reddit data into a tree-like data structure. The previous iteration, [RedditTreeGrab](https://github.com/mcandocia/RedditTreeGrab), relied on the shelve module and an earlier API version. This version should be much more efficient and easier to use for general querying.

For an example of previous work done using this type of data, see a course project I wrote a couple years ago: <https://www.scribd.com/document/250044180/Analyzing-Political-Discourse-on-Reddit>

The script is currently new and being debugged; here's how you can run it:

1. Set up PostgreSQL on your computer/server. Make a user to access the database.
2. Get a dev account with Reddit so you have a username, client ID, and client secret key
3. Edit dbinfo.py to reflect the PostgreSQL settings
4. Edit praw_user.py to reflect your Reddit developer account information.
5. Use the command line to run the code.

Also, see <http://maxcandocia.com/blog/2016/Dec/30/scraping-reddit-data/> for example use cases. Note that some features have been added since the post was made.

Right now the scraper (if run from scraper.py) can run through files if you manually give the post IDs and/or you supply a list of subreddits to cyclically collect data from. You can also supply a list of user names to scrape through beforehand, as well as having it search subreddits that have a minimum count frequency in the database as entries in the subreddit column for the comments and threads tables.

The basic command format is

```bash
python scraper.py SCHEMA_NAME --ids id1 id2 id3...
```

I also have another script, **get_comment_feed.py**, that allows you to get the top comments from any subreddit, including /r/all. The only downside is that they are new, so their final score/edits have yet to appear. The format for this command is

```bash
python get_comment_feed.py SCHEMA_NAME --subreddits sub1 sub2...
```

You can use `--help` if you want more details, but not all of the functions/features are implemented yet.

# Requirements

1. PostgreSQL (anything >=9.4 will work)
2. Python >= 2.7.6 (must be 2.x.x)
   * psycopg2 (install after PostgreSQL)
   * praw
   * pytz
3. A Reddit account with API permissions. You will need to make your own project page (most likely GitHub). See <https://www.reddit.com/wiki/api> .


# General Guidelines

* Reddit returns query results in groups of 100 (i.e., per API call). Unless you specifically want less data, it's recommended to have `--user_thread_limit`, `--user_comment_limit`, and `--limit` be integer multiples of 100.

* The `--history` parameter should have the same exact arguments each time you call a particular schema, since the presence of primary keys in the tables is based upon those.

* It takes roughly a second for each API call request. Gathering longer post and comment history of users takes up a very long time.

* You can use "random" or "randnsfw" as subreddit name arguments. However, post histories are not reused for subreddits selected this way. Larger `--limit` arguments will noticeably increase the amount of time it takes for the code to run, especially if less data is gathered from users via thread and comment limits. These names will not be used as a filter for rescraping.

* The `--pattern` argument is no longer particularly slow due to bugfixes, but navigating a thread can take a long time for large lengths of the pattern if there are many long comment chains in a thread. 

* If you want to scrape some subreddits more frequently than others, you can enter the subreddit name more than once either in the `--subreddits` argument or in the file referenced by the `-f_subreddits` argument.

* IDs are scraped first, then subreddits, then rescraping is done. By default, subreddits are skipped if rescraping is enabled unless `-n` is specified, since the subreddit argument is used as a filter for rescraping.

* `--constants` is not very well implemented yet, and if you want to automate a particular scrape command, you should save the command as text and run it in a python file with an `os.system()` call.

* You should probably do separate calls when rescraping posts/users. While you can share filters, the process can take a long time and they don't do the exact same thing.

* You can scrape subreddits now, as well as moderator information.

* You can scrape related subreddits using subreddit sidebars and wikis. There is also an option to generate your own .txt file of subreddits using `get_subreddit_list.py`.

## Other Notes

* The number of deleted comments in a thread is a bit buggy due to the way it's tracked. However, you can check to see how many empty author IDs/comments exist in a thread using SQL.

* If you discover any other bugs (unrelated to stdout output prettiness), please open a [new issue](https://github.com/mcandocia/TreeGrabForReddit/issues).
