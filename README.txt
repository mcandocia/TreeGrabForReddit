The goal of this script is to provide a way of storing Reddit data into a tree-like data structure. The previous iteration, RedditTreeGrab, relied on the shelve module and an earlier API version. This version should be much more efficient and easier to use for general querying.

For an example of previous work done using this type of data, see a course project I wrote a couple years ago: https://www.scribd.com/document/250044180/Analyzing-Political-Discourse-on-Reddit 

The script is currently in its early stage, but here's how you can run it:

1. Set up PostgreSQL on your computer/server. Make a user to access the database.
2. Get a dev account with Reddit so you have a username, client ID, and client secret key
3. Edit dbinfo.py to reflect the PostgreSQL settings
4. Edit praw_user.py to reflect your Reddit developer account information.

Right now the scraper (if run from scraper.py) will only go through files by ID (supplied in a line-separated file and/or manually on the command-line).

The basic command format is

    python scraper.py SCHEMA_NAME --ids id1 id2 id3...

You can use --help if you want more details, but not all of the functions/features are implemented yet.
