## build_subreddit_and_domain_indexes.py
# Max Candocia - mscandocia@gmail.com
# 09/01/2019
#
# builds indexes on comments and threads table in specified schemas for domains, subreddits, and usernames

from __future__ import print_function
import sys
import db

def main(schemas):
    for schema in schemas:
        cur = db.Database(name=schema)
        cur.build_username_subreddit_domain_indexes(commit=True, verbose=True)

            
    print('Done!')

if __name__=='__main__':
    main(sys.argv[1:])
