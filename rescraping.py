import datetime
import pytz
import re
from praw_object_data import retry_if_broken_connection
from prawcore.exceptions import NotFound, RequestException, Forbidden

from user_scrape import scrape_user
from thread_process import process_thread

#arguments that are relevant to choosing which IDs to rescrape:
#THREADS:
# * age - age of thread must be within specific range
# * thread_delay - if this exists, the timestamp must have passed a certain time range
# * subreddits - if this exists, all threads must be contained in the supplied subreddits (random
#   is ignored
#USERS:
# * user_delay - if this exists, the timestamp must have passed a certain time range
# * subreddits - if this exists, then a user's comment/post history must have intersected with
#   at least one of the specified subreddits

#note: unlike the on-line version of validation that is done in the scraper.py file, validation is
#done just once here, in one query

def construct_age_part_of_query(opts, user=False):
    if not opts.age:
        return ' true '
    now = datetime.datetime.now(pytz.utc)
    lower_bound = now - datetime.timedelta(days = opts.age[1])
    upper_bound = now - datetime.timedelta(days = opts.age[0])
    if not user:
        return opts.db.cur.mogrify(" created BETWEEN %s AND %s ", [lower_bound, upper_bound])
    else:
        return ' true '
    #return opts.db.cur.mogrify(" account_created BETWEEN %s AND %s ",
    #[lower_bound, upper_bound])

def construct_delay_part_of_query(opts, delay=None):
    if not delay or delay==-1:
        return ' true '
    now = datetime.datetime.now(pytz.utc)
    return opts.db.cur.mogrify(" timestamp < %s ", [now])

def construct_subreddit_part_of_thread_query(opts):
    if len(opts.subreddits) == 0:
        return ' true '
    elif all([s in ['random','randnsfw'] for s in opts.subreddits]):
        return ' true '
    return opts.db.cur.mogrify(" subreddit = ANY(%s) ", (opts.subreddits,))

def construct_subreddit_part_of_user_query(opts):
    if len(opts.subreddits) == 0:
        return ' true '
    elif all([s in ['random','randnsfw'] for s in opts.subreddits]):
        return ' true '
    string = opts.db.cur.mogrify(''' username IN 
    (SELECT DISTINCT author_name FROM %%s.comments 
    WHERE subreddit=ANY(%s)
    UNION
    SELECT DISTINCT author_name FROM %%s.threads
    WHERE subreddit=ANY(%s))''', (opts.subreddits, opts.subreddits))
    string = string % (opts.db.schema, opts.db.schema)
    return string

def construct_scrape_mode_part_of_query(opts):
    if opts.avoid_full_threads:
        return " scrape_mode='minimal' "
    if opts.full_rescraping:
        return ' true '
    else:
        return " scrape_mode='thread' "

#currently disabled since no way of easily gathering minimum comments for comment-based IDs
#additionally, more comments may have been added to a thread in the future
def construct_min_comments_part_of_query(opts):
    if not opts.mincomments or True:
        return ' true '
    else:
        return ' num_comments=%d ' % opts.mincomments

def rescrape_users(scraper, opts):
    query = ((''' SELECT username FROM %s.users WHERE ''' +
             'AND'.join([construct_age_part_of_query(opts, True),
                         construct_delay_part_of_query(opts, opts.user_delay),
                         construct_subreddit_part_of_user_query(opts)]) +
             ' ORDER BY random();') % opts.db.schema)
    print query
    opts.db.execute(query)
    user_list = opts.db.fetchmany(1000)
    while len(user_list) > 0:
        for username_ in user_list:
            username = username_[0]
            scrape_user(username, opts, scraper)
        if len(user_list) < 1000:
            break
        user_list = opts.db.fetchmany(1000)
        
    print 'done rescraping users'
    

def rescrape_threads(scraper, opts):
    if not opts.rescrape_with_comment_histories:
        query = ('SELECT id FROM %s.threads WHERE ' +
                 'AND'.join([construct_age_part_of_query(opts),
                             construct_delay_part_of_query(opts),
                             construct_subreddit_part_of_thread_query(opts),
                             construct_scrape_mode_part_of_query(opts),
                             construct_min_comments_part_of_query(opts)]) +
                 ' ORDER BY random();') % opts.db.schema
        print query
        opts.db.execute(query)
    else:
        query = ('SELECT id FROM (SELECT DISTINCT id FROM (SELECT id FROM %s.threads' +
                 ' WHERE ' + 
                 'AND'.join([construct_age_part_of_query(opts),
                             construct_delay_part_of_query(opts),
                             construct_subreddit_part_of_thread_query(opts),
                             construct_scrape_mode_part_of_query(opts)])  + '' + 
                 'UNION SELECT thread_id FROM %s.comments WHERE ' +
                 'AND'.join([construct_age_part_of_query(opts),
                             construct_delay_part_of_query(opts),
                             construct_subreddit_part_of_thread_query(opts),
                             construct_scrape_mode_part_of_query(opts)]) + '' + 
                 ') t2) t3  ORDER BY random();') % (opts.db.schema, opts.db.schema)
        print query
        opts.db.execute(query)
    thread_list = opts.db.fetchmany(1000)
    while len(thread_list) > 0:
        for thread_id_ in thread_list:
            thread_id = thread_id_[0]
            process_thread(thread_id, opts, scraper)
        if len(thread_list) < 1000:
            break
        thread_list = opts.db.fetchmany(1000)
    print 'done rescraping threads'
