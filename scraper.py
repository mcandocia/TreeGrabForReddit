from __future__ import print_function

try:
    unicode
except:
    unicode = str 

import praw
import re
import os
import time
import requests
from requests.exceptions import HTTPError, ConnectionError
try:
    from exceptions import UnicodeDecodeError, IndexError, AttributeError
except ImportError:
    pass
import calendar
import datetime
import sys
import random
import socket
from socket import AF_INET, SOCK_DGRAM
from argparse import ArgumentParser
from prawcore.exceptions import NotFound
import importlib

import rescraping

from navigator import Navigator

from praw_user import scraper

from user_scrape import scrape_user
from thread_process import process_thread
from praw_object_data import get_comment_data
from writer import write_comment
from moderator_scrape import scrape_moderators
from subreddit_scrape import scrape_subreddits
from new_users import get_new_users

from praw_object_data import retry_if_broken_connection
from rescrape_from_schema import rescrape_users_from_schema

from get_unscraped_ids import get_unscraped_ids

import pytz
from tzlocal import get_localzone

import db

# helps with redirected output
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

# if a command such as "python scraper.py ... | tee -a mylog" is used
# the exception is handled slightly differently, as a different error is raised

LOCAL_TIMEZONE = get_localzone()

#used for keyboard interrupt logging
logging = False
logopts = None

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def clean_keyboardinterrupt(f):
    def func(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except KeyboardInterrupt as e:
            print('exiting via keyboard interrupt')
            if logging:
                print('making log')
                logopts.db.update_log_entry(logopts, 'Keyboard Interrupt')
            sys.exit()

    return func



def get_age(timestamp, localize=True):
    if localize:
        now = datetime.datetime.now()
    else:
        now = datetime.datetime.utcnow()
    difference = (now - timestamp).total_seconds()
    #day_difference = (now - timestamp).days
    days = float(difference) / 3600. / 24.# + day_difference
    return days

@retry_if_broken_connection
def get_subreddit_posts(subreddit, opts):
    posts = getattr(subreddit, opts.rank_type)(limit=opts.limit)
    return [post for post in posts]

@retry_if_broken_connection
def validate_post(post, opts):
    #check number of comments
    if opts.mincomments:
        if opts.mincomments > post.num_comments:
            return False
    #check if age is correct if exists
    if opts.age:
        thread_time = datetime.datetime.utcfromtimestamp(post.created_utc)
        if thread_time is not None:
            thread_age = get_age(thread_time, localize=False)
            #print(thread_age)
            if thread_age < opts.age[0] or thread_age > opts.age[1]:
                return False
            
    #check if already in database, then check if able to be rescraped
    if post.num_comments > opts.max_comments:
        return False
    update_time = opts.db.get_thread_update_time(post.id)
    if update_time is None:
        return True
    else:
        if opts.thread_delay == -1:
            return False
        age = get_age(update_time)
        print('age: %d' % age)
        if age > opts.thread_delay:
            print('UPDATING THREAD')
        return age > opts.thread_delay

@retry_if_broken_connection
def select_post(subreddit_name, post_dict, opts, reddit_scraper, refreshed=False):
    valid_posts = post_dict[subreddit_name]
    while len(valid_posts) > 0:
        if opts.unordered:
            post = valid_posts.pop(random.choice(range(len(valid_posts))))
        else:
            post = valid_posts.pop(0)
        if validate_post(post, opts):
            return post
        elif opts.reappend:
            valid_posts.append(post)
    if not refreshed:
        if subreddit_name in opts.subreddit_refresh_timer_dict:
            time_remaining = time.time() - opts.subreddit_refresh_timer_dict[subreddit_name] + opts.subreddit_dict_refresh_min_period
            if time_remaining > 0:
                print('not enough time has passed to refresh dict (%ss remaining)' % round(time_remaining))
                if opts.terminate_after_sub_dict_unusable:
                    print('Exiting...')
                    exit(0)
                return None
        print('refreshing dictionary for %s' % subreddit_name)
        post_dict[subreddit_name] = get_subreddit_posts(
            reddit_scraper.subreddit(subreddit_name),
            opts
        )
        opts.subreddit_refresh_timer_dict[subreddit_name] = time.time()
        
        return select_post(subreddit_name, post_dict, opts, reddit_scraper, refreshed=True)
    else:
        print('cannot find valid entries for %s' % subreddit_name)
        return None

@retry_if_broken_connection
def get_subreddit(subreddit_name, scraper):
    return scraper.subreddit(subreddit_name)


def filter_users(opts):
    # filter out users who have already been scraped
    opts.db.filter_users(opts)
    # yeah, great programming...


@clean_keyboardinterrupt
def main(args):
    opts = options()
    reddit_scraper = scraper(token=opts.token)
    #parse users
    if opts.resample_users_from_schema:
        original_schema = opts.resample_users_from_schema
        print('Rescraping from %s.users' % original_schema)
        rescrape_users_from_schema(original_schema, opts, reddit_scraper)
        print('Done')
        exit(0)
    if len(opts.users) > 0:
        print('Filtering users')
        filter_users(opts)
        opts.old_user_comment_limit = opts.user_comment_limit
        opts.old_user_thread_limit = opts.user_thread_limit
        if opts.man_user_comment_limit != None:
            opts.user_comment_limit = opts.man_user_comment_limit
        if opts.man_user_thread_limit != None:
            opts.user_thread_limit = opts.man_user_thread_limit
    if opts.shuffle_usernames:
        print('Shuffling users')
        random.shuffle(opts.users)
    for username in opts.users:
        scrape_user(username, opts, reddit_scraper, force_read=opts.deepuser)
    if len(opts.users) > 0 and opts.deepuser:
        random.shuffle(opts.ids)
    elif opts.unordered and len(opts.ids) > 1:
        random.shuffle(opts.ids)
        random.shuffle(opts.comment_ids)
    if len(opts.users) > 0:
        opts.user_comment_limit = opts.old_user_comment_limit
        opts.user_thread_limit = opts.old_user_thread_limit
    #parse thread ids
    print('pattern:', opts.pattern)

    if not opts.thread_batch_mode:
        #print('n thread ids: %s' % len(opts.ids))
        for i, thread_id in enumerate(opts.ids):
            process_thread(thread_id, opts, reddit_scraper)
            if opts.N != -1 and i>= opts.N:
                break
    else:
        id_batches = chunks(['t3_' + id for id in opts.ids], 100)
        cnt = 0
        for i, id_batch in enumerate(id_batches):
            print('THREAD ID BATCH #%s' % (i+1))
            threads = reddit_scraper.info(id_batch)
            for j, thread in enumerate(threads):
                process_thread(thread, opts, reddit_scraper)
                cnt += 1
                if opts.N != -1 and cnt >= opts.N:
                    break
            if opts.N != -1 and cnt >= opts.N:
                break
            
    if len(opts.ids) > 0:
        counter = i+1
        print('finished with supplied thread ids')
    else:
        print('---------------------------------')
        counter = 0

    # comment scraping
    if not opts.comment_batch_mode:
        print('Currently only supporting batch mode for comment scraping')
        
    if True:
        id_batches = chunks(['t1_' + id for id in opts.comment_ids], 100)
        cnt = 0
        for i, id_batch in enumerate(id_batches):
            print('COMMENT ID BATCH #%s' % (i+1))
            comments = reddit_scraper.info(id_batch)
            n_bad = 0
            for j, comment in enumerate(comments):
                if comment is None:
                    print('Skipping comment %s' % comment.id)
                try:
                    comment_data = get_comment_data(comment, opts, mode='direct', scraper=reddit_scraper)
                except AttributeError as e:
                    comment_data = {}
                if len(comment_data) == 0:
                    #print('COULD NOT FIND COMMENT DATA FOR ID %s!!!' % comment.id)
                    n_bad += 1
                else:
                    cnt += 1
                    write_comment(comment_data[comment.id], opts)
                if opts.N != -1 and cnt >= opts.N:
                    break
            if n_bad > 0:
                print('Total bad in batch = %s' % n_bad)
            if opts.N != -1 and cnt >= opts.N:
                break
            
    if len(opts.comment_ids) > 0:
        counter = i+counter
        print('finished with supplied thread ids')

    
    # user scraping
    if opts.new_user_scrape_mode:
        print('Entering new user scrape mode. Beginning counter.')
        new_user_set = set()
        while opts.N == -1 or counter < opts.N:
            counter+=1
            new_users = get_new_users(reddit_scraper, new_user_set)
            if len(new_users) == 0:
                print('No new users found. Sleeping for 30 seconds.')
                counter-=1
                time.sleep(30)
            else:
                for username in new_users:
                    scrape_user(
                        username,
                        opts,
                        reddit_scraper,
                        force_read=False
                    )
        print('Exited new user loop')
            
    #go through subreddits
    n_subreddits = len(opts.subreddits)
    subreddit_post_dict = {}
    old_subreddit_post_dict = {}
    if opts.shuffle_subreddits:
        random.shuffle(opts.subreddits)
    while (opts.N == -1 or counter < opts.N) and n_subreddits > 0:
        subreddit_name = opts.subreddits[counter % n_subreddits]
        if subreddit_name not in subreddit_post_dict or subreddit_name in ['random','randnsfw']:
            subreddit = get_subreddit(subreddit_name, reddit_scraper)
            subreddit_post_dict[subreddit_name] = (get_subreddit_posts(subreddit, opts) + \
                                                  old_subreddit_post_dict.get(subreddit_name, [])
                                                   )[:15000]
            if subreddit_name in old_subreddit_post_dict:
                old_subreddit_post_dict.pop(subreddit_name)
        thread = select_post(subreddit_name, subreddit_post_dict, opts, reddit_scraper)
        if thread is None:
            print('skipping %s due to insufficient posts' % subreddit_name)
        else:
            process_thread(thread.id, opts, reddit_scraper)
        if len(subreddit_post_dict[subreddit_name]) == 0:
            subreddit_post_dict.pop(subreddit_name)
        counter += 1
        print('finished with %d threads' % counter)
        #check to see if dict should be refreshed
        if opts.post_refresh_time:
            if opts.post_refresh_time > get_age(opts.dictionary_time, localize=False):
                opts.dictionary_time = datetime.datetime.utcnow()
                if not opts.drop_old_posts:
                    old_subreddit_post_dict = subreddit_post_dict
                subreddit_post_dict = {}
    #moderator rescraping
    if opts.SCRAPE_ANY_SUBREDDITS:
        print('beginning subreddit scraping')
        scrape_subreddits(opts, reddit_scraper)
    if opts.moderators:
        scrape_moderators(opts, reddit_scraper)
    if opts.N != 0 and len(opts.subreddits):
        print('done with subreddit searching')
    if opts.rescrape_threads:
        rescraping.rescrape_threads(reddit_scraper, opts)
        print('done rescraping threads')
    if opts.rescrape_users:
        rescraping.rescrape_users(reddit_scraper, opts)
        print('done rescraping users')
        
    print('done')
    if opts.log:
        opts.db.update_log_entry(opts, 'completed')
    return 0

class options(object):
    """this class processes command-line input to create overall scraper options"""
    
    def __init__(self, *args, **kwargs):
        #initialize global accumulators
        self.deepcounter = 0
        #logging variables
        self.original_command =' '.join(sys.argv)
        self.start_time = datetime.datetime.now(pytz.utc)
        #create parser and add arguments
        parser = ArgumentParser(description='scrape data from Reddit')
        parser.add_argument('name',
                            help="the schema name for the tables to be stored in",
                            default='default')
        parser.add_argument('-p','--pattern',dest='pattern',nargs='+',
                            default=['D'] + [5e5 for _ in range(30)],
                            help='a space-separated list of ints that describe the maximum number'\
                            ' of comments that will be scraped at each level, with the first'\
                            'argument indicating the root of a thread (top-level comments)'\
                            'the default "D" indicates that it should be overwritten by a '\
                            'provided constant if a constants file is specified')
        parser.add_argument('-s','--subreddits', dest='subreddits', nargs='+',
                            help='a list of subreddits that will be rotated through for scraping')
        parser.add_argument('--ids',dest='thread_ids', nargs='+',
                            help='a list of thread IDs that will be scraped')
        parser.add_argument('-fs','--f-subreddits', dest='f_subreddits', nargs='+',
                            help='a list of filenames with line-separated subreddit names to be '\
                            ' added to a list of subreddits that will be scraped on a rotation')

        parser.add_argument('--c-ids', '--comments', dest='comment_ids', nargs='+',
                            help='A list of comment IDs to scrape')
        parser.add_argument('-fc','--f-comment-ids', dest='f_comment_ids', nargs='+',
                            help='a list of filenames with line-separated comment IDs to be scraped')
        
        parser.add_argument('-u','--users', dest='users', nargs='+',
                            help='A list of users to search through')
        parser.add_argument('--man-user-comment-limit',dest='man_user_comment_limit',type=int,
                            help="If user IDs are supplied, this overrides --user-comment-limit"\
                            " for those IDs.")
        parser.add_argument('--man-user-thread-limit', dest='man_user_thread_limit',type=int,
                            help="If user IDs are supplied, this overrides --user-thread-limit"\
                            " for those IDs.")
        parser.add_argument('-fu','--f-users', dest='f_users',nargs='+',
                            help="A filename containing line-separated usernames to be scraped.")
        parser.add_argument('--f-ids',dest='f_thread_ids', nargs='+',
                            help='a list of filenames with line-separated thread IDs to be scraped'\
                            ' sequentially')
        parser.add_argument('--log', dest='log',action='store_true',
                            help='Stores command, stop/start times, and start/stop causes to a log'\
                            ' in the database (under schema.log)')
        parser.add_argument('-a','--age',dest='age', nargs=2,type=float,
                            help='optional list of 2 values, representing the age in days of posts'\
                            ' that can be scraped; format [lower number] [higher number]')
        parser.add_argument('-uo','--unordered',action='store_true',
                            help='if it exists, then whenever scraping is done from a subreddit, '\
                            'the thread chosen for scraping is chosen at random as opposed to '\
                            'being put in a queue')
        
        parser.add_argument('--history',dest='history', nargs='*',choices=['threads','users',
                                                                           'comments',
                                                                           'subreddits'],
                            help="this option accepts up to three arguments, indicating which "\
                            'tables should have new rows made instead of row updates for when '\
                            'data is scraped. This should be disabled unless you need historical '\
                            'data, since it can affect performance for large databases and make '\
                            'queries more complicated.')
        parser.add_argument('-td', '--thread-delay',dest='thread_delay',default=-1,type=float,
                            help='If an argument is given, then this variable indicates how many '\
                            'days should pass before a thread is rescraped; if not provided, the '\
                            'threads will never be updated beyond the first scrape; this value '\
                            'also controls the delay for comments to be overwritten if history is'\
                            ' disabled (default)')
        parser.add_argument('-ud','--user-delay',dest='user_delay',default=-1,type=float,
                            help='If an argument is given, then this variable indicates how many '\
                            'days should pass before a user is rescraped; if not provided, the '\
                            'user will never be updated beyond the first scrape')
        parser.add_argument('-sd','--subreddit-delay',dest='subreddit_delay',default=-1,type=float,
                            help='If an argument is given, then this variable indicates how many '\
                            'days should pass before a subreddit is rescraped; if not provided, '\
                            'the subreddit will never be updated beyond the first scrape')
        
        parser.add_argument('--post-refresh-time',dest='post_refresh_time',type=float,
                            help='Time period in days after which post lists for subreddits are'\
                            ' forced to refresh. By default, this appends new posts to the old.')
        parser.add_argument('--drop-old-posts',dest='drop_old_posts',action='store_true',
                            help="If selected along with --post-refresh-time, then the "\
                            ' lists of posts for each subreddit will reset instead of being '\
                            'appended.')
        parser.add_argument('-du','--deepuser', action='store_true',dest='deepuser',
                            help="This option will add threads to the thread queue when scraping "\
                            'a user\'s history based on the posts they commented on/submitted. '\
                            'This is generally not a good idea, as a few users can take a very '\
                            'long time to completely scrape')
        parser.add_argument('-ucl','--user-comment-limit',dest='user_comment_limit',type=int,
                            default=100,
                            help='The number of comments in a user\'s history that will be'\
                            ' scraped at most.')
        parser.add_argument('-utl','--user-thread-limit',dest='user_thread_limit',type=int,
                            default=100,
                            help='The number of threads in a user\'s history that will be'\
                            ' scraped at most.')
        parser.add_argument('-l','--limit',dest='limit',default=100,type=int,
                            help='The number of threads in a subreddit that the parser will '\
                            'attempt to search')
        parser.add_argument('-t','--type',dest='rank_type',default='new',
                            choices=['top','hot','new','controversial','rising'],
                            help="the type of ranking used when querying from subreddits")
        parser.add_argument('-c','--constants',
                            help="a filename for a python file containing a dictionary named "\
                            '"kwargs". This dictionary will be used as an intializer for the '\
                            'arguments given via command line (and will be overwritten by any '\
                            'additions. Currently experimental.')
        parser.add_argument('--nouser',action='store_true',dest='nouser',
                            help='User information is not scraped if enabled')
        parser.add_argument('--grab-authors',action='store_true',dest='grabauthors',
                            help='Authors are scraped in addition to users of threads')
        
        parser.add_argument('--rescrape-threads',action='store_true',dest='rescrape_threads',
                            help='Rescrape all posts that have been collected')
        parser.add_argument('--rescrape-users',action='store_true',dest='rescrape_users',
                            help='Rescrape all users that have been collected')
        parser.add_argument('-n',default=-1,dest='N',type=int,
                            help="The number of posts that the scraper will collect. If <0, then"\
                            " posts will be collected until the script is halted")
        parser.add_argument('--skip-comments',action='store_true',dest='skip_comments',
                            help='Skips collecting comments and only stores general user and '\
                            'thread data')
        parser.add_argument('--hard-reset-and-quit',action='store_true',dest='hard_reset_quit',
                            help="Delete the schema and all data and exit")
        parser.add_argument('--hard-reset-and-continue', action='store_true',
                            dest='hard_reset_continue',
                            help="Reset the schema and delete all data and begin scraping (again).")
        parser.add_argument('--ratio',action='store_true',dest='get_upvote_ratio',
                            help='Get the calculated upvote ratio for threads. This is a separate'\
                            ' API call, so it significantly slows down the rate at which data can'\
                            ' be gathered')
        parser.add_argument('--mincomments', dest='mincomments', default=None,type=int,
                            help="Set minimum number of comments for a thread to be collected via"\
                            " subreddit searching.")
        parser.add_argument('--full-rescraping', dest='full_rescraping',action='store_true',
                            help="If used, then rescraping via --rescrape_users or "\
                            "--rescrape_threads will include all posts/comments, not just ones "\
                            "that were originally encountered froms scraping through threads. "\
                            "This is the easiest way to get random posts that go far back in the "\
                            "history of a subreddit.")
        parser.add_argument('--avoid-full-threads', dest='avoid_full_threads',action='store_true',
                            help="If used, then rescraping via --rescrape_threads will not include"\
                            " threads that had a full comment-history scrape. It will only include"\
                            " threads encountered in comment and thread histories of user's "\
                            "profiles.")
        parser.add_argument('--scrape-subreddits',action='store_true',dest='scrape_subreddits',
                            help="Subreddits will be scraped for their information, using "\
                            " the subreddits list as a source.")
        parser.add_argument('--rescrape-subreddits',action='store_true',dest='rescrape_subreddits',\
                            help="Will rescrape any subreddits previously scraped in subreddits "\
                            "table or the moderators table.")
        parser.add_argument('--scrape-related-subreddits',action='store_true',
                            dest='scrape_related_subreddits',
                            help="Will scrape the sidebar (and wiki if --scrape-wikis is enabled) "\
                            "for mentions of other subreddits.")
        parser.add_argument('--scrape-wikis',action='store_true',dest='scrape_wikis',
                            help="Will scrape wikis of a subreddit (if they exist). If combined "\
                            "with --scrape-related-subreddits, subreddit names will also be "\
                            "extracted and validated")
        parser.add_argument('--related-subreddit-recursion-depth',type=int,default=0,
                            dest='related_subreddit_recursion_depth',
                            help="If given, then any subreddits encountered (and validated) "\
                            "during related subreddits scraping will be also scraped, provided "\
                            "they are at most this many degrees away from one of the root "\
                            "subreddits scraped.")
        parser.add_argument('--scrape-traffic',action='store_true',dest='scrape_traffic',
                            help="Will scrape traffic data when subreddits are being scraped. "\
                            "This only works for subreddits that have public traffic, and it "\
                            "takes an extra API call to do this.")
        parser.add_argument('--scrape-subreddits-in-db',action='store_true',
                            dest='scrape_subreddits_in_db',
                            help='Will only scrape subreddits that appear in threads or comments '\
                            'tables of database (and schema)')
        parser.add_argument('--min-occurrences-for-subreddit-in-db',type=int,default=1,
                            dest='min_occurrences_for_subreddit_in_db',
                            help="The minimum number of times a subreddit should appear in "\
                            "the database & schema to be selected via --scrape-subreddits-in-db.")
        parser.add_argument('--repeat-subreddit-scraping',action='store_true',
                            dest='repeat_subreddit_scraping',
                            help="Will not avoid already-scraped subreddits if specified to loop "\
                            "many times")
        parser.add_argument('--rescrape-with-comment-histories',action='store_true',
                            dest='rescrape_with_comment_histories',
                            help="Uses comment histories to gather thread IDs. Comment timestamp "\
                            "is used for age-based validation. Other forms of validation are "\
                            "either implied or ignored due to the nature of this argument.")
        parser.add_argument('--moderators',dest='moderators',action='store_true',
                            help="If selected, the moderators of subreddits will be gathered in "\
                            "the .moderators table. Additional options in --moderators-all and "\
                            "--repeat-moderator-subreddits.")
        parser.add_argument('--moderators-all',dest='moderators_all',action='store_true',
                            help="Will use all subreddits gathered in database instead of just "\
                            "those in the subreddits list. Prioritizes those in subreddit "\
                            "list, though.")
        parser.add_argument('--scrape-moderators',dest='scrape_moderators',action='store_true',
                            help='Will scrape moderators comment and thread history if '\
                            '--moderators is selected.')
        parser.add_argument('--repeat-moderator-subreddits',dest="repeat_moderator_subreddits",
                            action='store_true',
                            help="Will regather moderator info for subreddits in case they have "\
                            "already been gathered (instead of skipping them).")
        parser.add_argument('--use-subreddit-table-for-moderators',action='store_true',
                            dest='use_subreddit_table_for_moderators',
                            help='Will use the subreddits table to choose subreddits for moderator'\
                            ' scraping.')
        parser.add_argument('--timer',dest='timer',type=float,
                            help="After approx. this amount of time, in hours, the program will "\
                            "stop.")
        parser.add_argument('--verbose','-v',dest='verbose',action='store_true',
                            help="Enabling this will increase the text output during scraping.")

        parser.add_argument('--reappend-invalid-posts',dest='reappend',action='store_true',
                            help="Instead of discarding a post from the queue for a subreddit, "\
                            "sends the post to the back of the queue when it is deemed invalid.")
        parser.add_argument('--n-unscraped-users-to-scrape',dest='n_unscraped_users_to_scrape',
                            type=int, default=0, help="If specified, will add up to [argument]"\
                            " number of users who appear in comments and threads but not user "\
                            "history. Good for when previous scraping skips user collection.")
        print('added arguments')
        parser.add_argument('--max-comments',dest='max_comments',
                            type=int, default=1e6, help="Will only scrape threads if number of "
                            "comments is less than or equal to this value.")

        parser.add_argument('--subreddit-dict-refresh-min-period',
                            dest='subreddit_dict_refresh_min_period',
                            type=int,
                            default=300,
                            help='Minimum time between refreshes of a subreddit\'s dictionary'
        )
        parser.add_argument('--shuffle-subreddits', dest='shuffle_subreddits',
                            action='store_true', help='Will shuffle order or loaded subreddits. Good if you do not want the same order each tiem the code is run (can save on memory leaks if you put it in a shell script).')
        parser.add_argument('--shuffle-usernames', dest='shuffle_usernames',
                            action='store_true', help='Shuffles usernames after loading')
        parser.add_argument('--user-gildings', dest='user_gildings', action='store_true',
                            help='Will scrape user history for gildings (silver, gold, platinum) if toggled; may take extra time')
        parser.add_argument('--scrape-gilded', dest='scrape_gilded',
                            action='store_true', help='Scrapes gilded comment & thread data from gilded posts collected via --user-gildings.')
        parser.add_argument('--max-wiki-size', default=120, help='Maximum size of wiki to scrape. If a wiki has more than this many pages, it will be skipped')

        parser.add_argument('--alt-scraper-file', required=False, nargs=2, help='If specified, this is a path to a python file followed by a function name that can be used instead of the default praw_user and scraper functions. Useful for extending scripts.', default=None
        )

        parser.add_argument(
            '--new-user-scrape-mode', action='store_true',
            help='Collect new user names and then scrape them after exhausting ID list'
        )

        parser.add_argument(
            '--resample-users-from-schema',
            help='Resamples users from given schema',
            default=None,
            required=False
        )

        parser.add_argument(
            '--resampling-age-range',
            help='Age range in days for resampled data',
            default=[0, 3e4],
            type=int,
            nargs=2
        )

        parser.add_argument(
            '--terminate-after-sub-dict-unusable',
            action='store_true',
            help='Stop script if cannot get any more entries from a subreddit'
        )

        parser.add_argument(
            '--awards',
            action='store_true',
            help='Record user/comment/submission awardings'
        )

        parser.add_argument(
            '--trophies',
            action='store_true',
            help='Record trophies of users'
        )

        parser.add_argument(
            '--token',
            required=False,
            default=None,
            help='2FA token'
        )

        parser.add_argument(
            '--count-duplicates',
            action='store_true',
            help='Count duplicates in threads (takes extra time)'
        )

        parser.add_argument(
            '--filter-existing-threads',
            action='store_true',
            help='Filters out thread IDs if they already exist in the data'
        )

        parser.add_argument(
            '--filter-existing-comments',
            action='store_true',
            help='Filters out thread IDs if they already exist in the data'
        )        

        parser.add_argument(
            '--thread-batch-mode',
            action='store_true',
            help='If not collecting extra comment/author info, expedites thread scraping significantly when IDs supplied.'
        )

        parser.add_argument(
            '--comment-batch-mode',
            action='store_true',
            help='If not collecting extra author info, expedites comment scraping significantly when IDs supplied.'
        )        
            
                            
        args = parser.parse_args()
        print('parsed arguments')
        #load template if exists
        self.args = args
        good_args = [a for a in dir(args) if not re.match('^_.*',a)]
        #DELETE
        if args.verbose:
            for arg in good_args:
                print(arg, getattr(args, arg), type(getattr(args, arg)))
        if args.constants is not None:
            constants = __import__(args.constants)
            for constant in constants.kwargs:
                setattr(self, constant, constants.kwargs[constant])
        #process all other values
        self.name = args.name
        if not hasattr(self, 'pattern'):
            if args.pattern[0] == 'D':
                self.pattern = [int(p) for p in args.pattern[1:]]
            else:
                self.pattern = [int(p) for p in args.pattern]

        #add to subreddit list
        if args.subreddits is not None:
            self.subreddits=args.subreddits
        elif not hasattr(self, 'subreddits'):
            self.subreddits = []
        if args.f_subreddits is not None:
            for fn in args.f_subreddits:
                self.subreddits += extract_lines(fn)
        elif hasattr(self, 'f_subreddits'):
            for fn in self.f_subreddits:
                self.subreddits += extract_lines(fn)
        #add to thread list
        if args.thread_ids is not None:
            self.ids = args.thread_ids
        elif not hasattr(self,'ids'):
            self.ids=[]
        if args.f_thread_ids is not None:
            for fn in args.f_thread_ids:
                self.ids += extract_lines(fn)
        elif hasattr(self, 'f_ids'):
            for fn in self.f_ids:
                self.ids += extract_lines(fn)

        # add to comment id list
        if args.comment_ids is not None:
            self.comment_ids = args.comment_ids
        elif not hasattr(self, 'comment_ids'):
            self.comment_ids = []

        if args.f_comment_ids is not None:
            for fn in args.f_comment_ids:
                self.comment_ids += extract_lines(fn)
                
        #add to user list
        if args.users is not None:
            self.users = args.users
        elif not hasattr(self,'users'):
            self.users = []
        if args.f_users is not None:
            for fn in args.f_users:
                self.users += extract_lines(fn)
        elif hasattr(self, 'f_users'):
            for fn in self.f_users:
                self.users += extract_lines(fn)

        # workaround in case you need to pass a name that starts with a hyphen via somethign like ' --someusername--xx--xx'
        self.users = [x.strip() for x in self.users]
        #unordered option
        self.unordered = handle_boolean(self, args, 'unordered')
        #history
        if not hasattr(self,'history'):
            self.history = args.history
        #simple arguments
        # yes, I know this is a bad way of doing it; I'll probably get around to refactoring it at some point.

        self.impose('age')
        self.impose('token')
        self.impose('resample_users_from_schema')
        self.impose('resampling_age_range')
        self.impose('mincomments')
        self.impose('max_comments')
        self.impose('history')
        self.impose('post_refresh_time')
        self.impose('thread_delay')
        self.impose('user_delay')
        self.impose('subreddit_delay')
        self.impose('user_comment_limit')
        self.impose('user_thread_limit')
        self.impose('man_user_comment_limit')
        self.impose('man_user_thread_limit')        
        self.impose('limit')
        self.impose('skip_comments')
        self.impose('rank_type')
        self.impose('timer')
        self.impose('min_occurrences_for_subreddit_in_db')
        self.impose('related_subreddit_recursion_depth')
        self.impose('reappend')
        self.impose('n_unscraped_users_to_scrape')
        self.impose('subreddit_dict_refresh_min_period')
        self.impose('shuffle_subreddits')
        self.impose('shuffle_usernames')
        self.impose('user_gildings')
        self.impose('scrape_gilded')
        self.impose('max_wiki_size')
        self.impose('alt_scraper_file')
        self.impose('new_user_scrape_mode')
        self.impose('terminate_after_sub_dict_unusable')
        self.rank_type = self.rank_type
        for elem in ['nouser','grabauthors','rescrape_threads','rescrape_users',
                     'get_upvote_ratio','deepuser','log', 'drop_old_posts',
                     'full_rescraping','avoid_full_threads',
                     'rescrape_with_comment_histories','verbose',
                     'moderators','moderators_all','repeat_moderator_subreddits',
                     'scrape_moderators','scrape_subreddits','scrape_subreddits_in_db',
                     'repeat_subreddit_scraping','use_subreddit_table_for_moderators',
                     'rescrape_subreddits','scrape_related_subreddits','scrape_wikis',
                     'scrape_traffic', 'user_gildings', 'scrape_gilded','awards'
                     ,'trophies','count_duplicates','filter_existing_threads','thread_batch_mode',
                     'comment_batch_mode','filter_existing_comments',]:
            setattr(self, elem, handle_boolean(self, args, elem))
        self.impose('N')
        self.dictionary_time = datetime.datetime.utcnow()
        self.SCRAPE_ANY_SUBREDDITS = self.scrape_subreddits_in_db or self.scrape_subreddits \
                                     or self.rescrape_subreddits
        #this is used to avoid navigating through the same subreddit twice if rescraping
        self.RELATED_SUBREDDIT_SET = set([x.lower() for x in self.subreddits])
        #intialize database...
        #check if reset options have been triggered
                
        #process history mode...
        if self.history is None:
            self.history = []
        self.histories = {txt:txt not in self.history for txt in ['threads','comments','users']}
        self.db = db.Database(self.name, self.histories, check_awards = True, check_trophies = True)

        # sort of auto-migration
        # postgres 10 has IF EXISTS and IF NOT EXISTS for ALTER TABLE, but I'm assuming not everyone uses
        # that version
        if True:
            self.db.add_utc_columns_to_existing_tables(verbose=args.verbose)
            self.db.add_gilded_columns_to_existing_tables(verbose=args.verbose)

        if self.n_unscraped_users_to_scrape > 0:
            self.users.extend(get_unscraped_ids(self.db, self.n_unscraped_users_to_scrape))
        if args.hard_reset_quit:
            var = input("ARE YOU SURE YOU WANT TO DELETE %s DATA? " % self.name)
            if var.lower() in ['y','yes']:
                self.db.dropall()
                print('deleted database')
                sys.exit()
            else:
                print('did not delete database')
                sys.exit()
        if args.hard_reset_continue:
            var = input("ARE YOU SURE YOU WANT TO DELETE %s DATA? " % self.name)
            if var.lower() in ['y','yes']:
                self.db.dropall()
                print('deleted database')
                del self.db
                self.db = db.Database(self.name, self.histories)
                print('created new database')
            else:
                print('did not delete database')
                sys.exit()
        if self.log:
            global logging
            global logopts
            logging = True
            logopts = self
            self.db.add_log_entry(self)
        if self.timer:
            self.init_time = datetime.datetime.now()
        #if rescraping is enabled, subreddit scraping will be either
        #of limited N or suppressed (default)
        if self.rescrape_users or self.rescrape_threads:
            self.N = max(0, self.N)

        # used to keep track of how frequently refreshes are made
        self.subreddit_refresh_timer_dict = {}

        self.scraped_subreddits = set()

        if self.alt_scraper_file is not None:
            print('ALT SCRAPER FILE')
            print(self.alt_scraper_file)
            global scraper

            module = importlib.import_module(self.alt_scraper_file[0])

            scraper = getattr(module, self.alt_scraper_file[1])
            print('Loaded %s function as %s to act as scraper' % tuple(self.alt_scraper_file[::-1]))

        if self.filter_existing_threads:
            existing_thread_ids = self.db.get_all_thread_ids()
            n_existing_thread_ids = len(existing_thread_ids)

            n_original_thread_ids = len(self.ids)
            self.ids = [x for x in self.ids if x not in existing_thread_ids]
            n_filtered_thread_ids = len(self.ids)
            sorted_existing_ids = sorted(list(existing_thread_ids))
            print('THREAD ID FILTERING %s/%s kept, %s already in db' % (
                n_filtered_thread_ids,
                n_original_thread_ids,
                n_existing_thread_ids,
            ))

        if self.filter_existing_comments:
            existing_comment_ids = self.db.get_all_comment_ids()
            n_existing_comment_ids = len(existing_comment_ids)

            n_original_comment_ids = len(self.comment_ids)
            self.comment_ids = [x for x in self.comment_ids if x not in existing_comment_ids]
            n_filtered_comment_ids = len(self.comment_ids)
            sorted_existing_ids = sorted(list(existing_comment_ids))
            print('COMMENT ID FILTERING %s/%s kept, %s already in db' % (
                n_filtered_comment_ids,
                n_original_comment_ids,
                n_existing_comment_ids,
            ))            

        print('intialized database')
    
    def impose(self, varname, validator=None,popfirst=False):
        val = getattr(self.args,varname)
        if val is not None and val is not []:
            if validator is not None:
                assert validator(val)
            setattr(self, varname,val)
        else:
            setattr(self, varname, None)
        return 0
        
def handle_boolean(obj, args, varname):
    """optional variables are only activated by command-line arguments; they cannot be disabled
    if they are activated in the constants file"""
    if  hasattr(obj,varname):
        return getattr(obj,varname) or getattr(args,varname)
    else:
        return getattr(args,varname)

def extract_lines(filename, limit=30):
    """reads lines of text file and returns the content of each line as an element of a list"""
    file=open(filename,'r')

    data = [x.strip() for x in file.readlines() if x.strip() != '' and len(x) <= limit]
    file.close()
    return data
            


if __name__=='__main__':
    main(sys.argv[1:])
