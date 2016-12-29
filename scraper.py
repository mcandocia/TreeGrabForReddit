import praw
import re
import os
import time
import requests
from requests.exceptions import HTTPError, ConnectionError
from exceptions import UnicodeDecodeError, IndexError, AttributeError
import calendar
import datetime
import sys
import random
import socket
from socket import AF_INET, SOCK_DGRAM
from argparse import ArgumentParser

from navigator import Navigator

from praw_user import scraper

from user_scrape import scrape_user

from praw_object_data import retry_if_broken_connection

import pytz

import db

#used for keyboard interrupt logging
logging = False
logopts = None

def clean_keyboardinterrupt(f):
    def func(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except KeyboardInterrupt:
            print 'exiting via keyboard interrupt'
            if logging:
                print 'making log'
                logopts.db.update_log_entry(logopts, 'Keyboard Interrupt')
            sys.exit()
    return func


def get_age(timestamp, localize=True):
    if localize:
        timestamp = pytz.utc.localize(timestamp)
        now = datetime.datetime.now(pytz.utc)
    else:
        now = datetime.datetime.now()
    difference = (now - timestamp).seconds
    days = float(difference) / 3600. / 24.
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
        thread_time = datetime.datetime.fromtimestamp(post.created_utc)
        if thread_time is not None:
            thread_age = get_age(thread_time)
            if thread_age < opts.age[0] or thread_age > opts.age[1]:
                return False
            
    #check if already in database, then check if able to be rescraped
    update_time = opts.db.get_thread_update_time(post.id)
    if update_time is None:
        return True
    else:
        if opts.thread_delay == -1:
            return False
        age = get_age(update_time)
        print 'age: %d' % age
        if age > opts.thread_delay:
            print 'UPDATING THREAD'
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
        else:
            valid_posts.append(post)
    if not refreshed:
        print 'refreshing dictionary for %s' % subreddit_name
        post_dict[subreddit_name] = get_subreddit_posts(reddit_scraper.subreddit(subreddit_name),
                                                        opts)
        return select_post(subreddit_name, post_dict, opts, reddit_scraper, refreshed=True)
    else:
        print 'cannot find valid entries for %s' % subreddit_name
        return None

@retry_if_broken_connection
def get_subreddit(subreddit_name, scraper):
    return scraper.subreddit(subreddit_name)

@retry_if_broken_connection
def process_thread(thread_id, opts, reddit_scraper):
        thread = reddit_scraper.submission(id=thread_id)
        print thread_id#keep until certain bug issue is gone
        start = datetime.datetime.now()
        print '+------------------------------------------------------+'
        print 'PROCESSING %s, id=%s, in /r/%s' % (thread.title, thread.id,
                                                  thread.subreddit.display_name)
        print 'created %s' % datetime.datetime.fromtimestamp(thread.created).strftime('%x %X')
        print 'author: %s' % str(thread.author)
        print 'score: %d, num_comments: %d' % (thread.score, thread.num_comments)
        print ''
        nav = Navigator(thread, opts)
        if opts.skip_comments:
            nav.store_thread_data()
        else:
            nav.navigate()
        end = datetime.datetime.now()
        print 'FINISHED thread w/id=%s, navigated %d comments, %d deleted'\
            % (thread.id, nav.traversed_comments, nav.deleted_comments)
        print 'thread scraping time: %d seconds' % (end-start).seconds
        print '+------------------------------------------------------+'


@clean_keyboardinterrupt
def main(args):
    opts = options()
    reddit_scraper = scraper()
    #parse users
    if len(opts.users) > 0:
        opts.old_user_comment_limit = opts.user_comment_limit
        opts.old_user_thread_limit = opts.user_thread_limit
        if opts.man_user_comment_limit <> None:
            opts.user_comment_limit = opts.man_user_comment_limit
        if opts.man_user_thread_limit <> None:
            opts.user_thread_limit = opts.man_user_thread_limit
    for username in opts.users:
        scrape_user(username, opts, reddit_scraper, force_read=opts.deepuser)
    if len(opts.users) > 0 and opts.deepuser:
        random.shuffle(opts.ids)
    if len(opts.users) > 0:
        opts.user_comment_limit = opts.old_user_comment_limit
        opts.user_thread_limit = opts.old_user_thread_limit
    #parse thread ids
    print 'pattern:', opts.pattern
    for thread_id in opts.ids:
        process_thread(thread_id, opts, reddit_scraper)
    if len(opts.ids) > 0:
        print 'finished with supplied thread ids'
    else:
        print '---------------------------------'
    #go through subreddits
    counter = 0
    n_subreddits = len(opts.subreddits)
    subreddit_post_dict = {}
    old_subreddit_post_dict = {}
    while opts.N == -1 or counter < opts.N and n_subreddits > 0:
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
            print 'skipping %s due to insufficient posts' % subreddit_name
        else:
            process_thread(thread.id, opts, reddit_scraper)
        if len(subreddit_post_dict[subreddit_name]) == 0:
            subreddit_post_dict.pop(subreddit_name)
        counter += 1
        print 'finished with %d threads' % counter
        #check to see if dict should be refreshed
        if opts.post_refresh_time:
            if opts.post_refresh_time > get_age(opts.dictionary_time, localize=False):
                opts.dictionary_time = datetime.datetime.now()
                if not opts.drop_old_posts:
                    old_subreddit_post_dict = subreddit_post_dict
                subreddit_post_dict = {}
    print 'done'
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
        parser.add_argument('-p','--pattern',dest='pattern',nargs='+',default=['D',50,10,5,5,4,2,1],
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
        parser.add_argument('-u','--users', dest='users', nargs='+',
                            help='A list of users to search through')
        parser.add_argument('--man-user-comment-limit',dest='man_user_comment_limit',type=int,
                            help="If user IDs are supplied, this overrides --user-comment-limit"\
                            " for those IDs.")
        parser.add_argument('--man-user-thread-limit', dest='man_user_thread_limit',type=int,
                            help="If user IDs are supplied, this overrides --user-thread-limit"\
                            " for those IDs.")
        parser.add_argument('-fu','--f-users', dest='f_users',nargs='+')
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
                                                                           'comments'],
                            help="this option accepts up to three arguments, indicating which "\
                            'tables should have new rows made instead of row updates for when '\
                            'data is scraped. This should be disabled unless you need historical '\
                            'data, since it can affect performance for large databases and make '\
                            'queries more complicated.')
        parser.add_argument('-td', '--thread_delay',dest='thread_delay',default=-1,nargs=1,type=int,
                            help='If an argument is given, then this variable indicates how many '\
                            'days should pass before a thread is rescraped; if not provided, the '\
                            'threads will never be updated beyond the first scrape; this value '\
                            'also controls the delay for comments to be overwritten if history is'\
                            ' disabled (default)')
        parser.add_argument('-ud','--user-delay',dest='user_delay',default=-1,nargs=1,type=int,
                            help='If an argument is given, then this variable indicates how many '\
                            'days should pass before a user is rescraped; if not provided, the '\
                            'user will never be updated beyond the first scrape')
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
                            default=100,nargs=1,
                            help='The number of comments in a user\'s history that will be'\
                            ' scraped at most.')
        parser.add_argument('-utl','--user-thread-limit',dest='user_thread_limit',type=int,
                            default=100,nargs=1,
                            help='The number of threads in a user\'s history that will be'\
                            ' scraped at most.')
        parser.add_argument('-l','--limit',dest='limit',default=100,nargs=1,type=int,
                            help='The number of threads in a subreddit that the parser will '\
                            'attempt to search')
        parser.add_argument('-t','--type',dest='rank_type',default='new',nargs=1,
                            choices=['top','hot','new','controversial','rising'],
                            help="the type of ranking used when querying from subreddits")
        parser.add_argument('-c','--constants',nargs=1,
                            help="a filename for a python file containing a dictionary named "\
                            '"kwargs". This dictionary will be used as an intializer for the '\
                            'arguments given via command line (and will be overwritten by any '\
                            'additions')
        parser.add_argument('--nouser',action='store_true',dest='nouser',
                            help='User information is not scraped if enabled')
        parser.add_argument('--grab_authors',action='store_true',dest='grabauthors',
                            help='Authors are scraped in addition to users of threads')
        parser.add_argument('--rescrape_posts',action='store_true',dest='rescrape_posts',
                            help='Rescrape all posts that have been collected')
        parser.add_argument('--rescrape_users',action='store_true',dest='rescrape_users',
                            help='Rescrape all users that have been collected')
        parser.add_argument('-n',nargs=1,default=-1,dest='N',
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
        print 'added arguments'
        args = parser.parse_args()
        print 'parsed arguments'
        #load template if exists
        self.args = args
        good_args = [a for a in dir(args) if not re.match('^_.*',a)]
        #DELETE
        for arg in good_args:
            print arg, getattr(args, arg), type(getattr(args, arg))
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
        #unordered option
        self.unordered = handle_boolean(self, args, 'unordered')
        #history
        if not hasattr(self,'history'):
            self.history = args.history
        #simple arguments
        self.impose('age')
        self.impose('mincomments')
        self.impose('history')
        self.impose('post_refresh_time')
        self.impose('thread_delay')
        self.impose('user_delay')
        self.impose('user_comment_limit')
        self.impose('user_thread_limit')
        self.impose('man_user_comment_limit')
        self.impose('man_user_thread_limit')        
        self.impose('limit')
        self.impose('skip_comments')
        self.impose('rank_type')
        self.rank_type = self.rank_type[0]
        for elem in ['nouser','grabauthors','rescrape_posts','rescrape_users',
                     'get_upvote_ratio','deepuser','log', 'drop_old_posts']:
            setattr(self, elem, handle_boolean(self, args, elem))
        self.impose('N')
        self.dictionary_time = datetime.datetime.now()

        #intialize database...
        #check if reset options have been triggered
                
        #process history mode...
        if self.history is None:
            self.history = []
        self.histories = {txt:txt not in self.history for txt in ['threads','comments','users']}
        self.db = db.Database(self.name, self.histories)
        if args.hard_reset_quit:
            var = raw_input("ARE YOU SURE YOU WANT TO DELETE %s DATA? " % self.name)
            if var.lower() in ['y','yes']:
                self.db.dropall()
                print 'deleted database'
                sys.exit()
            else:
                print 'did not delete database'
                sys.exit()
        if args.hard_reset_continue:
            var = raw_input("ARE YOU SURE YOU WANT TO DELETE %s DATA? " % self.name)
            if var.lower() in ['y','yes']:
                self.db.dropall()
                print 'deleted database'
                del self.db
                self.db = db.Database(self.name, self.histories)
                print 'created new database'
            else:
                print 'did not delete database'
                sys.exit()
        if self.log:
            global logging
            global logopts
            logging = True
            logopts = self
            self.db.add_log_entry(self)

        print 'intialized database'
    
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

def extract_lines(filename):
    """reads lines of text file and returns the content of each line as an element of a list"""
    file=open(filename,'r')
    objs=[]
    while True:
        nextobj=re.sub('[\n\r]','',file.readline())
        if nextobj=='':
            break
        else:
            objs+=[nextobj]
    return(objs)
            


if __name__=='__main__':
    main(sys.argv[1:])
