import praw
import re
import os
import time
import requests
from requests.exceptions import HTTPError, ConnectionError
from exceptions import UnicodeDecodeError, IndexError, AttributeError
import calendar
import sys
import random
import socket
from socket import AF_INET, SOCK_DGRAM
from argparse import ArgumentParser

from navigator import Navigator

from praw_user import scraper

import db

def main(args):
    opts = options()
    reddit_scraper = scraper()
    #parse thread ids
    print 'pattern:', opts.pattern
    for thread_id in opts.ids:
        thread = reddit_scraper.submission(id=thread_id)
        print 'PROCESSING %s, id=%s, in /r/%s' % (thread.title, thread.id,
                                                  thread.subreddit.display_name)
        nav = Navigator(thread, opts)
        if opts.skip_comments:
            nav.store_thread_data()
        else:
            nav.navigate()
        print 'FINISHED thread w/id=%s' % thread.id
        
    print 'done'
    return 0

class options(object):
    """this class processes command-line input to create overall scraper options"""
    def __init__(self, *args, **kwargs):
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
        parser.add_argument('-fu','--f-users', dest='f_users',nargs='+')
        parser.add_argument('--f-ids',dest='f_thread_ids', nargs='+',
                            help='a list of filenames with line-separated thread IDs to be scraped'\
                            ' sequentially')
        parser.add_argument('-a','--age',dest='age', nargs=2,type=int,
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
        parser.add_argument('-td', '--thread_delay',dest='thread_delay',default=-1,nargs=1,
                            help='If an argument is given, then this variable indicates how many '\
                            'days should pass before a thread is rescraped; if not provided, the '\
                            'threads will never be updated beyond the first scrape; this value '\
                            'also controls the delay for comments to be overwritten if history is'\
                            ' disabled (default)')
        parser.add_argument('-ud','--user-delay',dest='user_delay',default=-1,nargs=1,type=int,
                            help='If an argument is given, then this variable indicates how many '\
                            'days should pass before a user is rescraped; if not provided, the '\
                            'user will never be updated beyond the first scrape')
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
        self.impose('history')
        self.impose('thread_delay')
        self.impose('user_delay')
        self.impose('user_comment_limit')
        self.impose('user_thread_limit')
        self.impose('limit')
        self.impose('skip_comments')
        self.impose('rank_type')
        for elem in ['nouser','grabauthors','rescrape_posts','rescrape_users',
                     'get_upvote_ratio','deepuser']:
            setattr(self, elem, handle_boolean(self, args, elem))
        self.impose('N')

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

        print 'intialized database'
    
    def impose(self, varname, validator=None,popfirst=False):
        val = getattr(self.args,varname)
        if val is not None and val is not []:
            if validator is not None:
                assert validator(val)
            setattr(self, varname,val)
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
