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
from prawcore.exceptions import NotFound

import rescraping

from navigator import Navigator

import praw_user

from user_scrape import scrape_user
from thread_process import process_thread
from moderator_scrape import scrape_moderators
from subreddit_scrape import scrape_subreddits

from praw_object_data import retry_if_broken_connection, get_comment_data
import writer
from get_unscraped_ids import get_unscraped_ids

from multiprocessing import Process, Queue

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

@clean_keyboardinterrupt
def main():
    opts = options()
    counter = 0
    scraper = praw_user.scraper()
    print 'starting collection...'
    while opts.N < 0 or counter < opts.N:
        if counter % 5 == 0:
            sys.stdout.write('.')
            sys.stdout.flush()
        process_subreddit(opts.subreddits[counter % len(opts.subreddits)], opts, scraper)
        #p = Process(target=process_comments, args=(comments, opts))
        counter += 1
        if counter % 100 == 0:
            print 'total comments processed: %s' % len(opts.COMMENT_ID_SET)
    print 'done'
@retry_if_broken_connection
def process_subreddit(subreddit_text, opts, scraper):
    subreddit = scraper.subreddit(subreddit_text)
    comments = subreddit.comments(limit=100)
    process_comments(comments, opts)

@retry_if_broken_connection
def process_comments(comments, opts):
    data_dict = {}
    for comment in comments:
        if comment.id in opts.COMMENT_ID_SET:
            continue
        else:
            opts.COMMENT_ID_SET.add(comment.id)
        data = get_comment_data(comment, opts)
        data_dict.update(data)
    for key, value in data_dict.iteritems():
        writer.write_comment(value, opts)
    return True

class options(object):
    """this class processes command-line input to create overall scraper options"""
    
    def __init__(self, *args, **kwargs):
        #initialize global accumulators
        self.deepcounter = 0
        #logging variables
        self.original_command =' '.join(sys.argv)
        self.start_time = datetime.datetime.now(pytz.utc)
        self.COMMENT_ID_SET = set()
        #create parser and add arguments
        parser = ArgumentParser(description='scrape data from Reddit')
        parser.add_argument('name',
                            help="the schema name for the tables to be stored in",
                            default='default')
        parser.add_argument('-s','--subreddits', dest='subreddits', nargs='+',
                            help='a list of subreddits that will be rotated through for scraping')
        parser.add_argument('-fs','--f-subreddits', dest='f_subreddits', nargs='+',
                            help='a list of filenames with line-separated subreddit names to be '\
                            ' added to a list of subreddits that will be scraped on a rotation')
        parser.add_argument('--n-clusters',dest='n_clusters',type=int,
                            help='How many clusters to use for asynchronous comment insertion.'\
                            ' Currently unused.')
        parser.add_argument('--log', dest='log',action='store_true',
                            help='Stores command, stop/start times, and start/stop causes to a log'\
                            ' in the database (under schema.log)')
        
        parser.add_argument('--history',dest='history', nargs='*',choices=['threads','users',
                                                                           'comments',
                                                                           'subreddits'],
                            help="Used to initialize database; this option accepts up to three arguments, indicating which "\
                            'tables should have new rows made instead of row updates for when '\
                            'data is scraped. This should be disabled unless you need historical '\
                            'data, since it can affect performance for large databases and make '\
                            'queries more complicated.')
        parser.add_argument('-n',default=-1,dest='N',type=int,
                            help="The number of comment cycles (100 at a time) that the scraper will collect. If <0, then"\
                            " posts will be collected until the script is halted")
        parser.add_argument('--timer',dest='timer',type=float,
                            help="After approx. this amount of time, in hours, the program will "\
                            "stop.")
        parser.add_argument('--verbose','-v',dest='verbose',action='store_true',
                            help="Enabling this will increase the text output during scraping.")

        print 'added arguments'
        args = parser.parse_args()
        print 'parsed arguments'
        #load template if exists
        self.args = args
        good_args = [a for a in dir(args) if not re.match('^_.*',a)]
        #DELETE
        if args.verbose:
            for arg in good_args:
                print arg, getattr(args, arg), type(getattr(args, arg))
        #process all other values
        self.name = args.name
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
        if not hasattr(self,'history'):
            self.history = args.history
        #simple arguments
        self.impose('history')
        self.impose('timer')
        self.impose('N')
        self.impose('n_clusters')
        for elem in ['verbose','log']:
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
    main()
