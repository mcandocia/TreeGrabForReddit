import praw_user
import sys
import os
from argparse import ArgumentParser
import datetime
import re
from prawcore.exceptions import NotFound
from prawcore.exceptions import RequestException
from prawcore.exceptions import Forbidden
from prawcore.exceptions import Redirect
from prawcore.exceptions import BadRequest

from praw_object_data import retry_if_broken_connection

from scraper import extract_lines
from scraper import handle_boolean
#for recursion
opts=None

def search_for_subreddits(text):
    return re.findall(r'/r/([\w\-]+)(?=([^\w\-]|$))',text)

def main(iterations=1, initial=True):
    global opts
    if initial:
        opts.proper_subreddit_set.update(opts.subreddits)
    if len(opts.subreddits) == 0:
        print "YOU NEED TO SPECIFY SUBREDDITS TO SEARCH!!!"
        return 1
    scraper = praw_user.scraper()
    for subreddit_text in opts.subreddits:
        if subreddit_text in opts.traversed_subreddits:
            continue
        sys.stdout.flush()
        scrape_subreddit(subreddit_text, opts, scraper)
    if iterations==1:
        validate_subreddits(opts, scraper, initial)
        write_subreddits(opts)
    else:
        validate_subreddits(opts, scraper, initial)
        main(iterations=iterations-1,initial=False)
        return 0
    print 'done'

#initial argument is used to allow large subreddits as initial seeds
def validate_subreddits(opts, scraper, initial):
    opts.traversed_subreddits.update([x.lower() for x in opts.subreddits])
    for subreddit_text in opts.searching_subreddits:
        if subreddit_text in opts.traversed_subreddits:
            continue
        if subreddit_text in opts.exclude_subreddits:
            continue
        print 'searching /r/%s' % subreddit_text
        try:
            subreddit = scraper.subreddit(subreddit_text)
            num_subscribers = subreddit.subscribers
            proper_name = subreddit.display_name
        except Forbidden:
            print '%s is restricted' % subreddit_text
            continue
        except (NotFound, Redirect):
            print '%s is not found' % subreddit_text
            continue
        except BadRequest:
            print '%s is a bad request' % subreddit_text
            print sys.exc_info()
            continue
        if num_subscribers >= opts.min_subscribers and \
           (num_subscribers <= opts.max_subscribers):
            opts.proper_subreddit_set.add(proper_name)
    opts.subreddits = list(opts.proper_subreddit_set)

def write_subreddits(opts):
    if opts.append_list:
        mode = 'w+'
    else:
        mode='w'
    with open(opts.outfile, mode) as f:
        for i, text in enumerate(opts.proper_subreddit_set):
            if i <> 0:
                f.write('\n')
            f.write(text)
    print 'saved %d subreddits to %s' % (i+1, opts.outfile)

@retry_if_broken_connection
def scrape_subreddit(subreddit_text, opts, scraper):
    try:
        subreddit = scraper.subreddit(subreddit_text)
    except Forbidden:
        print '%s is restricted' % subreddit_text
        return 1
    except (Redirect, NotFound):
        print '%s is not found' % subreddit_text
    desc = subreddit.description
    description_search = search_for_subreddits(desc)
    opts.searching_subreddits.update([x[0].lower() for x in description_search])
    if subreddit.wiki_enabled and opts.use_wikis:
        wikis = subreddit.wiki
        wiki_text_list = [get_content_or_blank(w) for w in wikis]
        wiki_search = [search_for_subreddits(text) for text in wiki_text_list]
        for search in wiki_search:
            opts.searching_subreddits.update([r[0].lower() for r in search])

                
            
@retry_if_broken_connection
def get_content_or_blank(wiki):
    if wiki is None:
        return ''
    else:
        try:
            return wiki.content_md
        except TypeError:
            return ''

class listmaker_options(object):
    def __init__(self):
        parser = ArgumentParser()
        parser.add_argument('outfile',
                            help="The file to output the list to.")
        parser.add_argument('-s','--subreddits', dest='subreddits', nargs='+',
                            help='a list of subreddits that are used to search')
        parser.add_argument('-fs','--f-subreddits', dest='f_subreddits', nargs='+',
                            help='a list of filenames with line-separated subreddit names to be '\
                            ' searched')
        parser.add_argument('--min-subscribers', dest='min_subscribers', type=int, default=None,
                             help="If no subreddits are selected, this is the minimum count "\
                             "of (full) threads required for scraping")
        parser.add_argument('--use-wikis', dest='use_wikis', action='store_true',
                            help='Will go through wiki pages of subreddits as well as sidebars.')
        parser.add_argument('--iterations',dest='iterations',default=1,type=int,
                            help="If used, then subreddits that are newly found will be searched "\
                            "in a future iteration. This can be any number, although subreddits "\
                            "that have already been scraped will be omitted.")
        parser.add_argument('--append-list',dest='append_list', action='store_true',
                            help='Will append results to any existing list rather than overwrite '\
                            'it')
        parser.add_argument('--exclude-subreddits',dest='exclude_subreddits',nargs='+',
                            help='lists subreddits that should be excluded from being found '\
                            'This is helpful when there are specific subreddits you want to block '\
                            'off for larger iterations')
        parser.add_argument('--max-subscribers', dest='max_subscribers', default=10e12,
                            type=int,help="Will avoid subs with size greater than this. "\
                            "Useful for when there are many iterations")
        
        
        args = parser.parse_args()
        self.args = args
        good_args = [a for a in dir(args) if not re.match('^_.*',a)]
        for arg in good_args:
            print arg, getattr(args, arg), type(getattr(args, arg))
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
        if args.exclude_subreddits is not None:
            self.exclude_subreddits = args.exclude_subreddits
        else:
            self.exclude_subreddits = []
        self.exclude_subreddits.extend(['random','randnsfw'])
        self.impose('outfile')
        self.impose('min_subscribers')
        self.impose('iterations')
        self.impose('max_subscribers')
        self.use_wikis = args.use_wikis
        self.append_list = args.append_list
        #these will be written in the end
        self.proper_subreddit_set = set()
        #these are lowercase queries for subreddits
        self.searching_subreddits = set()
        self.traversed_subreddits = set()

    def impose(self, varname, validator=None,popfirst=False):
        val = getattr(self.args,varname)
        if val is not None and val is not []:
            if validator is not None:
                assert validator(val)
            setattr(self, varname,val)
        else:
            setattr(self, varname, None)
        return 0

if __name__=='__main__':
    global opts
    opts = listmaker_options()
    main(iterations=opts.iterations)

