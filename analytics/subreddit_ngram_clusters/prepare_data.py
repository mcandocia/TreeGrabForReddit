import sys
import os
from argparse import ArgumentParser
import datetime
import re
import nltk
from copy import deepcopy
import csv

base_dir = os.path.abspath(
    os.path.join(
        os.path.join( 
            os.path.dirname( 
                os.path.abspath(__file__) 
            ),
            os.pardir
        ),
        os.pardir
    )
)

sys.path.insert(0, base_dir)

import db
from scraper import extract_lines
from scraper import handle_boolean

def remove_non_ascii(x):
    return ''.join([s for s in x if ord(s) < 128])

def proper_split(x):
    return re.split(' ', re.sub(r'[!?_^.*|`\[\] \n\r,():/]+',' ', x))

def parse_string(x):
    return proper_split(remove_non_ascii(x).lower())

def date_range_conditional(opts):
    if opts.date_range==None:
        return ' true '
    else:
        return opts.db.cur.mogrify(" created BETWEEN %s AND %s ",
                                   (opts.date_range[0],opts.date_range[1]))

def reduce_dict_size(x, size=100000):
    if len(x) > size:
        #print 'reducing dict size...'
        for key in x.keys():
            if x[key] < 3:
                x.pop(key)
            elif len(x) > size * 1.6:
                #try avoiding doing this at first to avoid major alterations, but may be necessary
                #to avoid a second scrape
                x[key] == x[key] - 1
def main():
    #set initial query for processing
    opts = tree_options()
    query = opts.db.cur.mogrify('''SELECT subreddit, text FROM %s.comments
    WHERE subreddit=ANY(%%s) AND %s''' % (opts.db.schema,
                                          date_range_conditional(opts)),
                                (opts.subreddits,))
    print query
    #set up nltk
    stopwords = nltk.corpus.stopwords.words('english')
    stopwords.append('')
    stopwords.extend(['www','http','https','com','org','net','gov','io','ly','co','uk'])
    #set greatly increases efficiency versus list
    stopwords = set(stopwords)
    n_values = (1,2,3)
    #get overall ngram counts
    n_dict_list = [{} for _ in n_values]
    opts.db.execute(query)
    print 'beginning data extraction'
    loop_counter = 0
    while True:
        print loop_counter * 50000
        batch = opts.db.fetchmany(50000)
        if len(batch)==0:
            break
        for entry in batch:
            subreddit = entry[0]
            text = entry[1]
            proper_text = parse_string(text)
            for n in n_values:
                #used for avoiding duplicates
                comment_set = set()
                ngrams = nltk.ngrams(proper_text, n)
                for gram in ngrams:
                    if gram in comment_set:
                        continue
                    #limit number of checks if possible
                    if n==1:
                        if gram[0] in stopwords:
                            continue
                    if n==2:
                        if gram[0] in stopwords:
                            if gram[1] in stopwords:
                                continue
                    comment_set.add(gram)
                for comment in comment_set:
                    n_dict_list[n-1][comment] = n_dict_list[n-1].get(comment, 0) + 1
        [reduce_dict_size(x, 200000) for x in n_dict_list]
        loop_counter += 1
        if len(batch) < 50000:
            break
    print 'gathered overall counts'
    #determine most common words
    ngram_sizes = [5000,1500,500,300,200,100]
    sorted_ngrams = [sorted(x, key = x.__getitem__)[::-1] for x in n_dict_list]
    ngram_list = [set(x[:ngram_sizes[i]]) for i, x in enumerate(sorted_ngrams)]
    print 'extracted most common words'
    #now re-extract words over subreddit
    subreddit_example_dict = {subreddit:{} for subreddit in opts.subreddits}
    subreddit_dict_list = [deepcopy(subreddit_example_dict) for _ in n_values]
    opts.db.execute(query)
    loop_counter = 0
    while True:
        batch = opts.db.fetchmany(50000)
        print loop_counter * 50000
        if len(batch)==0:
            break
        for entry in batch:
            subreddit = entry[0]
            text = entry[1]
            proper_text = parse_string(text)
            for n in n_values:
                #used for avoiding duplicates
                comment_set = set()
                ngrams = nltk.ngrams(proper_text, n)
                for gram in ngrams:
                    if gram not in ngram_list[n-1]:
                        continue
                    comment_set.add(gram)
                for gram in comment_set:
                    subreddit_dict_list[n-1][subreddit][gram] = subreddit_dict_list[n-1][subreddit]\
                                                        .get(gram, 0) + 1
        loop_counter += 1
        if len(batch) < 50000:
            break
    print 'gathered subreddit word-count data'
    #more reliable
    ngram_list2 = [list(x) for x in ngram_list]
    #now write data to CSV
    with open(opts.outfile,'wb') as f:
        writer = csv.writer(f)
        all_keys = [x for ngrams in ngram_list2 for x in ngrams]
        header = ['SUBREDDIT'] + ['_'.join(x) for x in all_keys]
        writer.writerow(header)
        print 'wrote header'
        for subreddit in opts.subreddits:
            row = [subreddit] + [subreddit_dict_list[i][subreddit].get(key, 0)
                                 for i in range(len(n_values))
                                 for key in ngram_list2[i]]
            writer.writerow(row)
        print 'wrote subreddits'
    print 'done'
            
            



class tree_options(object):
    def __init__(self):
        parser = ArgumentParser()
        parser.add_argument('name',
                            help="the schema name that is used to retrieve and store tables in")
        parser.add_argument('--mode',choices=['subreddit','user','thread'],dest='mode',
                            help="The clustering mode desired after the new table is set up. "\
                            "User mode will use subreddits, and subreddit/thread mode will use "\
                            "users.")
        parser.add_argument('--date-range',nargs=2,dest='date_range',
                            help=r"If provided, only threads/comments within the date range are "
                            " used. Format is %m-%d-%Y (e.g., 05-21-2016)")
        parser.add_argument('--destination-table',dest='destination_table',
                            help="The table within the schema to contain the resulting summary "
                            "data")
        parser.add_argument('--summary-statistics', dest='summary_statistics', nargs='+',
                            choices=['count','signed_counts','sums','signed_sums','min','max',
                                     'presence','all'],
                            help="The different statistics collected for each subreddit. ")
        parser.add_argument('-s','--subreddits', dest='subreddits', nargs='+',
                            help='a list of subreddits that are used as a filter or target')
        parser.add_argument('--ids',dest='thread_ids', nargs='+',
                            help='a list of thread IDs that will be used as a filter or target')
        parser.add_argument('-fs','--f-subreddits', dest='f_subreddits', nargs='+',
                            help='a list of filenames with line-separated subreddit names to be '\
                            ' used as filters are targets')
        parser.add_argument('-u','--users', dest='users', nargs='+',
                            help='A list of users to search through that will be used as targets')
        parser.add_argument('-fu','--f-users', dest='f_users',nargs='+',
                            help="A filename containing a list of users to be used as targets")
        parser.add_argument('--f-ids',dest='f_thread_ids', nargs='+',
                            help='a list of filenames with line-separated thread IDs to be used as'\
                            ' filters or targets')
        parser.add_argument('--out',dest='outfile',help="If specified, a CSV will be produced "\
                            "with the output of the resulting table. Make sure the target "\
                            "directory has appropriate permissions.")
        parser.add_argument('--min-samples', dest='min_samples', type=int, default=None,
                             help="If no subreddits are selected, this is the minimum count "\
                             "of (full) threads required for scraping")
        args = parser.parse_args()
        self.args = args
        good_args = [a for a in dir(args) if not re.match('^_.*',a)]
        for arg in good_args:
            print arg, getattr(args, arg), type(getattr(args, arg))
        self.name = args.name
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
        self.impose('mode')
        self.impose('summary_statistics')
        self.impose('date_range')
        self.impose('destination_table')
        self.impose('outfile')
        self.impose('min_samples')
        self.db = db.Database(self.name, {})
        #if 'all' in self.summary_statistics:
        #    self.summary_statistics = ['count','signed_counts','sums','signed_sums','min','max',
        #                               'presence']
        if self.date_range:
            self.date_range = [datetime.datetime.strptime(d, '%m-%d-%Y') for d in self.date_range]
        

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
    main()
