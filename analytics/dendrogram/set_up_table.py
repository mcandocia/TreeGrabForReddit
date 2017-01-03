import sys
import os
from argparse import ArgumentParser
import datetime
import re

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


def main():
    opts = tree_options()
    #note that this will not work in history mode (conversion is needed first)
    db = opts.db
    if opts.mode == 'user':
        print 'not implemented yet'
    elif opts.mode == 'thread':
        print 'not implemented yet'
    elif opts.mode == 'subreddit':
        #select all comments from users in appropriate subreddits that fit the date range
        #for each user-subreddit pair, evaluate each chosen summary statistic and place into a row
        #user_id | subreddit | statistic_name | value (integer)
        #(make a temporary table containing this subset)
        opts.db.execute(("""CREATE TABLE %s.%s AS """ % (opts.name,
                                                                        opts.destination_table +
                                                                        '__TMP') ) +
                        ("""SELECT author_name AS username,
                        subreddit,
                        score FROM %s.comments """ % (opts.name) ) +
                        """ WHERE """ + ' AND '.join([date_range_conditional(opts),
                                                      subreddit_conditional(opts)]))
        
        #make table to store data in
        opts.db.execute("""DROP TABLE IF EXISTS %s.%s""" % (opts.name, opts.destination_table))
        query = ("""CREATE TABLE %s.%s AS (""" % (opts.name, opts.destination_table) + 
                 """SELECT subreddit, username """ +
                 construct_aggregate_queries(opts) +
                 ' FROM %s.%s ' % (opts.name, opts.destination_table + '__TMP') +
                 ' GROUP BY subreddit, username);')
        print query
        opts.db.execute(query)
        opts.db.execute("DROP TABLE %s.%s" % (opts.name, opts.destination_table + '__TMP'))
        opts.db.commit()
        print 'created table and droppped temp table'
        if opts.outfile:
            opts.db.execute("COPY %s.%s TO '%s' DELIMITER ',' CSV HEADER" %
                            (opts.name, opts.destination_table, opts.outfile))

def construct_aggregate_queries(opts):
    #choices=['count','signed_counts','sums','signed_sums','min','max',
    #'presence'],
    queries = []
    choices = opts.summary_statistics
    if len(choices) == 0 or choices==['presence']:
        return ' '

    if 'count' in choices:
        queries.append(' count(*) AS count')
    if 'signed_counts' in choices:
        queries.append(' count(CASE WHEN score > 0 THEN 1 ELSE NULL END) AS pos_count')
        queries.append(' count(CASE WHEN score < 1 THEN 1 ELSE NULL END) AS neg_count')
    if 'sums' in choices:
        queries.append(' sum(score) ')
    if 'signed_sums' in choices:
        queries.append(' sum(CASE WHEN score > 0 THEN score ELSE 0 END) as pos_sum ')
        queries.append(' sum(CASE WHEN score < 0 THEN score ELSE 0 END) as neg_sum ')
    if 'min' in choices:
        queries.append(' min(score) ')
    if 'max' in choices:
        queries.append(' max(score) ')
    return ', ' + ' ,'.join(queries) + ' ' 


def date_range_conditional(opts):
    if opts.date_range==None:
        return ' true '
    else:
        return opts.db.cur.mogrify(" created BETWEEN %s AND %s ",
                                   (opts.date_range[0],opts.date_range[1]))

def subreddit_conditional(opts):
    if len(opts.subreddits)==0:
        print 'WARNING: no subreddit filter may take a long time/many resources '
        prompt_yesno()
        return ' true '
    else:
        return opts.db.cur.mogrify(" subreddit=ANY(%s) ", [opts.subreddits,])

def make_sub_table_for_subreddit(opts):
    opts.db.execute()

def prompt_yesno():
    res = raw_input("Proceed? ")
    if res.lower() in ['y','yes']:
        return True
    else:
        sys.exit()

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
        parser.add_argument('--destination-table',dest='destination_table',required=True,
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
        self.db = db.Database(self.name, {})
        if 'all' in self.summary_statistics:
            self.summary_statistics = ['count','signed_counts','sums','signed_sums','min','max',
                                       'presence']
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
