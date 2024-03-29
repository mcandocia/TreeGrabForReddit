from __future__ import print_function
import psycopg2
from psycopg2.extensions import AsIs
try:
    from psycopg2.extras import execute_values
except:
    print('Cannot use execute_values function for psycopg2 cursor')
    
from dbinfo import *
import sys
import itertools
import datetime
import pytz

try:
    import regex as re
except ImportError:
    import re

default_schema="default"

class Database(object):
    """this class uses info stored in dbinfo.py (database, user, host, port, password, and 
    tablespace) to create a new schema (if it doesn't exist) under a given name in the database and
    tables (if they don't already exist) for users, comments, and threads.

    unique_ids is a dictionary that controls the creation of primary keys and indexes. It 
    can contain logical values for 'comments', 'users', and 'threads' keys. These must be
    consistent every time the code is run for the same schema. Elements must be explicitly
    set to false to be disabled (useful for keeping track of historical data vs. updating one row).

    The tablespace option is needed for indexing columns that are queried during scraping. Other
    columns and indexes may be made apart from that for your own purposes.

    The returned object has execute(), fetchone(), fetchmany(), and fetchall() methods
    that call the respective methods of the psycopg2 cursor.

    There is a dropall() method that is useful for testing if you want to drop the
    schema and tables. It is vulnerable to injection, so don't use it if you don't 
    have a sanitized schema name."""
    dbtype = 'postgres'
    def clone(self):
        """returns a copy of itself, but with different connections/cursors"""
        return Database(self.name, self,unique_ids, self.silence)
    
    def __init__(
            self,
            name=default_schema,
            unique_ids={'comments':True,'users':True,'threads':True,'subreddits':True},

            silence=False,
            connect_only=False,
            cursor_name = None,
            check_index_mode=None,
            check_awards=False,
            check_trophies=False
    ):
        #initialize connection
        # note: if cursor_name is defined, using this class will be more memory-efficient
        # when retrieving large amounts of data

        # connect_only will not initialize the schema/tables in them; it behaves mostly like a regular cursor,
        # with some extra functionality
        # check_index_mode checkes create index queries for schemas in index name and replaces dots with underscores

        self.conn = psycopg2.connect(database=database, user=username, host=host, port=port,
                                     password=password)

        self.check_trophies = check_trophies
        self.check_awards = check_awards

        self.documented_awards = set()

        if cursor_name is None:
            self.cur = self.conn.cursor()
        else:
            self.cur = self.conn.cursor(cursor_name)
            self.cur_i = 0

        if check_index_mode is None:
            self.check_index_mode = connect_only
        else:
            self.check_index_mode = check_index_mode

        self.cursor_name = cursor_name
        
        if connect_only:
            self.silence=silence
            self.unique_ids = unique_ids
            self.name = None
            return None
        if not silence:
            print(unique_ids)
        self.unique_ids = unique_ids#for clone method
        self.schema=name
        self.silence = silence
        if not silence:
            print('started connection')
        #make schema and tables
        if not silence:
            print("""CREATE SCHEMA IF NOT EXISTS %s;""" % self.schema)
        self.cur.execute("""CREATE SCHEMA IF NOT EXISTS %s;""" % self.schema)
        if not silence:
            print('made schema')
        self.make_log_table()

        if unique_ids.get('threads',True):
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.threads(
            id CHAR(7) PRIMARY KEY,
            title TEXT,
            subreddit VARCHAR(40),
            subreddit_id VARCHAR(9),
            created TIMESTAMP,
            created_utc TIMESTAMP,
            score INT,
            percentage FLOAT,
            author_name VARCHAR(40),
            author_id VARCHAR(8),
            edited TIMESTAMP,
            edited_utc TIMESTAMP,
            author_flair TEXT,
            author_flair_css_class TEXT,
            link_flair_text TEXT,
            link_flair_css_class TEXT,
            is_distinguished BOOLEAN,
            is_spoiler BOOLEAN,
            gold INT,
            silver INT DEFAULT 0,
            platinum INT DEFAULT 0,
            is_self BOOLEAN,
            is_stickied BOOLEAN,
            is_pinned BOOLEAN DEFAULT false,
            url TEXT,
            self_text TEXT,
            over_18 BOOLEAN,
            permalink TEXT,
            self_text_html TEXT,
            num_comments INT,
            comments_deleted INT,
            comments_navigated INT,
            domain TEXT,
            in_contest_mode BOOLEAN,
            scrape_mode VARCHAR(10),--'thread|list|profile|minimal'
            timestamp TIMESTAMP,
            timestamp_utc TIMESTAMP,
            is_video BOOLEAN,
            is_original_content BOOLEAN,
            is_reddit_media_domain BOOLEAN,
            is_robot_indexable BOOLEAN,
            is_meta BOOLEAN,
            is_crosspostable BOOLEAN,
            locked BOOLEAN,
            archived BOOLEAN,
            contest_mode BOOLEAN
            );""" % self.schema)
            '''self.execute("""CREATE INDEX IF NOT EXISTS %s.%s ON %s.threads 
                USING id TABLESPACE %s;""" % (self.schema,'thread_id_index',
                                                    self.schema, tablespace))'''
        else:

            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.threads(
            id CHAR(7),
            title TEXT,
            subreddit VARCHAR(40),
            subreddit_id VARCHAR(9),
            created TIMESTAMP,
            created_utc TIMESTAMP,
            score INT,
            percentage FLOAT,
            author_name VARCHAR(40),
            author_id VARCHAR(8),
            edited TIMESTAMP,
            edited_utc TIMESTAMP,
            author_flair TEXT,
            author_flair_css_class TEXT,
            link_flair_text TEXT,
            link_flair_css_class TEXT,
            is_distinguished BOOLEAN,
            is_spoiler BOOLEAN,
            gold INT,
            silver INT DEFAULT 0,
            platinum INT DEFAULT 0,
            is_self BOOLEAN,
            is_pinned BOOLEAN DEFAULT false,
            is_stickied BOOLEAN,
            url TEXT,
            self_text TEXT,
            over_18 BOOLEAN,
            permalink TEXT,
            self_text_html TEXT,
            num_comments INT,
            comments_deleted INT,
            comments_navigated INT,
            in_contest_mode BOOLEAN,
            domain TEXT,
            scrape_mode VARCHAR(10),--'thread|list|profile|minimal'
            timestamp TIMESTAMP,
            timestamp_utc TIMESTAMP,
            is_video BOOLEAN,
            is_original_content BOOLEAN,
            is_reddit_media_domain BOOLEAN,
            is_robot_indexable BOOLEAN,
            is_meta BOOLEAN,
            is_crosspostable BOOLEAN,
            locked BOOLEAN,
            archived BOOLEAN,
            contest_mode BOOLEAN
            );""" % self.schema)

        if not self.silence:
            print('made threads table')
        if unique_ids.get('users',True):
            #main info
            #username is primary key due to existence of shadowbanned users, who
            #are only identified by name and have no other record other than comments
            #manually encountered
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.users(
            username VARCHAR(40) PRIMARY KEY,
            id VARCHAR(8),
            comment_karma INT,
            post_karma INT,
            is_mod BOOLEAN,
            account_created TIMESTAMP,
            account_created_utc TIMESTAMP,
            is_gold BOOLEAN,
            shadowbanned_by_date TIMESTAMP,
            suspended_by_date TIMESTAMP,
            shadowbanned_by_date_utc TIMESTAMP,
            suspended_by_date_utc TIMESTAMP,
            deleted_by_date TIMESTAMP,
            submissions_silver INT,
            submissions_gold INT,
            submissions_platinum INT,
            comments_silver INT,
            comments_gold INT,
            comments_platinum INT,
            verified BOOLEAN,
            gilded_visible BOOLEAN,
            timestamp TIMESTAMP,
            timestamp_utc TIMESTAMP
            );""" % self.schema)
            
        else:

            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.users(
            username VARCHAR(40),
            id VARCHAR(8),
            comment_karma INT,
            post_karma INT,
            is_mod BOOLEAN,
            account_created TIMESTAMP,
            account_created_utc TIMESTAMP,
            is_gold BOOLEAN,
            shadowbanned_by_date TIMESTAMP,
            suspended_by_date TIMESTAMP,
            shadowbanned_by_date_utc TIMESTAMP,
            suspended_by_date_utc TIMESTAMP,
            deleted_by_date TIMESTAMP,
            submissions_silver INT,
            submissions_gold INT,
            submissions_platinum INT,
            comments_silver INT,
            comments_gold INT,
            comments_platinum INT,
            verified BOOLEAN,
            gilded_visible BOOLEAN,
            timestamp TIMESTAMP,
            timestamp_utc TIMESTAMP
            );""" % self.schema)
            '''self.execute("""CREATE INDEX IF NOT EXISTS %s.%s ON %s.users 
            USING id TABLESPACE %s;""" % 
            (self.schema,'user_id_index',self.schema, tablespace))'''
        if not self.silence:
            print('made users table')

        if unique_ids.get('comments', True):
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.comments(
            id VARCHAR(8) PRIMARY KEY,
            author_name VARCHAR(40),
            author_id VARCHAR(8),
            parent_id VARCHAR(11),
            is_root BOOLEAN,
            text TEXT,
            created TIMESTAMP,
            created_utc TIMESTAMP,
            edited TIMESTAMP,
            edited_utc TIMESTAMP,
            gold INT,
            silver INT DEFAULT 0,
            platinum INT DEFAULT 0,
            is_stickied BOOLEAN DEFAULT false,
            score INT,
            is_distinguished BOOLEAN,
            thread_id VARCHAR(8),
            subreddit VARCHAR(40),
            subreddit_id VARCHAR(9),
            absolute_position INTEGER[],
            nreplies INT,
            thread_begin_timestamp TIMESTAMP,
            scrape_mode VARCHAR(10),--'sub|minimal'
            timestamp TIMESTAMP,
            timestamp_utc TIMESTAMP,
            controversiality SMALLINT
            );""" % self.schema)
            '''self.execute("""CREATE INDEX IF NOT EXISTS %s.%s ON %s.comments 
            USING id TABLESPACE %s;""" % (self.schema,'comment_id_index'
                                          ,self.schema, tablespace))'''
        else:
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.comments(
            id VARCHAR(8),
            author_name VARCHAR(40),
            author_id VARCHAR(8),
            parent_id VARCHAR(11),
            is_root BOOLEAN,
            text TEXT,
            created TIMESTAMP,
            created_utc TIMESTAMP,
            edited TIMESTAMP,
            edited_utc TIMESTAMP,
            gold INT,
            silver INT DEFAULT 0,
            platinum INT DEFAULT 0,
            is_stickied BOOLEAN DEFAULT false,
            score INT,
            is_distinguished BOOLEAN,
            thread_id VARCHAR(8),
            subreddit VARCHAR(40),
            subreddit_id VARCHAR(9),
            absolute_position INTEGER[],
            nreplies INT,
            thread_begin_timestamp TIMESTAMP,
            scrape_mode VARCHAR(10),--'sub|minimal'
            timestamp TIMESTAMP,
            timestamp_utc TIMESTAMP,
            controversiality SMALLINT
            );""" % self.schema)
        self.make_subreddit_table(unique_ids)
        if not self.silence:
            print('made comments table')
        self.usertable = '%s.users' % self.schema
        self.threadtable = '%s.threads' % self.schema
        self.commenttable = '%s.comments'% self.schema
        self.create_moderator_table()
        self.create_traffic_table()
        self.create_related_subreddits_table()
        self.create_wiki_table()
        self.create_awards_tables()
        self.create_trophies_tables()
        self.add_controversial_to_existing_tables()
        self.commit()
        if not self.silence:
            print('committed initial config')
        #create indexes
        #hold off for now

        return None

    # use this to set/reset cursor name
    # may be useful for very large queries
    def set_cursor_name(self, name):
        if not self.silence:
            print('setting cursor name to %s' % name)
        self.cur = self.conn.cursor(name)
        self.cursor_name = name
        self.cur_i = 0
        return 0

    # use this to unset cursor name
    def unset_cursor_name(self):
        if not self.silence:
            print('unsetting cursor name')
        self.cur = self.conn.cursor()
        self.cursor_name = None
        return 0
        
    def execute(self,*args,**kwargs):
        if self.check_index_mode:
            # fix syntax error with schema name if it exists
            args = list(args)
            args[0] = re.sub(
                r'CREATE +INDEX +(IF +NOT +EXISTS\s+)?([A-Za-z_0-9]+)\.([A-Za-z_0-9]+)',
                r'CREATE INDEX \1 \2_\3',
                args[0]
            )
            args = tuple(args)
        if self.cursor_name is None:
            return self.cur.execute(*args, **kwargs)
        else:
            #self.cur.close()
            self.cur_i = (self.cur_i + 1) % 100000
            self.cur = self.conn.cursor('%s_%s' % (self.cursor_name, self.cur_i))
            return self.cur.execute(*args, **kwargs)

    def execute_values(self, *args, **kwargs):
        return execute_values(self.cur, *args, **kwargs)

    # this would have been easier to implement from the start
    def __getattr__(self, name):
        try:
            return getattr(self.cur, name)
        except AttributeError:
            return getattr(self.conn, name)
            

    def insert_user(self, data):
        if '_trophies' in data:
            trophy_data = data.pop('_trophies')
            self.write_trophy_data(trophy_data)
            
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.users(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)
        self.commit()

    def get_user_update_time(self, user_id):
        if user_id is None:
            return None
        try:
            self.execute(("SELECT max(timestamp) FROM %s.users WHERE" % self.schema) +\
                         " id=%s",[user_id])
            return self.fetchall()[0][0]
        except:
            return None
        
    def check_user_update_time(self, user_id, opts):
        update_time = self.get_user_update_time(user_id)
        if not update_time:
            return True
        elif opts.user_delay == -1:
            return False
        else:
            now = datetime.datetime.now(pytz.utc)
            update_time = pytz.utc.localize(update_time)
            if (now - update_time).seconds < 3600*24 * opts.user_delay:# - (now-update_time).days:
                return False
            return True

    def filter_users(self, opts):
        # just get all of the darn data
        print('Filtering %d users' % len(opts.users))
        now = pytz.utc.localize(datetime.datetime.now())
        self.execute("SELECT username, timestamp FROM %s.users" % self.schema)
        user_timestamps = {x[0]: pytz.utc.localize(x[1]) for x in self.fetchall()}
        opts.users = [
            user for user in opts.users
            if user not in user_timestamps
            or ((now - user_timestamps.get(user, now)).seconds >= 3600*24 * opts.user_delay and opts.user_delay != -1)
        ]
        print('Now have %d users remaining after filtering' % len(opts.users))
        return True
        
    def get_thread_update_time(self, thread_id):
        try:
            self.execute(("SELECT max(timestamp) FROM %s.threads WHERE" % self.schema) +\
                         " id=%s AND scrape_mode='thread'",[thread_id])
            update_time =  self.fetchall()[0][0]
            #print thread_id
            #print update_time
            return update_time
        except psycopg2.ProgrammingError:
            print('cannot get thread update time...check for bugs')
            print(sys.exc_info())
            return None

    def get_subreddit_update_time(self, subreddit_text):
        try:
            self.execute(("SELECT max(timestamp) FROM %s.subreddits WHERE" % self.schema) +\
                         " lower(subreddit)=lower(%s)",[subreddit_text])
            return self.fetchall()[0][0]
        except:
            print(sys.exc_info())
            return None

    def check_subreddit_update_time(self, subreddit_text, opts):
        update_time = self.get_subreddit_update_time(subreddit_text)
        if not update_time:
            return True
        elif opts.subreddit_delay == -1:
            return False
        else:
            now = datetime.datetime.now(pytz.utc)
            update_time = pytz.utc.localize(update_time)
            diff1 = (now - update_time).seconds
            diff2 = 3600*24 * (opts.subreddit_delay - (now-update_time).days)
            if opts.verbose and diff1 < diff2:
                
                print('%s seconds remaining' % round(diff2 - diff1))
            if diff1 < diff2:
                return False
            return True

    def update_user(self, data):
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('UPDATE %s' % self.schema) + '.users SET ' + make_update_template(values) + \
                    ' WHERE id=\'%s\'' % data['id']
        self.execute(statement, make_update_data(cols, values))


    def insert_thread(self, data):
        if '_award_data' in data:
            awards = data.pop('_award_data', {})
            if awards:
                self.insert_thread_awards(awards)
        #print data
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.threads(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)

    def update_thread(self, data):
        if '_award_data' in data:
            awards = data.pop('_award_data', {})
            if awards:
                self.insert_thread_awards(awards)
        cols = data.keys()                
        values = [data[key] for key in cols]
        statement = ('UPDATE %s' % self.schema) + '.threads SET ' + make_update_template(values) + \
                    ' WHERE id=\'%s\'' % data['id']
        self.execute(statement, make_update_data(cols, values))


    def insert_comment(self, data):
        if '_award_data' in data:
            awards = data.pop('_award_data', {})
            if awards:
                self.insert_comment_awards(awards)
        #print data
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.comments(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)

    def update_comment(self, data):
        if '_award_data' in data:
            awards = data.pop('_award_data', {})
            if awards:
                self.insert_comment_awards(awards)
        cols = data.keys()
        values = [data[key] for key in cols]

        statement = ('UPDATE %s' % self.schema) + '.comments SET ' + make_update_template(values)+ \
                    ' WHERE id=\'%s\'' % data['id']
        self.execute(statement, make_update_data(cols, values))

    def insert_subreddit(self, data):
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.subreddits(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)

    def update_subreddit(self, data):
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('UPDATE %s' % self.schema) + '.subreddits SET ' + \
                    make_update_template(values)+ \
                    ' WHERE subreddit=\'%s\'' % data['subreddit']
        self.execute(statement, make_update_data(cols, values))

    def check_if_traffic_entry_exists(self, data):
        self.execute("""SELECT max(timestamp) FROM %s.traffic 
        WHERE
        subreddit=%%s AND
        period_type=%%s AND
        time=%%s""" % self.schema, (data['subreddit'],
                                    data['period_type'],
                                    data['time']))
        result = self.fetchall()[0][0]
        return result is not None

    def insert_thread_awards(self, awards):
        if len(awards) == 0:
            return

        keys = list(awards[0].keys())
        BASE_TEMPLATE = """
        INSERT INTO {schema}.thread_awards({columns}) VALUES({qm_template})
        ON CONFLICT(thread_id, award_name) DO UPDATE SET award_count = excluded.award_count
        """.format(
            schema=self.schema,
            columns = ','.join(keys),
            qm_template = ','.join([' %s '] * len(keys))
        )

        for rowdict in awards:
            row = [rowdict[k] for k in keys]            
            self.execute(
                BASE_TEMPLATE,
                row
            )
        self.commit()


    def insert_comment_awards(self, awards):
        if len(awards) == 0:
            return

        keys = list(awards[0].keys())
        BASE_TEMPLATE = """
        INSERT INTO {schema}.comment_awards({columns}) VALUES({qm_template})
        ON CONFLICT(comment_id, award_name) DO UPDATE SET award_count = excluded.award_count
        """.format(
            schema=self.schema,
            columns = ','.join(keys),
            qm_template = ','.join([' %s '] * len(keys))
        )

        for rowdict in awards:
            row = [rowdict[k] for k in keys]
            self.execute(
                BASE_TEMPLATE,
                row
            )
        self.commit()        


    def insert_traffic(self, data):
        #print data
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.traffic(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)


    def update_traffic(self, data):
        cols = data.keys()
        values = [data[key] for key in cols]

        statement = """UPDATE %s.traffic SET %s
                     WHERE subreddit=%%s AND
        period_type=%%s AND
        time=%%s""" % (self.schema, make_update_template(values))
        self.execute(statement, make_update_data(cols, values) + [data['subreddit'],
                                                                  data['period_type'],
                                                                  data['time']])

        
    #for now wiki and related_subreddits are in history mode
    #so queries that search for subreddits that have been collected multiple times should
    #use "HAVING max(timestamp)" in a GROUP BY clause can alleviate this easily
    def insert_wiki(self, data):
        #print data
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.wikis(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)


    def insert_related_subreddit(self, data):
        #print data
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.related_subreddits(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)


    def commit(self):
        self.conn.commit()
        return True

    def close(self):
        self.conn.close()
        return 0
    
    def fetchall(self):
        return self.cur.fetchall()
    
    def fetchone(self):
        return self.cur.fetchone()
    
    def fetchmany(self, *args, **kwargs):
        return self.cur.fetchmany(*args, **kwargs)

    def dropall(self, exclude_schema=False):
        """WARNING: THIS should only be used for cleaning up during testing"""
        for table in (self.commenttable, self.usertable, self.threadtable):
            self.execute("DROP TABLE %s;" % table)
            print('dropped %s' % table)
        self.execute("DROP INDEX IF EXISTS %s.user_name_index;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.log;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.subreddits;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.moderators;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.traffic;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.related_subreddits;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.wikis;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.trophies" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.awards" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.user_awards" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.comment_awards" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.thread_awards" % self.schema)
        if not exclude_schema:
            self.execute("DROP SCHEMA %s;" % self.schema)
            print('dropped schema %s' % self.schema)
        self.commit()

    def make_subreddit_table(self, unique_ids):
        if unique_ids.get('subreddits',True):
            self.execute("""CREATE TABLE IF NOT EXISTS %s.subreddits(
            subreddit VARCHAR(40) PRIMARY KEY,
            accounts_active INTEGER,
            created TIMESTAMP,
            created_utc TIMESTAMP,
            description TEXT,
            has_wiki BOOLEAN,
            public_traffic BOOLEAN,
            rules JSONb,
            submit_text TEXT,
            submit_link_label TEXT,
            subreddit_type VARCHAR(16),
            subscribers INTEGER,
            title TEXT,
            timestamp TIMESTAMP,
            timestamp_utc TIMESTAMP
            ); """ % self.schema)
        else:
            self.execute("""CREATE TABLE IF NOT EXISTS %s.subreddits(
            subreddit VARCHAR(40),
            accounts_active INTEGER,
            created TIMESTAMP,
            created_utc TIMESTAMP,
            description TEXT,
            has_wiki BOOLEAN,
            public_traffic BOOLEAN,
            rules JSONb,
            submit_text TEXT,
            submit_link_label TEXT,
            subreddit_type VARCHAR(16),
            subscribers INTEGER,
            title TEXT,
            timestamp TIMESTAMP,
            timestamp_utc TIMESTAMP
            ); """ % self.schema)

    def make_log_table(self):
        self.execute("CREATE TABLE IF NOT EXISTS %s.log" % self.schema + """(
        start_time TIMESTAMP PRIMARY KEY,
        end_time TIMESTAMP,
        command TEXT,
        stop_reason VARCHAR(20),
        notes TEXT);""")

    def add_log_entry(self, opts):
        data={'start_time':opts.start_time,
              'end_time':None,
              'command':opts.original_command,
              'stop_reason':None,
              'notes':None}
        
        self.execute(("INSERT INTO %s.log(start_time, " % self.schema) + """
                     end_time,
                     command,
                     stop_reason,
        notes)
        VALUES (%s, %s, %s, %s, %s);""", [data['start_time'],
                                          data['end_time'],
                                          data['command'],
                                          data['stop_reason'],
                                          data['notes']])
        self.commit()
        if not self.silence:
            print('made log entry')

    def update_log_entry(self, opts, reason, notes=None, silent=False):
        start_time = opts.start_time
        data={'end_time':datetime.datetime.now(pytz.utc),
              'stop_reason':reason,
              'notes':notes}
        cols = data.keys()
        values = [data[key] for key in cols]
        update_data = make_update_data(cols, values)
        template = make_update_template(values)
        statement = ('UPDATE %s' % self.schema) + '.log SET ' + template + \
                    ' WHERE start_time=%s'
        self.execute(statement, update_data + [opts.start_time,] )
        self.commit()
        if not silent:
            print('updated log')

    def create_moderator_table(self):
        #this table does not use primary keys
        self.execute(("""CREATE TABLE IF NOT EXISTS %s.moderators""" % self.schema) +
                     """(subreddit VARCHAR(40),
                     username VARCHAR(40),
                     timestamp TIMESTAMP,
                     timestamp_utc TIMESTAMP,
                     pos INTEGER)""")
        
    def create_traffic_table(self):
        self.execute("""CREATE TABLE IF NOT EXISTS %s.traffic(
        subreddit VARCHAR(40),
        period_type VARCHAR(4),
        time TIMESTAMP,
        time_utc TIMESTAMP,
        total_visits INTEGER,
        unique_visits INTEGER,
        timestamp TIMESTAMP,
        timestamp_utc TIMESTAMP,
        PRIMARY KEY (subreddit, period_type, time))
        """ % self.schema)

    def create_related_subreddits_table(self):
        self.execute("""CREATE TABLE IF NOT EXISTS %s.related_subreddits(
        subreddit VARCHAR(40),
        related_subreddit VARCHAR(40),
        relationship_type VARCHAR(8),
        wiki_name TEXT,
        related_is_private BOOLEAN,
        timestamp TIMESTAMP,
        timestamp_utc TIMESTAMP)""" % self.schema)

    def create_wiki_table(self):
        self.execute("""CREATE TABLE IF NOT EXISTS %s.wikis(
        subreddit VARCHAR(40),
        content TEXT,
        name TEXT,
        timestamp TIMESTAMP,
        timestamp_utc TIMESTAMP)""" % self.schema)

    def create_awards_tables(self):
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS %s.awards(
                name VARCHAR(64),
                coin_price INTEGER,
                description TEXT,
                coin_reward INTEGER,
                giver_coin_reward INTEGER,
                subreddit_coin_reward INTEGER,
                days_of_premium INTEGER,
                award_type VARCHAR(32),
                award_sub_type VARCHAR(32),
                subreddit_id VARCHAR(16),
                awardings_required_to_grant_benefits INTEGER,
                days_of_drip_extension INTEGER,
                static_icon_url TEXT,
                UNIQUE (name, subreddit_id)
            )

            """ % self.schema
        )
        
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS %s.user_awards(
            username VARCHAR(40),
            award_name VARCHAR(64),
            subreddit_id VARCHAR(10),
            award_count INTEGER,

            PRIMARY KEY (username, award_name)
            )

            """ % self.schema
        )

        self.execute(
            """
            CREATE TABLE IF NOT EXISTS %s.thread_awards(
            thread_id VARCHAR(9),
            subreddit_id VARCHAR(10),
            award_name VARCHAR(64),
            award_count INTEGER,

            PRIMARY KEY (thread_id, award_name)
            )

            """ % self.schema
        )

        self.execute(
            """
            CREATE TABLE IF NOT EXISTS %s.comment_awards(
            comment_id VARCHAR(9),
            award_name VARCHAR(64),
            award_count INTEGER,
            subreddit_id VARCHAR(10),
            PRIMARY KEY (comment_id, award_name)
            )

            """ % self.schema
        )

        # update documented awards
        self.execute("SELECT name, subreddit_id FROM %s.awards" % self.schema)
        self.documented_awards.update([(x[0], x[1]) for x in self.fetchall()])
        if not self.silence:
            print('Established awards tables, with %s awards currently present' % len(self.documented_awards))


    def create_trophies_tables(self):
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS {schema}.trophies(
                trophy_name VARCHAR(128),
                username VARCHAR(64),
                granted_at TIMESTAMP,
                granted_at_utc TIMESTAMP,

            PRIMARY KEY (trophy_name, username)
            )

            """.format(schema = self.schema)
        )
        self.commit()

    def write_trophy_data(self, trophy_data):

        for trophy in trophy_data:
            self.execute("""
            INSERT INTO {schema}.trophies(
                trophy_name, 
                username,
                granted_at,
                granted_at_utc
            ) VALUES(%s , %s , %s , %s)
            ON CONFLICT DO NOTHING
            """.format(schema=self.schema), trophy)
        self.commit()

    def update_documented_awards(self, award_data):
        if len(award_data) == 0:
            return 0

        #print(award_data)
        #exit()

        keys = list(list(award_data.values())[0].keys())
        value_list = [
            [x[key] for key in keys]
            for x in award_data.values()
        ]

        TEMPLATE = """INSERT INTO {schema}.awards({values_columns}) VALUES ({qm_template});""".format(
            schema=self.schema,
            values_columns = ','.join(keys),
            qm_template = ','.join([' %s '] * len(keys))
        )

        cnt = 0

        for v in value_list:
            if (v[keys.index('name')], v[keys.index('subreddit_id')]) not in self.documented_awards:
                cnt += 1
                self.execute(TEMPLATE, v)
                self.commit()
                self.documented_awards.add((v[keys.index('name')], v[keys.index('subreddit_id')]))
                if not self.silence:
                    print('Added %s to documented awards' % v[keys.index('name')])
                    subreddit_id = v[keys.index('subreddit_id')]
                    if subreddit_id is not None:
                        print('(subreddit ID = %s)' % subreddit_id)

        return cnt
        

    def add_utc_columns_to_existing_tables(self, verbose=False):
        # this function is just a table-alterer so that users with older tables don't have to go in and manually adjust them
        print('ensuring tables have utc columns')

        for table, cols in [
                ('comments', ['created','edited']),
                ('threads', ['created','edited']),
                ('users', ['account_created','shadowbanned_by_date','suspended_by_date']),
                ('wikis', []),
                ('moderators', []),
                ('subreddits', ['created',]),
                ('traffic', ['time']),
                ('related_subreddits', [])
        ]:
            for col in cols + ['timestamp']:
                table_name = "%s.%s" % (self.schema, table)
                try:
                    colname = '%s_utc' % col
                    self.execute(
                        'ALTER TABLE  {table_name} ADD COLUMN  {colname} TIMESTAMP'.format(
                            colname=colname,
                            table_name=table_name
                        )
                    )
                    self.commit()
                except Exception as e:
                    self.rollback()
                    if verbose:
                        print(e)

    def add_gilded_columns_to_existing_tables(self, verbose=False):
        for table, cols in [
                ('comments', ['silver','gold','platinum']),
                ('threads', ['silver','gold','platinum']),
                ('users', ['comments_silver','comments_gold','comments_platinum', 'submissions_silver','submissions_gold','submissions_platinum']),
                

        ]:
            table_name = "%s.%s" % (self.schema, table)
            for colname in cols:
                try:
                    self.execute(
                        'ALTER TABLE  {table_name} ADD COLUMN  {colname} INTEGER'.format(
                            colname=colname,
                            table_name=table_name
                        )
                    )
                    self.commit()
                except Exception as e:
                    self.rollback()
                    if verbose:
                        print(e)

        try:
            self.execute('ALTER TABLE  {schema}.users ADD COLUMN  visible_gilded BOOLEAN'.format(
                schema=self.schema
            )
            )
            self.commit()
        except Exception as e:
            self.rollback()
            if verbose:
                print(e)

    # also adds more bool columns to thread table
    def add_controversial_to_existing_tables(self, verbose=False):
        for table, cols in [
                ('comments', ['controversiality']),
                ('threads', [
                    'is_video',
                    'is_original_content',
                    'is_reddit_media_domain',
                    'is_robot_indexable',
                    'is_meta',
                    'is_crosspostable',
                    'locked',
                    'archived',
                    'contest_mode' 
                ]
                )
        ]:
            for colname in cols:
                if table == 'threads':
                    dtype = 'BOOLEAN'
                else:
                    dtype = 'SMALLINT'
                try:
                    query = 'ALTER TABLE {schema}.{tablename} ADD COLUMN {column} {coltype}'.format(
                        schema=self.schema,
                        tablename=table,
                        column=col,
                        coltype=dtype
                    )
                    self.execute(query)
                    self.commit()
                except Exception as e:
                    self.rollback()
                    if verbose:
                        print(e)
                        
        return 0
                    

    # extra function that can be used to optimize queries summarizing data with subreddits and users
    def build_username_subreddit_domain_indexes(self, schema=None, commit=False, verbose=False):
        if schema is None:
            schema = self.schema

        index_queries = [
            'CREATE INDEX IF NOT EXISTS {schema}_{table}_{cols}_idx ON {schema}.{table}({comma_cols})'.format(
                schema=schema,
                table=table,
                cols='_'.join(cols),
                comma_cols=','.join(cols)
            )
            for table, cols in
            [
                # comments
                ('comments',['subreddit']),
                ('comments',['author_name']),
                ('comments',['author_name','subreddit']),
                
                # threads
                ('threads',['subreddit']),
                ('threads',['author_name']),
                ('threads',['author_name', 'subreddit']),

                # domains
                ('threads', ['domain']),
                ('threads', ['author_name','domain']),
            ]
        ]

        for query in index_queries:
            if verbose:
                print(query)
            self.execute(query)

        if commit:
            print('Committing!')
            self.commit()

        print('Built indices!')

        return True

        
# helpful function to create queries for indexes with uniform naming convention
def standardized_index_query(
        table_name,
        columns,
        schema=None
):
    if isinstance(columns, str):
        columns = [columns]
        
    if schema is not None:
        safe_table_name = '%s_%s' % (schema, table_name)        
        table_name = '%s.%s' % (schema, table_name)
    else:
        safe_table_name = table_name

    index_name = "{table_name}_{cols}_idx".format(table_name=safe_table_name, cols='_'.join(columns))

    index_query = "CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({cols})".format(
        index_name=index_name,
        table_name=table_name,
        cols=','.join(columns)
    )

    return index_query
    



def make_update_data(cols, values):
    d1 =  ((AsIs(col), val) for col, val in zip(cols, values) if val is not None)
    merged = tuple(itertools.chain.from_iterable(d1))
    return list(merged)

def make_update_template(values):
    return ','.join(['%s=%s' for v in values if v is not None])
