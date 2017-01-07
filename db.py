import psycopg2
from psycopg2.extensions import AsIs
from dbinfo import *
import sys
import itertools
import datetime
import pytz

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
    def __init__(self, name=default_schema,
                 unique_ids={'comments':True,'users':True,'threads':True,'subreddits':True}):
        #initialize connection
        self.conn = psycopg2.connect(database=database, user=username, host=host, port=port,
                                     password=password)
        self.cur = self.conn.cursor()
        print unique_ids
        self.schema=name
        print 'started connection'
        #make schema and tables
        print """CREATE SCHEMA IF NOT EXISTS %s;""" % self.schema
        self.cur.execute("""CREATE SCHEMA IF NOT EXISTS %s;""" % self.schema)
        print 'made schema'
        self.make_log_table()
        if unique_ids.get('threads',True):
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.threads(
            id CHAR(6) PRIMARY KEY,
            title TEXT,
            subreddit VARCHAR(30),
            subreddit_id VARCHAR(9),
            created TIMESTAMP,
            score INT,
            percentage FLOAT,
            author_name VARCHAR(30),
            author_id VARCHAR(6),
            edited TIMESTAMP,
            author_flair TEXT,
            author_flair_css_class TEXT,
            is_distinguished BOOLEAN,
            gold INT,
            is_self BOOLEAN,
            is_stickied BOOLEAN,
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
            timestamp TIMESTAMP
            );""" % self.schema)
            '''self.execute("""CREATE INDEX IF NOT EXISTS %s.%s ON %s.threads 
                USING id TABLESPACE %s;""" % (self.schema,'thread_id_index',
                                                    self.schema, tablespace))'''
        else:
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.threads(
            id CHAR(6),
            title TEXT,
            subreddit VARCHAR(30),
            subreddit_id VARCHAR(9),
            created TIMESTAMP,
            score INT,
            percentage FLOAT,
            author_name VARCHAR(30),
            author_id VARCHAR(6),
            edited TIMESTAMP,
            author_flair TEXT,
            author_flair_css_class TEXT,
            is_distinguished BOOLEAN,
            gold INT,
            is_self BOOLEAN,
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
            timestamp TIMESTAMP
            );""" % self.schema)
        print 'made threads table'
        if unique_ids.get('users',True):
            #main info
            #username is primary key due to existence of shadowbanned users, who
            #are only identified by name and have no other record other than comments
            #manually encountered
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.users(
            username VARCHAR(30) PRIMARY KEY,
            id VARCHAR(6),
            comment_karma INT,
            post_karma INT,
            is_mod BOOLEAN,
            account_created TIMESTAMP,
            is_gold BOOLEAN,
            shadowbanned_by_date TIMESTAMP,
            suspended_by_date TIMESTAMP,
            deleted_by_date TIMESTAMP,
            timestamp TIMESTAMP
            );""" % self.schema)
            
        else:
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.users(
            username VARCHAR(30),
            id VARCHAR(6),
            comment_karma INT,
            post_karma INT,
            is_mod BOOLEAN,
            account_created TIMESTAMP,
            is_gold BOOLEAN,
            shadowbanned_by_date TIMESTAMP,
            suspended_by_date TIMESTAMP,
            deleted_by_date TIMESTAMP,
            timestamp TIMESTAMP
            );""" % self.schema)
            '''self.execute("""CREATE INDEX IF NOT EXISTS %s.%s ON %s.users 
            USING id TABLESPACE %s;""" % 
            (self.schema,'user_id_index',self.schema, tablespace))'''
        print 'made users table'
        if unique_ids.get('comments', True):
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.comments(
            id VARCHAR(8) PRIMARY KEY,
            author_name VARCHAR(30),
            author_id VARCHAR(6),
            parent_id VARCHAR(11),
            is_root BOOLEAN,
            text TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            gold INT,
            score INT,
            is_distinguished BOOLEAN,
            thread_id VARCHAR(8),
            subreddit VARCHAR(30),
            subreddit_id VARCHAR(9),
            absolute_position INTEGER[],
            nreplies INT,
            thread_begin_timestamp TIMESTAMP,
            scrape_mode VARCHAR(10),--'sub|minimal'
            timestamp TIMESTAMP
            );""" % self.schema)
            '''self.execute("""CREATE INDEX IF NOT EXISTS %s.%s ON %s.comments 
            USING id TABLESPACE %s;""" % (self.schema,'comment_id_index'
                                          ,self.schema, tablespace))'''
        else:
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.comments(
            id VARCHAR(8),
            author_name VARCHAR(30),
            author_id VARCHAR(6),
            parent_id VARCHAR(11),
            is_root BOOLEAN,
            text TEXT,
            created TIMESTAMP,
            edited TIMESTAMP,
            gold INT,
            score INT,
            is_distinguished BOOLEAN,
            thread_id VARCHAR(8),
            subreddit VARCHAR(30),
            subreddit_id VARCHAR(9),
            absolute_position INTEGER[],
            nreplies INT,
            thread_begin_timestamp TIMESTAMP,
            scrape_mode VARCHAR(10),--'sub|minimal'
            timestamp TIMESTAMP
            );""" % self.schema)
        self.make_subreddit_table(unique_ids)
        print 'made comments table'
        self.usertable = '%s.users' % self.schema
        self.threadtable = '%s.threads' % self.schema
        self.commenttable = '%s.comments'% self.schema
        self.create_moderator_table()
        self.commit()
        print 'committed initial config'
        #create indexes
        #hold off for now 
        
    def execute(self,*args,**kwargs):
        return self.cur.execute(*args, **kwargs)

    def insert_user(self, data):
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.users(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)
        self.commit()

    def get_user_update_time(self, user_id):
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
            if (now - update_time).seconds < 3600*24 * opts.user_delay:
                return False
            return True
        
    def get_thread_update_time(self, thread_id):
        try:
            self.execute(("SELECT max(timestamp) FROM %s.threads WHERE" % self.schema) +\
                         " id=%s AND scrape_mode='thread'",[thread_id])
            return self.fetchall()[0][0]
        except:
            print sys.exc_info()
            return None

    def get_subreddit_update_time(self, subreddit_text):
        try:
            self.execute(("SELECT max(timestamp) FROM %s.subreddits WHERE" % self.schema) +\
                         " subreddit=%s",[subreddit_text])
            return self.fetchall()[0][0]
        except:
            print sys.exc_info()
            return None

    def check_subreddit_update_time(self, subreddit_text, opts):
        update_time = self.get_user_update_time(subreddit_text)
        if not update_time:
            return True
        elif opts.subreddit_delay == -1:
            return False
        else:
            now = datetime.datetime.now(pytz.utc)
            update_time = pytz.utc.localize(update_time)
            if (now - update_time).seconds < 3600*24 * opts.subreddit_delay:
                return False
            return True

    def update_user(self, data):
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('UPDATE %s' % self.schema) + '.users SET ' + make_update_template(values) + \
                    ' WHERE id=\'%s\'' % data['id']
        self.execute(statement, make_update_data(cols, values))


    def insert_thread(self, data):
        #print data
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.threads(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)

    def update_thread(self, data):
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('UPDATE %s' % self.schema) + '.threads SET ' + make_update_template(values) + \
                    ' WHERE id=\'%s\'' % data['id']
        self.execute(statement, make_update_data(cols, values))


    def insert_comment(self, data):
        #print data
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('INSERT INTO %s' % self.schema) + '.comments(%s) values %s;'
        parsed_statement = self.cur.mogrify(statement, (AsIs(','.join(cols)), tuple(values)))
        self.execute(parsed_statement)

    def update_comment(self, data):
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


    def commit(self):
        self.conn.commit()
        return True
    
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
            print 'dropped %s' % table
        self.execute("DROP INDEX IF EXISTS %s.user_name_index;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.log;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.subreddits;" % self.schema)
        self.execute("DROP TABLE IF EXISTS %s.moderators;" % self.schema)
        if not exclude_schema:
            self.execute("DROP SCHEMA %s;" % self.schema)
            print 'dropped schema %s' % self.schema
        self.commit()

    def make_subreddit_table(self, unique_ids):
        if unique_ids.get('subreddits',True):
            self.execute("""CREATE TABLE IF NOT EXISTS %s.subreddits(
            subreddit VARCHAR(30) PRIMARY KEY,
            accounts_active INTEGER,
            created TIMESTAMP,
            description TEXT,
            rules JSONb,
            submit_text TEXT,
            submit_link_label TEXT,
            subreddit_type VARCHAR(16),
            subscribers INTEGER,
            title TEXT,
            timestamp TIMESTAMP
            ); """ % self.schema)
        else:
            self.execute("""CREATE TABLE IF NOT EXISTS %s.subreddits(
            subreddit VARCHAR(30),
            accounts_active INTEGER,
            created TIMESTAMP,
            description TEXT,
            rules JSONb,
            submit_text TEXT,
            submit_link_label TEXT,
            subreddit_type VARCHAR(16),
            subscribers INTEGER,
            title TEXT,
            timestamp TIMESTAMP
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
        print 'made log entry'

    def update_log_entry(self, opts, reason, notes=None):
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
        self.execute(statement, update_data + (opts.start_time,) )
        self.commit()
        print 'updated log'

    def create_moderator_table(self):
        #this table does not use primary keys
        self.execute(("""CREATE TABLE IF NOT EXISTS %s.moderators""" % self.schema) +
                     """(subreddit VARCHAR(30),
                     username VARCHAR(30),
                     timestamp TIMESTAMP,
                     pos INTEGER)""")

def make_update_data(cols, values):
    d1 =  ((AsIs(col), val) for col, val in zip(cols, values) if val is not None)
    merged = tuple(itertools.chain.from_iterable(d1))
    return merged

def make_update_template(values):
    return ','.join(['%s=%s' for v in values if v is not None])
