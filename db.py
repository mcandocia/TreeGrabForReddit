import psycopg2
from psycopg2.extensions import AsIs
from dbinfo import *
import itertools

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
                 unique_ids={'comments':True,'users':True,'threads':True}):
        #initialize connection
        self.conn = psycopg2.connect(database=database, user=username, host=host, port=port,
                                     password=password)
        self.cur = self.conn.cursor()
        self.schema=name
        print 'started connection'
        #make schema and tables
        print """CREATE SCHEMA IF NOT EXISTS %s;""" % self.schema
        self.cur.execute("""CREATE SCHEMA IF NOT EXISTS %s;""" % self.schema)
        print 'made schema'
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
            scrape_mode VARCHAR(10),--'thread|list|profile|minimal'
            timestamp TIMESTAMP
            );""" % self.schema)
        print 'made threads table'
        if unique_ids.get('users',True):
            #main info
            self.cur.execute("""CREATE TABLE IF NOT EXISTS %s.users(
            username VARCHAR(30),
            id VARCHAR(6) PRIMARY KEY,
            comment_karma INT,
            post_karma INT,
            is_mod BOOLEAN,
            account_created TIMESTAMP,
            is_gold BOOLEAN,
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
            timestamp TIMESTAMP
            );""" % self.schema)
            '''self.execute("""CREATE INDEX IF NOT EXISTS %s.%s ON %s.users 
            USING id TABLESPACE %s;""" % 
            (self.schema,'user_id_index',self.schema, tablespace))'''
            self.execute("""CREATE INDEX IF NOT EXISTS %s.%s ON %s.users 
            USING username TABLESPACE %s;""" % (self.schema,'user_name_index',
                                                self.schema, tablespace))
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
        print 'made comments table'
        self.usertable = '%s.users' % self.schema
        self.threadtable = '%s.threads' % self.schema
        self.commenttable = '%s.comments'% self.schema
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
                         "id=%s",user_id)
            return self.fetchall()[0][0]
        except:
            return None

    def update_user(self, data):
        cols = data.keys()
        values = [data[key] for key in cols]
        statement = ('UPDATE %s' % self.schema) + '.users SET ' + make_update_template(values) + \
                    ' WHERE id=%s' % data['id']
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
                    ' WHERE id=%s' % data['id']
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
                    ' WHERE id=%s' % data['id']
        print self.cur.mogrify(statement, values)
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
        if not exclude_schema:
            self.execute("DROP SCHEMA %s;" % self.schema)
            print 'dropped schema %s' % self.schema
        self.commit()

def make_update_data(cols, values):
    d1 =  ((col, val) for col, val in zip(cols, values) if val is not None)
    merged = tuple(itertools.chain.from_iterable(d1))
    return merged

def make_update_template(values):
    return ','.join(['%s=%s' for v in values if v is not None])
