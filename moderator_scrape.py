from praw_object_data import retry_if_broken_connection, get_user_data
import pytz
import datetime
import writer

def scrape_moderators(opts, scraper):
    target_subreddits = get_target_subreddits(opts)
    if opts.use_subreddit_table_for_moderators:
        target_subreddits.extend(get_subreddit_table_subreddits(opts))
    print 'getting moderators from %s total subreddits' % len(target_subreddits)
    opts.master_moderator_set = set()
    for subreddit in target_subreddits:
        scrape_subreddit_for_moderators(subreddit, opts, scraper)

def get_subreddit_table_subreddits(opts):
    db = opts.db
    db.execute("""SELECT DISTINCT subreddit FROM %s.subreddits""" % db.schema)
    return [x[0] for x in db.fetchall()]

def get_target_subreddits(opts):
    db = opts.db
    if not opts.moderators_all:
        if opts.repeat_moderator_subreddits:
            target_subreddits = opts.subreddits
            return target_subreddits
        else:
            db.execute("""SELECT subreddit FROM 
            (SELECT unnest(%%s) AS subreddit) t1
            WHERE subreddit NOT IN (SELECT DISTINCT subreddit FROM %s.moderators)
            GROUP BY subreddit
            """ % (db.schema,), [opts.subreddits,])
    else:
        if opts.repeat_moderator_subreddits:
            db.execute("""SELECT subreddit FROM 
            (SELECT subreddit, count(*) FROM 
            (SELECT subreddit FROM %s.threads 
            UNION ALL
            SELECT subreddit FROM %s.comments) t1
            GROUP BY subreddit 
            ORDER BY count DESC
            )""" % ( db.schema, db.schema))
        else:
            db.execute("""SELECT subreddit FROM 
            (SELECT subreddit, count(*) FROM 
            (SELECT subreddit FROM %s.threads 
            UNION ALL
            SELECT subreddit FROM %s.comments) t1 
            WHERE subreddit NOT IN (SELECT DISTINCT subreddit FROM %s.moderators) 
            GROUP BY subreddit
            ORDER BY count DESC
            ) t2""" % ( db.schema, db.schema, db.schema))
        
    return [x[0] for x in db.fetchall()]

@retry_if_broken_connection
def scrape_subreddit_for_moderators(subreddit, opts, scraper):
    print subreddit
    sub = scraper.subreddit(subreddit)
    mods = [x for x in sub.moderator()]
    #in case user missed capitalization
    subreddit_proper_name = sub.display_name
    print '+-----------------------------+'
    print 'getting data for /r/%s' % subreddit_proper_name
    current_time = datetime.datetime.now()
    for i, mod in enumerate(mods):
        if opts.verbose:
            print 'getting data for /u/%s' % str(mod)
        opts.db.execute("""INSERT INTO %s.moderators(subreddit, username, timestamp, pos)
        VALUES (%%s, %%s, %%s, %%s);""" % opts.db.schema, (subreddit_proper_name,
                                                           str(mod),
                                                           current_time,
                                                           i)
        )
        if opts.scrape_moderators:
            if str(mod) in opts.master_moderator_set:
                continue
            data = get_user_data(mod, opts, 'minimal')
            writer.write_user(data['userdata'], opts)
            for key, value in data['commentdata'].iteritems():
                writer.write_comment(value, opts)
            for key, value in data['threaddata'].iteritems():
                writer.write_thread(value, opts)
            #print 'wrote data for %s' % str(mod)
            opts.master_moderator_set.add(str(mod))
    opts.db.commit()
    print 'got moderators for /r/%s' % subreddit_proper_name
    return True
        
        
