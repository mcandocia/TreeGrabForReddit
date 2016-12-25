import os
import sys

#opts relevant parameters:
#user_delay - number of days before a user should be rescraped; if -1 never update/overwrite
#thread_delay - number of days before a thread is rescraped; if -1 never update/overwrite
#history - can contain 'threads',  'users', and 'comments'; if present, append data to database
#          if id is present and enough time has passed; if not present, just make sure that
#          there is valid time between them; comments may not be overwritten unless the data is
#          in thread mode (which overrides time delays); they may be appended regardless

seconds_to_days = 1./(3600 * 24)

def write_user(data, opts):
    history_mode = 'users' in opts.history
    db = opts.db
    #check if user in database
    #print data
    try:
        db.execute('SELECT max(timestamp) FROM %s.users WHERE id=%%s' % db.schema,[data['id']])
        last_time = db.fetchall()[0][0]
        if last_time is None:
            db.insert_user(data)
        elif opts.user_delay == -1 or ((data['timestamp'] - last_time).seconds*seconds_to_days < \
                                     opts.user_delay):
            print 'already have %s in database' % data['username']
            return False
        else:
            can_update = True
            if history_mode:
                db.insert_user(data)
            else:
                db.update_user(data)
    except:
        #print sys.exc_info()
        #print data
        db.insert_user(data)
    db.commit()
    return True
        
def write_thread(data, opts):
    history_mode = 'threads' in opts.history
    db = opts.db
        #check if user in database
    try:
        db.execute('SELECT max(timestamp) FROM %s.threads WHERE id=%%s' % db.schema, [data['id']])
        last_time = db.fetchall()[0][0]
        #last_time = None will trigger error
        fails_time = (data['timestamp'] - last_time).seconds*seconds_to_days < opts.thread_delay
        if opts.thread_delay == -1 or fails_time:
            return False
        else:
            if history_mode:
                db.insert_thread(data)
            elif data['scrape_mode'] == 'thread':
                db.update_thread(data)
            else:
                return False
    except:
        #print sys.exc_info()
        db.insert_thread(data)
    db.commit()
    return True
    
def write_comment(data, opts):
    history_mode = 'comments' in opts.history
    db = opts.db
        #check if user in database
    try:
        query = 'SELECT max(timestamp) FROM %s.comments WHERE id=%%s' % db.schema
        #print query
        db.execute(query, [data['id']])
        #print 'executed query'
        last_time = db.fetchall()[0][0]
        #last_time = None will trigger error
        meets_time_cutoff = (data['timestamp'] - last_time).seconds*seconds_to_days < \
                            opts.thread_delay
        updateable = opts.thread_delay <> -1
        thread_mode = data['scrape_mode'] == 'thread'
        if (not (thread_mode or history_mode)) or \
           ((not thread_mode) and meets_time_cutoff and updateable):
            return False
        else:
            can_update = True
            if history_mode:
                db.insert_comment(data)
            else:
                db.update_comment(data)
    except:
        #print data
        #print sys.exc_info()
        db.insert_comment(data)
    db.commit()
    return True
