import sys
import praw_object_data as pod
import writer

import datetime
import pytz

from prawcore.exceptions import NotFound
from prawcore.exceptions import RequestException

from praw_object_data import localize

@pod.retry_if_broken_connection
def scrape_user(username, opts, scraper, force_read=False):
    sys.stdout.write('extracting information for %s...' % username)
    sys.stdout.flush()
    try:
        user = scraper.redditor(username)
        user_id = user.id
    except (NotFound, RequestException, AttributeError):
        print 'user %s is not a valid user' % username
        return 1
    
    previous_time = localize(opts.db.get_user_update_time(user_id))
    if previous_time is None or force_read:
        data = pod.get_user_data(user, opts, mode='user')
    elif float((datetime.datetime.now(pytz.utc) - previous_time).\
               seconds)/(3600.*24) > opts.user_delay:
        data = pod.get_user_data(user, opts, mode='user')
    else:
        print '%s is too recently in database' % username
        return 2
    comments = data['commentdata']
    threads = data['threaddata']
    userdata = data['userdata']
    for key, value in comments.iteritems():
        writer.write_comment(value, opts)
        if opts.deepuser:
            thread_id = value['thread_id']
            if thread_id is not None:
                opts.ids.append(thread_id)
                opts.deepcounter+=1
            else:
                print 'check %s\'s comment on comment %s for id issues' % (username, value['text'])
    for key, value in threads.iteritems():
        writer.write_thread(value, opts)
        if opts.deepuser:
            thread_id = value['id']
            opts.ids.append(thread_id)
            opts.deepcounter+=1
    writer.write_user(userdata, opts)
    sys.stdout.write('complete!\n')
    sys.stdout.flush()
        
