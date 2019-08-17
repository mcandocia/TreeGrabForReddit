from navigator import Navigator
import datetime
from praw_object_data import retry_if_broken_connection

@retry_if_broken_connection
def process_thread(thread_id, opts, reddit_scraper):
        thread = reddit_scraper.submission(id=thread_id)
        if thread.num_comments > opts.max_comments:
                print 'too many comments: %d' % thread.num_comments
                print 'skipping thread id %s' % thread_id
                return 2
        print thread_id#keep until certain bug issue is gone
        start = datetime.datetime.now()
        print '+------------------------------------------------------+'
        try:
                print 'PROCESSING %s, id=%s, in /r/%s' % (unicode(thread.title), thread.id,
                                                  unicode(thread.subreddit.display_name))
        except UnicodeDecodeError:
                print 'PROCESSING id=%s; name cannot be parsed' % thread.id
        print 'created %s' % datetime.datetime.fromtimestamp(thread.created).strftime('%x %X')
        print 'author: %s' % str(thread.author)
        print 'score: %d, num_comments: %d' % (thread.score, thread.num_comments)
        print ''
        nav = Navigator(thread, opts)
        if opts.skip_comments:
            nav.store_thread_data()
        else:
            nav.navigate()
        end = datetime.datetime.now()
        print 'FINISHED thread w/id=%s, navigated %d comments, %d deleted'\
            % (thread.id, nav.traversed_comments, nav.deleted_comments)
        print 'thread scraping time: %d seconds' % (end-start).seconds
        print '+------------------------------------------------------+'
