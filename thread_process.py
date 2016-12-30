from navigator import Navigator
import datetime
from praw_object_data import retry_if_broken_connection

@retry_if_broken_connection
def process_thread(thread_id, opts, reddit_scraper):
        thread = reddit_scraper.submission(id=thread_id)
        print thread_id#keep until certain bug issue is gone
        start = datetime.datetime.now()
        print '+------------------------------------------------------+'
        print 'PROCESSING %s, id=%s, in /r/%s' % (thread.title, thread.id,
                                                  thread.subreddit.display_name)
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
