from datetime import datetime
import time
import sys
from psycopg2 import InternalError, ProgrammingError
from prawcore.exceptions import NotFound

#update this over time with appropriate errors; monitor while it's relaxed
def retry_if_broken_connection(f):
    def func(*args, **kwargs):
        while True:
            try:
                return f(*args, **kwargs)
            except InternalError, ProgrammingError:
                print sys.exc_info()
                raise
            except TypeError:
                print sys.exc_info()
                print 'sleeping...'
                time.sleep(10)
    return func

@retry_if_broken_connection
def get_user_data(user, opts, mode='thread'):
    try:
        data = {'username':user.name,
                'id':user.id,
                'comment_karma':user.comment_karma,
                'post_karma':user.link_karma,
                'is_mod':user.is_mod,
                'account_created':datetime.fromtimestamp(user.created_utc),
                'is_gold':user.is_gold,
                'timestamp':datetime.now()}
        #get comment history
        comment_history = user.comments.new(limit=opts.user_comment_limit)
        comments = {}
        for comment in comment_history:
            comments.update(get_comment_data(comment, opts, mode='minimal', author_id = data['id']))

        #get submission history
        post_history = user.submissions.new(limit=opts.user_thread_limit)
        threads = {}
        for post in post_history:
            threads.update(get_thread_data(post, opts, mode='minimal'))
    except NotFound:
        print '%s is shadowbanned' % str(user)
        comments = {}
        threads = {}
        data = {'username':user.name,
                'id':None,
                'comment_karma':None,
                'post_karma':None,
                'is_mod':None,
                'account_created':None,
                'is_gold':None,
                'timestamp':datetime.now()}
    return {'userdata':data,
            'commentdata':comments,
            'threaddata':threads}

@retry_if_broken_connection
def get_thread_data(thread, opts, mode='minimal'):
    """thread comment data must be retrieved via Navigator's navigate() method"""
    edited = thread.edited
    if not edited:
        edited = None
    else:
        edited = datetime.fromtimestamp(edited)
    #too expensive to gather this
    subreddit_id = None
    author_id = None
    if mode=='thread' or opts.get_upvote_ratio:
        ratio = thread.upvote_ratio
    else:
        ratio = None
    if thread.author is None:
        author_name = None
    else:
        author_name = thread.author.name
    data = {'id':thread.id,
            'title':thread.title,
            'subreddit':thread.subreddit.display_name,
            'subreddit_id':subreddit_id,
            'created':datetime.fromtimestamp(thread.created_utc),
            'edited':edited,
            'score':thread.score,
            'percentage':ratio,
            'author_name':author_name,
            'author_id':author_id,
            'author_flair':thread.author_flair_text,
            'author_flair_css_class':thread.author_flair_css_class,
            'is_distinguished':thread.distinguished is not None,
            'gold':thread.gilded,
            'is_self':thread.is_self,
            'is_stickied':thread.stickied,
            'url':thread.url,
            'self_text':thread.selftext,
            'self_text_html':thread.selftext_html,
            'num_comments':thread.num_comments,
            'over_18':thread.over_18,
            'permalink':thread.permalink,
            'comments_navigated':None,
            'comments_deleted':None,
            'scrape_mode':mode,
            'timestamp':datetime.now()
            }
    return {thread.id:data}

@retry_if_broken_connection
def get_comment_data(comment, opts, mode='minimal', author_id=None):
    #print 'getting data for comment id %s' % comment.id
    #data that requires pre-processing
    try:
        edited = comment.edited
        if not edited:
            edited = None
        else:
            edited = datetime.fromtimestamp(edited)
        #main processing
        #for deleted comments
        author = comment.author
        if author is None:
            author_id = None
            author_name = None
        else:
            author_id = author_id # author.id slows down the code gathering too much; join later
            author_name = author.name
        #too costly to grab this
        subreddit_id = None
        data = {'id':comment.id,
                'author_name':author_name,
                'author_id':author_id,
                'parent_id':comment.parent_id,
                'is_root':comment.is_root,
                'text':comment.body,
                'created':datetime.fromtimestamp(comment.created_utc),
                'edited':edited,
                'gold':comment.gilded,
                'score':comment.score,
                'is_distinguished':comment.distinguished is not None,
                'thread_id':comment.link_id[3:],
                'subreddit':comment.subreddit.display_name,
                'subreddit_id':subreddit_id,
                'absolute_position':None,
                'nreplies':None,
                'thread_begin_timestamp':None,
                'scrape_mode':mode,
                'timestamp':datetime.now()
                }
    except NotFound:
        print 'comment deleted before cacheable (shouldn\'t happen)'
        return {}
    return {comment.id:data}

