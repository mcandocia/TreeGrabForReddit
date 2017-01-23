from datetime import datetime, timedelta
import time
import sys
import re
from psycopg2 import InternalError, ProgrammingError
from prawcore.exceptions import NotFound
from prawcore.exceptions import RequestException
from prawcore.exceptions import Forbidden
from prawcore.exceptions import Redirect
from prawcore.exceptions import BadRequest
from prawcore.exceptions import ServerError

from praw.models.reddit.submission import Submission
from praw.models.reddit.comment import Comment
from praw.models.reddit.subreddit import Subreddit

import json

import pytz

def search_for_subreddits(text):
    return [x[0] for x in re.findall(r'/r/([\w\-]+)(?=([^\w\-]|$))',text)]

def localize(obj):
    if obj is None:
        return None
    else:
        return pytz.utc.localize(obj)

def check_time(init_time, time_delta):
    if datetime.datetime.now() > init_time + timedelta(hours = time_delta):
        return True
    return False

#update this over time with appropriate errors; monitor while it's relaxed
def retry_if_broken_connection(f):
    def func(*args, **kwargs):
        while True:
            try:
                #timer check
                for arg in args:
                    #hasattr() on submissions is very slow
                    if isinstance(arg, (Submission, Comment)):
                        continue
                    #will only ever apply to options class
                    if hasattr(arg, 'init_time'):
                        if check_time(arg.init_time, arg.timer):
                            if arg.log:
                                arg.db.update_log_entry(arg, 'Timer')
                            sys.exit()
                return f(*args, **kwargs)
            except InternalError, ProgrammingError:
                print sys.exc_info()
                raise
            except RequestException:
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
        try:
            for i, comment in enumerate(comment_history):
                comments.update(get_comment_data(comment, opts, mode='minimal',
                                                 author_id = data['id']))
        except Forbidden:
            print user.name
            print 'forbidden comment history for some reason'

        #get submission history
        post_history = user.submissions.new(limit=opts.user_thread_limit)
        threads = {}
        try:
            for i, post in enumerate(post_history):
                threads.update(get_thread_data(post, opts, mode='minimal'))
        except Forbidden:
            print user.name
            print 'forbidden post history for some reason'
    except (NotFound, AttributeError) as err:
        if isinstance(err, NotFound):
            print '%s is shadowbanned' % str(user)
        elif isinstance(err, AttributeError):
            print '%s is suspended' % str(user)
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
            'link_flair_text':thread.link_flair_text,
            'link_flair_css_class':thread.link_flair_css_class,
            'is_distinguished':thread.distinguished is not None,
            'is_spoiler':thread.spoiler,
            'gold':thread.gilded,
            'domain':thread.domain,
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

#related options
#'scrape_related_subreddits','scrape_wikis',
#                     'scrape_traffic'
#related_subreddit_recursion_depth
@retry_if_broken_connection
def get_subreddit_data(subreddit, opts, recursion_depth=0):
    now = datetime.now()
    try:
        data = {
            'subreddit':subreddit.display_name,
            'accounts_active':subreddit.accounts_active,
            'created':datetime.fromtimestamp(subreddit.created_utc),
            'description':subreddit.description,
            'has_wiki':subreddit.wiki_enabled,
            'public_traffic':subreddit.public_traffic,
            'rules':json.dumps(subreddit.rules()['rules']),
            'submit_text':subreddit.submit_text,
            'submit_link_label':subreddit.submit_link_label,
            'subreddit_type':subreddit.subreddit_type,
            'subscribers':subreddit.subscribers,
            'title':subreddit.title,
            'timestamp':now
            }
    except NotFound:
        print sys.exc_info()
        print 'subreddit data not found'
        return {}
    except ServerError:
        print sys.exc_info()
        print 'server error!'
        raise ServerError
    if opts.scrape_related_subreddits:
        related_subreddits_data = get_related_subreddits(subreddit, opts, now)
    else:
        related_subreddits_data = None
    if opts.scrape_wikis and data['has_wiki']:
        wiki_data = get_wiki_data(subreddit, opts, related_subreddits_data, now)
    else:
        wiki_data = None
    if opts.scrape_traffic and data['public_traffic']:
        traffic_data = get_traffic_data(subreddit, opts, now)
    else:
        traffic_data = None
    return {'data':data,
            'related_subreddits_data':related_subreddits_data,
            'wiki_data':wiki_data,
            'traffic_data':traffic_data}

@retry_if_broken_connection
def get_traffic_data(subreddit, opts, timestamp):
    traffic = subreddit.traffic()
    day_data = traffic['day']
    hour_data = traffic['hour']

    day_traffic = [{'time':datetime.fromtimestamp(x[0]),
                    'period_type':'day',
                    'subreddit':subreddit.display_name,
                    'unique_visits':x[1],
                    'total_visits':x[2],
                    'timestamp':timestamp} for x in day_data]
    hour_traffic = [{'time':datetime.fromtimestamp(x[0]),
                    'period_type':'hour',
                    'subreddit':subreddit.display_name,
                    'unique_visits':x[1],
                    'total_visits':x[2],
                     'timestamp':timestamp} for x in hour_data]

    return day_traffic + hour_traffic

@retry_if_broken_connection
def get_related_subreddits(subreddit, opts, timestamp):
    description = subreddit.description
    subreddits = search_for_subreddits(subreddit.description)
    #validation will update these entries later
    return [{'subreddit':subreddit.display_name,
             'related_subreddit':sub.lower(),
             'relationship_type':'sidebar',
             'timestamp':timestamp} for sub in subreddits]

@retry_if_broken_connection
def get_wiki_data(subreddit, opts, related_subreddits, timestamp):
    wikis = subreddit.wiki
    wiki_text_list = [get_content_or_blank(w) for w in wikis]
    wiki_name = [w.name for w in wikis]
    wiki_data = []
    for name, text in zip(wiki_name, wiki_text_list):
        if len(text)==0:
            continue
        wiki_data.append({'subreddit':subreddit.display_name,
                          'content':text,
                          'name':name,
                          'timestamp':timestamp})
        if opts.scrape_related_subreddits:
            subreddits = search_for_subreddits(subreddit.description)
            new_data = [{'subreddit':subreddit.display_name,
                         'related_subreddit':sub.lower(),
                         'relationship_type':'wiki',
                         'wiki_name':name,
                         'timestamp':timestamp} for sub in subreddits]
            related_subreddits.extend(new_data)
        return wiki_data
            
@retry_if_broken_connection
def get_content_or_blank(wiki):
    if wiki is None:
        return ''
    else:
        try:
            return wiki.content_md
        except TypeError:
            return ''

    
