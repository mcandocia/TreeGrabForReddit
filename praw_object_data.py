from __future__ import print_function

from datetime import datetime, timedelta
import time
import sys
import re
from collections import Counter
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

from award_process import process_award_data

import json
from functools import wraps
import time

import pytz

def remove_null(x):
    if x is None:
        return None
    else:
        return x.replace(chr(0), ' <NULL_CHARACTER_REPLACEMENT> ')

def ts_to_utc(timestamp):
    try:
        return datetime.utcfromtimestamp(timestamp.timestamp())
    except AttributeError:
        return datetime.utcfromtimestamp(time.mktime(timestamp.timetuple()) + timestamp.microsecond/1e6)

def search_for_subreddits(text):
    return [x[0] for x in re.findall(r'/r/([\w\-]+)(?=([^\w\-]|$))',text)]

def trophy_ts_process(ts, utc=False):
    if ts is None:
        return None
    if utc:
        return datetime.utcfromtimestamp(ts)
    else:
        return datetime.fromtimestamp(ts)

def localize(obj):
    if obj is None:
        return None
    else:
        return pytz.utc.localize(obj)

def check_time(init_time, time_delta):
    if datetime.now() > init_time + timedelta(hours = time_delta):
        return True
    return False

#update this over time with appropriate errors; monitor while it's relaxed
def retry_if_broken_connection(f):
    @wraps(f)
    def func(*args, **kwargs):
        while True:
            try:
                #timer check
                for arg in args:
                    #hasattr() on submissions is very slow
                    if isinstance(arg, (Submission, Comment)):
                        continue
                    #will only ever apply to options class
                    try:
                        if hasattr(arg, 'init_time'):
                            if check_time(arg.init_time, arg.timer):
                                if arg.log:
                                    arg.db.update_log_entry(arg, 'Timer')
                                sys.exit()
                    except NotFound:
                        print('Encountered NotFound error when extracting attributes. This will be handled later.')
                        continue
                        
                return f(*args, **kwargs)
            except(InternalError, ProgrammingError) as e:
                print(sys.exc_info())
                raise e
            except(RequestException, ServerError):
                print(sys.exc_info())
                print('sleeping...')
                time.sleep(10)

    return func

@retry_if_broken_connection
def get_user_data(user, opts, mode='thread'):
    gilded_comments = []
    gilded_submissions = []
    thread_awards = {}
    comment_awards = {}
    trophy_data = []
    try:
        timestamp = datetime.now()
        data = {'username':user.name,
                'id':user.id,
                'comment_karma':user.comment_karma,
                'post_karma':user.link_karma,
                'is_mod':user.is_mod,
                # yeah, the naming convention doesn't make sense to me, either
                'account_created':datetime.fromtimestamp(user.created_utc),
                'account_created_utc':datetime.utcfromtimestamp(user.created_utc),                
                'is_gold':user.is_gold,
                'timestamp':timestamp,
                'timestamp_utc': ts_to_utc(timestamp),
                'is_employee': user.is_employee,
                'awarder_karma': user.awarder_karma,
                'awardee_karma': user.awardee_karma,
                'verified': user.verified}
        if opts.user_gildings:
            if opts.verbose:
                print('getting gildings')
            try:
                gildings = list(user.gilded())
                comment_gildings = Counter()
                gilded_comments = [
                    e for e in gildings
                    if isinstance(e, Comment)
                ]
                
                submission_gildings = Counter()
                gilded_submissions = [
                    e for e in gildings
                    if isinstance(e, Submission)
                ]

                [comment_gildings.update(c.gildings) for c in gilded_comments]
                [submission_gildings.update(s.gildings) for s in gilded_submissions]
                gilding_data = {

                    'gilded_visible': True,
                    'submissions_silver': submission_gildings.get('gid_1', 0),
                    'submissions_gold': submission_gildings.get('gid_2', 0),
                    'submissions_platinum': submission_gildings.get('gid_3', 0),
                    'comments_silver': comment_gildings.get('gid_1', 0),
                    'comments_gold': comment_gildings.get('gid_2', 0),
                    'comments_platinum': comment_gildings.get('gid_3', 0),
                }
                
            except Exception as e:
                print(e)
                gilding_data = {'gilded_visible': False}

            data.update(gilding_data)

        if opts.trophies:
            try:
                #print(user.name)                
                #print(dir(user))
                user_trophies = user.trophies()
                
                trophy_data.extend([
                    [
                        t.name,
                        user.name,
                        trophy_ts_process(t.granted_at),
                        trophy_ts_process(t.granted_at, utc=True)

                    ]
                    for t in user_trophies
                ]
                )
                if len(user_trophies) == 0 and opts.verbose:
                    print('No Trophies found for %s' % user.name)
                data['_trophies'] = trophy_data

            except Exception as e:
                print('Could not get trophies for user %s' % user.name)

        if opts.awards:
            # not able to do anything at this point in time
            if opts.verbose:
                print('Skipping user-awards until API feature implemented')

        #get comment history
        comment_history = user.comments.new(limit=opts.user_comment_limit)
        comments = {}

        try:
            for i, comment in enumerate(comment_history):
                comments.update(get_comment_data(comment, opts, mode='minimal',
                                                 author_id = data['id']))
                if opts.awards and False:
                    award_data = comment.all_awardings
                    processed_award_data = process_award_data(award_data, key={'comment_id': str(comment.id)})
                    simple_award_data = processed_award_data['simple_data']
                    complete_award_data = processed_award_data['complete_data']
                    if any([x not in opts.db.documented_awards for x in complete_award_data.keys()]):
                        opts.db.update_documented_awards(
                            {k:v for k, v in complete_award_data.items() if k not in opts.db.documented_awards}
                        )
                    comment_awards[comment.id] = award_data
        except Forbidden:
            print(user.name)
            print('forbidden comment history for some reason')

            
        #get submission history
        post_history = user.submissions.new(limit=opts.user_thread_limit)
        threads = {}


        try:
            for i, post in enumerate(post_history):
                threads.update(get_thread_data(post, opts, mode='minimal'))
                if opts.awards and False:
                    award_data = post.all_awardings
                    processed_award_data = process_award_data(award_data, key={'thread_id': str(post.id)})
                    simple_award_data = processed_award_data['simple_data']
                    complete_award_data = processed_award_data['complete_data']
                    if any([x not in opts.db.documented_awards for x in complete_award_data.keys()]):
                        opts.db.update_documented_awards(
                            {k:v for k, v in complete_award_data.items() if k not in opts.db.documented_awards}
                        )
                    thread_awards[post.id] = award_data                
        except Forbidden:
            print(user.name)
            print('forbidden post history for some reason')

        if opts.scrape_gilded and opts.user_gildings:
            for i, post in enumerate(gilded_submissions):
                if post.id not in threads:
                    td = get_thread_data(post, opts, mode='gildings')
                    threads.update(td)
            for i, comment in enumerate(gilded_comments):
                if comment.id not in comments:
                    cd = get_comment_data(comment, opts, mode='gildings')
                    comments.update(cd)
            
    except (NotFound, AttributeError) as err:
        # 
        extra_data = {}
        if isinstance(err, NotFound):
            print('%s is shadowbanned' % str(user))
            extra_data['shadowbanned_by_date'] = datetime.now()
        elif isinstance(err, AttributeError):
            print(err)
            extra_data['suspended_by_date'] = datetime.now()
            print('%s is suspended' % str(user))
            
        comments = {}
        threads = {}
        timestamp = datetime.now()
        data = {'username':user.name,
                'id':None,
                'comment_karma':None,
                'post_karma':None,
                'is_mod':None,
                'account_created':None,
                'account_created_utc': None,
                'is_gold':None,
                'timestamp':timestamp,
                'timestamp_utc': ts_to_utc(timestamp)}
        data.update(extra_data)
        
    return {
        'userdata':data,
        'commentdata':comments,
        'threaddata':threads,
        #'thread_awards': thread_awards,
        #'comment_awards': comment_awards,
    }

@retry_if_broken_connection
def get_thread_data(thread, opts, mode='minimal'):
    """thread comment data must be retrieved via Navigator's navigate() method"""
    edited = thread.edited
    if not edited:
        edited = None
        edited_utc = None
    else:
        edited = datetime.fromtimestamp(edited)
        edited_utc = ts_to_utc(edited)
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

    timestamp = datetime.now()

    thread_awards = {}

    if opts.awards and True:
        award_data = thread.all_awardings
        processed_award_data = process_award_data(award_data, key={'thread_id': str(thread.id)})
        simple_award_data = processed_award_data['simple_data']
        complete_award_data = processed_award_data['complete_data']
        if any([x not in opts.db.documented_awards for x in complete_award_data.keys()]):
            opts.db.update_documented_awards(
                {k:v for k, v in complete_award_data.items() if k not in opts.db.documented_awards}
            )
        thread_awards = simple_award_data

    if opts.count_duplicates:
        num_duplicates = thread.num_duplicates
    else:
        num_duplicates = None
        
    data = {'id':thread.id,
            'title':remove_null(thread.title),
            'subreddit':thread.subreddit.display_name,
            'subreddit_id':subreddit_id,
            'created':datetime.fromtimestamp(thread.created_utc),
            'created_utc':datetime.utcfromtimestamp(thread.created_utc),            
            'edited':edited,
            'edited_utc': edited_utc,
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
            'silver': thread.gildings.get('gid_1', 0),
            'platinum': thread.gildings.get('gid_3', 0),
            'domain':thread.domain,
            'is_self':thread.is_self,
            'is_stickied':thread.stickied,
            'url':thread.url,
            'self_text':remove_null(thread.selftext),
            'self_text_html':remove_null(thread.selftext_html),
            'num_comments':thread.num_comments,
            'over_18':thread.over_18,
            'permalink':thread.permalink,
            'comments_navigated':None,
            'comments_deleted':None,
            'scrape_mode':mode,
            'timestamp':timestamp,
            'timestamp_utc': ts_to_utc(timestamp),
            # boolean update columns
            'is_video': thread.is_video,
            'is_original_content': thread.is_original_content,
            'is_reddit_media_domain': thread.is_reddit_media_domain,
            'is_robot_indexable': thread.is_robot_indexable,
            'is_meta': thread.is_meta,
            'is_crosspostable': thread.is_crosspostable,
            'locked': thread.locked,
            'archived': thread.archived,
            'contest_mode': thread.contest_mode,
            'num_duplicates': num_duplicates,
            'num_crossposts': thread.num_crossposts,
            'author_flair_background_color': thread.author_flair_background_color,
            'link_flair_background_color': thread.link_flair_background_color,
            'author_flair_text_color': thread.author_flair_text_color,
            'link_flair_text_color': thread.link_flair_text_color,            
            
            '_award_data': thread_awards,
    }
    return {thread.id:data}

@retry_if_broken_connection
def get_comment_data(comment, opts, mode='minimal', author_id=None, scraper=None):
    #print('getting data for comment id %s' % comment.id)
    #data that requires pre-processing
    comment_awards = {}
    try:
        edited = comment.edited
        if not edited:
            edited = None
            edited_utc = None
        else:
            edited = datetime.fromtimestamp(edited)
            edited_utc = ts_to_utc(edited)
        #main processing
        #for deleted comments
        author = comment.author
        if author is None and mode != 'direct':
            author_id = None
            author_name = None
        elif mode == 'direct':
            author_id = None
            try:
                author_name = author.name
            except AttributeError:
                author_name = None
        else:
            author_id = author_id # author.id slows down the code gathering too much; join later
            try:
                author_name = author.name
            except AttributeError:
                author_name = None
        #too costly to grab this
        # somehow null bytes can find their way into reddit comments
        def remove_null(x):
            return x.replace(chr(0), ' <NULL_CHARACTER_REPLACEMENT> ')
        now = datetime.now()
                             
        subreddit_id = None
        
        if opts.awards and True:
            award_data = comment.all_awardings
            processed_award_data = process_award_data(award_data, key={'comment_id': str(comment.id)})
            simple_award_data = processed_award_data['simple_data']
            complete_award_data = processed_award_data['complete_data']
            if any([x not in opts.db.documented_awards for x in complete_award_data.keys()]):
                opts.db.update_documented_awards(
                    {k:v for k, v in complete_award_data.items() if k not in opts.db.documented_awards}
                )
            comment_awards = simple_award_data
            
        data = {'id':comment.id,
                'author_name':author_name,
                'author_id':author_id,
                'parent_id':comment.parent_id,
                'is_root':comment.is_root,
                'text':remove_null(comment.body),
                'created':datetime.fromtimestamp(comment.created_utc),
                'created_utc':datetime.utcfromtimestamp(comment.created_utc),
                'edited':edited,
                'edited_utc': edited_utc,
                'gold':comment.gilded,
                'silver': comment.gildings.get('gid_1', 0),
                'platinum': comment.gildings.get('gid_3', 0),
                'is_stickied': comment.stickied,
                'score':comment.score,
                'is_distinguished':comment.distinguished is not None,
                'thread_id':comment.link_id[3:],
                'subreddit':comment.subreddit.display_name,
                'subreddit_id':subreddit_id,
                'absolute_position':None,
                'nreplies':None,
                'thread_begin_timestamp':None,
                'scrape_mode':mode,
                'timestamp':now,
                'timestamp_utc': ts_to_utc(now),
                'controversiality': comment.controversiality,
                'author_flair_background_color': comment.author_flair_background_color,
                'author_flair_text_color': comment.author_flair_text_color,
                'author_flair_text': comment.author_flair_text,
                'author_flair_css_class': comment.author_flair_css_class,
                'is_locked': comment.locked,
                'is_submitter': comment.is_submitter,

            '_award_data': comment_awards,                
        }
    except NotFound:
        print('comment deleted before cacheable (shouldn\'t happen)')
        return {}
    return {comment.id:data}

#related options
#'scrape_related_subreddits','scrape_wikis',
#                     'scrape_traffic'
#related_subreddit_recursion_depth
@retry_if_broken_connection
def get_subreddit_data(subreddit, opts, recursion_depth=0):
    print('attempting to get subreddit data')
    timestamp = datetime.now()
    try:
        data = {
            'subreddit':subreddit.display_name,
            'accounts_active':subreddit.accounts_active,
            'created':datetime.fromtimestamp(subreddit.created_utc),
            'created_utc':datetime.utcfromtimestamp(subreddit.created_utc),
            'description':subreddit.description,
            'has_wiki':subreddit.wiki_enabled,
            'public_traffic':subreddit.public_traffic,
            'rules':json.dumps(subreddit.rules()['rules']),
            'submit_text':subreddit.submit_text,
            'submit_link_label':subreddit.submit_link_label,
            'subreddit_type':subreddit.subreddit_type,
            'subscribers':subreddit.subscribers,
            'title':subreddit.title,
            'timestamp':timestamp,
            'timestamp_utc': ts_to_utc(timestamp),
            }
    except NotFound:
        print(sys.exc_info())
        print('subreddit data not found')
        return {}
    #except ServerError:
    #print(sys.exc_info())
    #print('server error!')
    #raise ServerError
    try:
        opts.scraped_subreddits.add(subreddit.display_name.lower())
    except Exception as e:
        print(e)
        return {}
    if opts.scrape_related_subreddits:
        if opts.verbose:
            print('getting related subreddits')
        related_subreddits_data = get_related_subreddits(subreddit, opts, timestamp)
    else:
        related_subreddits_data = None
    if opts.scrape_wikis and data['has_wiki']:
        if opts.verbose:
            print('getting wiki data')
        wiki_data = get_wiki_data(subreddit, opts, related_subreddits_data, timestamp)
    else:
        wiki_data = None
    if opts.scrape_traffic and data['public_traffic']:
        if opts.verbose:
            print('getting traffic data')
        traffic_data = get_traffic_data(subreddit, opts, timestamp)
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
                    'timestamp':timestamp,
                    'timestamp_utc': ts_to_utc(timestamp)} for x in day_data]
    hour_traffic = [{'time':datetime.fromtimestamp(x[0]),
                    'period_type':'hour',
                    'subreddit':subreddit.display_name,
                    'unique_visits':x[1],
                    'total_visits':x[2],
                     'timestamp':timestamp,
                     'timestamp_utc': ts_to_utc(timestamp)} for x in hour_data]

    return day_traffic + hour_traffic

@retry_if_broken_connection
def get_related_subreddits(subreddit, opts, timestamp):
    description = subreddit.description
    subreddits = search_for_subreddits(subreddit.description)
    #validation will update these entries later
    return [{'subreddit':subreddit.display_name,
             'related_subreddit':sub.lower(),
             'relationship_type':'sidebar',
             'timestamp':timestamp,
             'timestamp_utc': ts_to_utc(timestamp)} for sub in subreddits]

@retry_if_broken_connection
def get_wiki_data(subreddit, opts, related_subreddits, timestamp):
    if opts.verbose:
        print('loading subreddit wiki')
    wikis = subreddit.wiki
    if opts.verbose:
        print('getting content from wikis')
    wikis = list(wikis)
    if len(wikis) > opts.max_wiki_size:
        print('SKIPPING WIKI BECAUSE LENGTH IS %s' % len(wikis))
        return []
    wiki_text_list = [get_content_or_blank(w) for w in wikis]
    wiki_name = [w.name for w in wikis]
    wiki_data = []
    for name, text in zip(wiki_name, wiki_text_list):
        #print('on wiki %s' % repr(wiki_name))
        if len(text)==0:
            continue
        wiki_data.append({'subreddit':subreddit.display_name,
                          'content':text,
                          'name':name,
                          'timestamp':timestamp,
                          'timestamp_utc': ts_to_utc(timestamp)})
        if opts.scrape_related_subreddits:
            if opts.verbose:
                print('searching for related subreddits')
            subreddits = search_for_subreddits(subreddit.description)
            new_data = [{'subreddit':subreddit.display_name,
                         'related_subreddit':sub.lower(),
                         'relationship_type':'wiki',
                         'wiki_name':name,
                         'timestamp':timestamp,
                         'timestamp_utc': ts_to_utc(timestamp)} for sub in subreddits]
            related_subreddits.extend(new_data)
        return wiki_data
            
@retry_if_broken_connection
def get_content_or_blank(wiki):
    if wiki is None:
        print('no wiki')
        return ''
    else:
        try:
            return wiki.content_md
        except TypeError:
            print('typeerror encountered in wiki')
            return ''
        except Exception as e:
            print(e)
            raise e
            

    
