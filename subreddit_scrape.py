import praw_object_data as pod
from writer import write_subreddit
from writer import write_traffic
from writer import write_related_subreddits
from writer import write_wikis

from praw.models.reddit.subreddit import Subreddit
from prawcore.exceptions import Forbidden
from prawcore.exceptions import NotFound
from prawcore.exceptions import Redirect
from prawcore.exceptions import BadRequest

import sys
import re

#scrape_subreddits
#scrape_subreddits_in_db
#repeat_subreddit_scraping
#n_subreddits_to_scrape
#subreddit_delay

@pod.retry_if_broken_connection
def scrape_subreddits(opts, scraper):
    subreddit_list = []
    if opts.scrape_subreddits:
        subreddit_list.extend(opts.subreddits)
    if opts.rescrape_subreddits:
        opts.db.execute("""SELECT DISTINCT subreddit FROM %s.subreddits;""" % opts.db.schema)
        subreddit_list.extend([x[0] for x in opts.db.fetchall()])
    if opts.scrape_subreddits_in_db:
        min_occurrences = opts.min_occurrences_for_subreddit_in_db
        if min_occurrences <= 1:
            opts.db.execute("""SELECT DISTINCT subreddit FROM %s.comments 
            UNION
            SELECT DISTINCT subreddit FROM %s.threads""" % (opts.db.schema, opts.db.schema))
        else:
            opts.db.execute("""SELECT subreddit FROM (
            SELECT subreddit FROM %s.comments
            UNION ALL
            SELECT subreddit FROM %s.threads) t1
            GROUP BY subreddit HAVING count(*) > %%s;""" % (opts.db.schema, opts.db.schema),
                            [min_occurrences])
        subreddit_list.extend([x[0] for x in opts.db.fetchall()])
    subreddit_counter = 0
    subreddit_set = set()
    n_subreddits = len(subreddit_list)
    while opts.n_subreddits_to_scrape == -1 or subreddit_counter < opts.n_subreddits_to_scrape:
        subreddit_text = subreddit_list[subreddit_counter % n_subreddits]
        if subreddit_text in subreddit_set and not opts.repeat_subreddit_scraping:
            if opts.verbose:
                print 'skipping /r/%s' % subreddit_text
            subreddit_list.pop(subreddit_counter % n_subreddits)
            n_subreddits -= 1
            if len(subreddit_list) == 0:
                break
            continue
        #check subreddit_delay
        try:
            scrape_subreddit_info(subreddit_text, opts, scraper)
        except Forbidden:
            print '/r/%s is no longer available' % subreddit_text
            subreddit_list.pop(subreddit_counter % n_subreddits)
            n_subreddits -=1
            if len(subreddit_list) == 0:
                break
            continue
        subreddit_counter += 1
        subreddit_set.add(subreddit_text)
        if subreddit_counter % 50 == 0:
            print 'gone through %s subreddits out of %s' % (subreddit_counter, n_subreddits)
    print 'went through %s subreddits out of %s' % (subreddit_counter, n_subreddits)
    print 'done scraping subreddits'

@pod.retry_if_broken_connection
def scrape_subreddit_info(text, opts, scraper, recursion_depth=0):
    if isinstance(text, Subreddit):
        subreddit = text
        text = subreddit.display_name
    else:
        subreddit = scraper.subreddit(text)
    if not opts.db.check_subreddit_update_time(text, opts):
        if opts.verbose:
            print 'too recently scraped /r/%s' % text
        return False
    if opts.verbose:
        sys.stdout.write( 'scraping /r/%s\r' % text)
        sys.stdout.flush()
    #if recursion_depth > 1 this should be the case; saves an extra call
    data = pod.get_subreddit_data(subreddit, opts, recursion_depth)
    if 'subreddit' not in data['data']:
        print '/r/%s not found' % text
        return False
    write_subreddit(data['data'], opts)
    #now put in traffic data and wiki data
    if opts.scrape_traffic and data['traffic_data'] is not None:
        write_traffic(data['traffic_data'], opts)
        opts.db.commit()
    if opts.scrape_wikis and data['wiki_data'] is not None:
        write_wikis(data['wiki_data'], opts)
        opts.db.commit()
    #now validate related_subreddits data, which involves recursion
    if opts.scrape_related_subreddits:
        validate_related_subreddits(data['related_subreddits_data'], opts, scraper,
                                    recursion_depth)
        write_related_subreddits(data['related_subreddits_data'], opts)
        print 'wrote related data'
        opts.db.commit()
    if opts.verbose:
        print 'done with /r/%s' % text

#remember:
#subreddit
#related_subreddit (needs validation)
#relationship_type (wiki/sidebar)
#wiki name (if relationship_type==wiki)
#timestamp
#
def validate_related_subreddits(data, opts, scraper, recursion_depth):
    sidebar_set = set()
    wiki_sets = {}
    i = 0
    while i < len(data):
        entry = data[i]
        results = validate_subreddit(entry, opts, scraper)
        #print results
        #process results to determine whether to remove or update set
        if results is None:
            data.pop(i)
        elif results.display_name in sidebar_set and entry['relationship_type']=='sidebar':
            data.pop(i)
        elif entry['relationship_type']=='wiki':
            if results.display_name in wiki_sets.get(entry['wiki_name'],set()):
                data.pop(i)
            else:
                entry['related_subreddit'] = results.display_name
                if results.display_name.lower() not in opts.RELATED_SUBREDDIT_SET:
                    process_subreddit_recursively(results, opts, recursion_depth, scraper)
                if entry['wiki_name'] in wiki_sets:
                    wiki_sets[entry['wiki_name']].add(results.display_name)
                else:
                    wiki_sets[entry['wiki_name']] = set(results.display_name)
                opts.RELATED_SUBREDDIT_SET.add(results.display_name.lower())
                i+=1
        else:
            entry['related_subreddit'] = results.display_name
            if results.display_name not in opts.RELATED_SUBREDDIT_SET:
                process_subreddit_recursively(results, opts, recursion_depth, scraper)
            sidebar_set.add(results.display_name)
            opts.RELATED_SUBREDDIT_SET.add(results.display_name.lower())
            i+=1
            
def process_subreddit_recursively(subreddit, opts, recursion_depth, scraper):
    if recursion_depth < opts.related_subreddit_recursion_depth:
        scrape_subreddit_info(text = subreddit,
                              opts=opts,
                              scraper=scraper,
                              recursion_depth = recursion_depth + 1)
    else:
        return 0

def validate_subreddit(entry, opts, scraper):
    try:
        if entry['related_subreddit'] in opts.RELATED_SUBREDDIT_SET:
            return None
        subreddit = scraper.subreddit(entry['related_subreddit'])
        #need to force update of display_name
        num_subscribers = subreddit.subscribers
        display_name = subreddit.display_name
        return subreddit
    except Forbidden:
        #at this time display_name is not updated, so just ignore it
        return None
    except (NotFound, Redirect):
        return None
    except BadRequest:
        return None
