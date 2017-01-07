import praw_object_data as pod
from writer import write_subreddit

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
                            min_occurrences)
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
        scrape_subreddit_info(subreddit_text, opts, scraper)
        subreddit_counter += 1
        subreddit_set.add(subreddit_text)
        if subreddit_counter % 50 > 0:
            print 'gone through %s subreddits' % subreddit_counter
    print 'went through %s subreddits' % subreddit_counter
    print 'done scraping subreddits'

@pod.retry_if_broken_connection
def scrape_subreddit_info(text, opts, scraper):
    if not opts.db.check_subreddit_update_time(text, opts):
        if opts.verbose:
            print 'too recently scraped /r/%s' % text
        return False
    if opts.verbose:
        print 'scraping /r/%s' % text
    subreddit = scraper.subreddit(text)
    data = pod.get_subreddit_data(subreddit, opts)
    write_subreddit(data, opts)
    if opts.verbose:
        print 'done with /r/%s' % text
