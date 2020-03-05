from datetime import datetime, timedelta
import pytz
from tzlocal import get_localzone
import time

from user_scrape import scrape_user

def rescrape_users_from_schema(schema, opts, reddit_scraper):
    db = opts.db
    db.execute(
        "SELECT username, timestamp FROM {schema}.users WHERE username NOT IN (SELECT username FROM {name}.users) ORDER BY timestamp".format(
            schema=schema,
            name=opts.name,
        )
    )

    userlist = db.fetchall()
    print(userlist[0])

    age_range = [timedelta(days=x) for x in opts.resampling_age_range]
    print(age_range)

    total_skipped = 0
    print('Current time: %s' % datetime.utcnow())
    for user, timestamp in userlist:
        current_time = datetime.utcnow()
        delta = current_time - timestamp
        if delta > age_range[1]:
            total_skipped += 1
            print('Skipping %a due to timedelta = %s (%d)' % (user, delta, total_skipped))
            continue

        elif delta < age_range[0]:
            sleep_time = (age_range[0] - delta).total_seconds()
            print('Sleeping for %s seconds due to timedelta = %s for user %a (original timestamp=%s)' % (
                sleep_time,
                delta,
                user,
                timestamp
            ))
            time.sleep(sleep_time + 0.5)

        scrape_user(
            user,
            opts,
            reddit_scraper,
            force_read=False
        )

    print('Skipped %d total' % total_skipped)

        
                  
