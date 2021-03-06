## build_profile.py
# Max Candocia - mscandocia@gmail.com
# 08/29/2019
#
# script that allows user to create materialized view & download profile of data
# from two schemas in order to compare statistics

from __future__ import print_function
import argparse
import os
import psycopg2
import db
from color_printing import *

try:
    import regex as re
except:
    import re

def get_options():
    parser = argparse.ArgumentParser(
        description = 'Build a table containing users that were sampled in two different schemas at different times and collect statistics over that period, including top subreddits for comments and links, and top domains, as well as ban/suspend flags/subdivisions'
    )

    parser.add_argument(
        'name',
        help='The view name (schema included) that you want to store '
        'results into.'
    )

    parser.add_argument(
        '--schema-1',
        help='The first schema to reference (earlier)'
    )

    parser.add_argument(
        '--schema-2',
        help='The second schema to reference (later)'
    )

    parser.add_argument(
        '--create-table',
        action='store_true',
        help='Creates a table instead of a materialized view'
    )

    parser.add_argument(
        '--insert-into',
        action='store_true',
        help='Inserts non-existing entries into table instead of updating whole table'
    )


    parser.add_argument(
        '--gildings-1',
        help='Collect gilding information (incl. platinum and silver) for first schema',
        action='store_true',
    )

    parser.add_argument(
        '--gildings-2',
        help='Collect gilding information (incl. platinum and silver) for second schema',
        action='store_true',
    )

    parser.add_argument(
        '--n-top-subreddits-for-comments',
        type=int,
        default=5,
        help='The number of top subreddits to collect for user comment history.'
    )

    parser.add_argument(
        '--n-top-subreddits-for-threads',
        type=int,
        default=5,
        help='The number of top subreddits to collect for user post history.'
    )

    parser.add_argument(
        '--n-top-domains-for-threads',
        type=int,
        default=5,
        help='The number of top domains to collect for user post history'


    )

    parser.add_argument(
        '--strong-negative-thread-score-maximum',
        help='The maximum score a thread should have to be considered "strongly negative". Default is -4.',
        type=int,
        default=-4
    )

    parser.add_argument(
        '--strong-negative-comment-score-maximum',
        help='The maximum score a comment should have to be considered "strongly negative". Default is -4.',
        type=int,
        default=-4
    )

    parser.add_argument(
        '--strong-positive-thread-score-minimum',
        help='The minimum score a thread should have to be considered "strongly positive". Default is 50.',
        type=int,
        default=50,
    )

    parser.add_argument(
        '--strong-positive-comment-score-minimum',
        help='The minimum score a comment should have to be considered "strongly positive". Default is 20.',
        type=int,
        default=20,
    )

    parser.add_argument('--calculate-account-age', action='store_true', help='store account age as variable')

    parser.add_argument(
        '--drop-view',
        action='store_true',
        help='Drops the materialized view at the beginning'
    )

    parser.add_argument(
        '--export',
        help='File to export data to. Not yet implemented.',
        required=False,
    )

    parser.add_argument(
        '--skip-creation',
        help='Do not create any views',
        action='store_true'
    )
    '''
    parser.add_argument(
        '--record-most-recent-activity',
        help='Record timestamps of most recent comment and most recent submission '
        '(store NULL if unavailable, will probably use absurdly low TS value in later processing',
        action='store_true'
    )
    '''


    args = parser.parse_args()

            

    options = vars(args)
    if options['insert_into']:
        try:
            assert options['create_table']
        except AssertionError:
            raise AssertionError('Must have "--create-table" if "--insert-into" is specified')
    

    return options
                        
    

## main 
def main(options):
    cur = db.Database(
        connect_only=True
    )

    if options['drop_view']:
        drop_query = drop_template.format(**options)
        if options['create_table']:
            drop_query = re.sub('MATERIALIZED VIEW', 'TABLE', drop_query)
        dimprint(drop_query)
        bprint('EXECUTING!')
        cur.execute(drop_query)



    if options['gildings_1']:
        gilding_injection_part = gilding_injection
        gilding_injection_user_part = gilding_injection_user
        
        subreddit_comment_part = ' \n'.join([
            subreddit_comment_rank_template_gilded.format(
                n=n,
                user_filter=''
            )
            for n in range(1, options['n_top_subreddits_for_comments']+1)

        ])

        subreddit_thread_part = ' \n'.join([
            subreddit_thread_rank_template_gilded.format(
                n=n,
                user_filter=''
            )
            for n in range(1, options['n_top_subreddits_for_threads']+1)

        ])

        domain_thread_part = ' \n'.join([
            domain_rank_template_gilded.format(
                n=n,
                user_filter=''
            )
            for n in range(1, options['n_top_domains_for_threads']+1)

        ])
    else:
        gilding_injection_part = ' '
        gilding_injection_user_part = ' '
        
        subreddit_comment_part = ' \n'.join([
            subreddit_comment_rank_template.format(
                n=n,
                user_filter=''
            )
            for n in range(1, options['n_top_subreddits_for_comments']+1)

        ])

        subreddit_thread_part = ' \n'.join([
            subreddit_thread_rank_template.format(
                n=n,
                user_filter=''
            )
            for n in range(1, options['n_top_subreddits_for_threads']+1)

        ])

        domain_thread_part = ' \n'.join([
            domain_rank_template.format(
                n=n,
                user_filter=''
            )
            for n in range(1, options['n_top_domains_for_threads']+1)

        ])

    cte_part = cte_template.format(gilding_injection=gilding_injection_part, user_filter='', **options)

    
    if options['create_table']:
        cte_part = re.sub('MATERIALIZED VIEW', 'TABLE', cte_part)

    if not options['gildings_2']:
        base_template_ = re.sub('(submissions|comments)_(silver|platinum),',' ', base_template)
    else:
        base_template_ = base_template

    if options.get('calculate_account_age'):
        account_age_part = ACCOUNT_AGE_PART
    else:
        account_age_part = ' '
        
    base_part = base_template_.format(
        user_filter = '',
        schema_1_gildings=gilding_injection_user_part,
        account_age_part=account_age_part,
        **options
    )

    query = '\n'.join(
        [
            cte_part,
            base_part,
            subreddit_comment_part,
            subreddit_thread_part,
            domain_thread_part
        ]
    )

    if not options['skip_creation']:
        dimprint(query)
        bprint("EXECUTING!")
        cur.execute(query)

    cur.commit()

    ## INSERT INTO
    # mostly a copy/paste from above
    # note that it uses blind insert, which means that it should only interact with tables created with
    # this exact script
    
    if options['insert_into']:
        user_filter = 'AND username NOT IN (SELECT username FROM {name})'.format(**options)
        if options['gildings_1']:
            gilding_injection_part = gilding_injection
            gilding_injection_user_part = gilding_injection_user

            subreddit_comment_part = ' \n'.join([
                subreddit_comment_rank_template_gilded.format(
                    n=n,
                    user_filter=user_filter,
                )
                for n in range(1, options['n_top_subreddits_for_comments']+1)

            ])

            subreddit_thread_part = ' \n'.join([
                subreddit_thread_rank_template_gilded.format(
                    n=n,
                    user_filter=user_filter,
                )
                for n in range(1, options['n_top_subreddits_for_threads']+1)

            ])

            domain_thread_part = ' \n'.join([
                domain_rank_template_gilded.format(
                    n=n,
                    user_filter=user_filter,
                )
                for n in range(1, options['n_top_domains_for_threads']+1)

            ])
        else:
            gilding_injection_part = ' '
            gilding_injection_user_part = ' '
            
            subreddit_comment_part = ' \n'.join([
                subreddit_comment_rank_template.format(
                    n=n,
                    user_filter=user_filter,
                )
                for n in range(1, options['n_top_subreddits_for_comments']+1)

            ])

            subreddit_thread_part = ' \n'.join([
                subreddit_thread_rank_template.format(
                    n=n,
                    user_filter=user_filter,
                )
                for n in range(1, options['n_top_subreddits_for_threads']+1)

            ])

            domain_thread_part = ' \n'.join([
                domain_rank_template.format(
                    n=n,
                    user_filter=user_filter,
                )
                for n in range(1, options['n_top_domains_for_threads']+1)

            ])


        cte_part = cte_template.format(
            gilding_injection=gilding_injection_part, user_filter=user_filter, **options
        )
        
        cte_part = re.sub('MATERIALIZED VIEW', 'TABLE', cte_part)
        # change from CREATE to INSERT INTO
        cte_part = re.sub('CREATE TABLE (.+?) AS', r'INSERT INTO \1 ', cte_part)
        cte_part = re.sub('IF NOT EXISTS',' ', cte_part)

        if not options['gildings_2']:
            base_template_ = re.sub('(submissions|comments)_(silver|platinum),',' ', base_template)
        else:
            base_template_ = base_template

        base_part = base_template_.format(
            user_filter = '',
            schema_1_gildings=gilding_injection_user_part,
            account_age_part = account_age_part,
            **options
        )

        query = '\n'.join(
            [
                cte_part,
                base_part,
                subreddit_comment_part,
                subreddit_thread_part,
                domain_thread_part
            ] + [
                'WHERE username NOT IN (SELECT username FROM {name})'.format(
                    name=options['name']
                )
            ]
        )

        dimprint(query)
        
        cur.execute(query)
        
        cur.commit()
        bprint('INSERTED!')

    if options['export']:
        pass

    gprint("Done!")



## define templates here


# CTEs and definition

'''
cte_alt_template = "CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS "
cte_alt_definitions_templates = [
    """
CREATE TEMPORARY TABLE user_subreddit_comment_stats AS ( SELECT * FROM (
    SELECT *, row_number() OVER ( PARTITION BY username ORDER BY comment_count DESC) AS subreddit_rank FROM 

    (SELECT subreddit, author_name AS username, sum(score) AS total_score, count(*) AS comment_count, sum(CASE WHEN score < 1 THEN 1 ELSE 0 END) AS total_negative_comments, 
   sum(CASE WHEN score > 1 THEN 1 ELSE 0 END) AS total_positive_comments, sum(gold) AS total_gold, avg(length(text)) AS average_comment_length{gilding_injection}

  FROM {schema_1}.comments 
  GROUP BY subreddit, author_name
  ORDER BY comment_count DESC) i1 ) ii1
    WHERE subreddit_rank <= {n_top_subreddits_for_comments}
)""",
    """
CREATE TEMPORARY TABLE user_subreddit_thread_stats AS ( SELECT * FROM (
  SELECT *, row_number() OVER ( PARTITION BY username ORDER BY thread_count DESC) AS subreddit_rank FROM 

    (SELECT subreddit, author_name AS username, sum(score) AS total_score, count(*) AS thread_count, sum(CASE WHEN score < 1 THEN 1 ELSE 0 END) AS total_negative_threads, 
   sum(CASE WHEN score > 1 THEN 1 ELSE 0 END) AS total_positive_threads, sum(gold) AS total_gold{gilding_injection}

  FROM {schema_1}.threads 
  GROUP BY subreddit, author_name
  ORDER BY thread_count DESC) i2 ) ii2
    WHERE subreddit_rank <= {n_top_subreddits_for_threads}
)""",
    """
CREATE TEMPORARY TABLE user_domain_stats AS ( SELECT * FROM (
SELECT *, row_number() OVER ( PARTITION BY username ORDER BY domain_count DESC) AS domain_rank FROM 

    (SELECT domain, author_name AS username, sum(score) AS total_score, count(*) AS domain_count, sum(CASE WHEN score < 1 THEN 1 ELSE 0 END) AS total_negative_threads, 
   sum(CASE WHEN score > 1 THEN 1 ELSE 0 END) AS total_positive_threads, sum(gold) AS total_gold{gilding_injection}

  FROM {schema_1}.threads
  GROUP BY domain, author_name
  ORDER BY domain_count DESC) i3 ) ii3
  WHERE domain_rank <= {n_top_domains_for_threads}
)
"""
]
'''

cte_template = """
CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS 
WITH 
user_subreddit_comment_stats AS ( SELECT * FROM (
    SELECT *, row_number() OVER ( PARTITION BY username ORDER BY comment_count DESC) AS subreddit_rank FROM 

    (SELECT subreddit, author_name AS username, sum(score) AS total_score, count(*) AS comment_count, sum(CASE WHEN score < 1 THEN 1 ELSE 0 END) AS total_negative_comments, 
   sum(CASE WHEN score > 1 THEN 1 ELSE 0 END) AS total_positive_comments, sum(gold) AS total_gold, avg(length(text)) AS average_comment_length{gilding_injection}

  FROM {schema_1}.comments 
  GROUP BY subreddit, author_name
  ORDER BY comment_count DESC) i1 ) ii1
    WHERE subreddit_rank <= {n_top_subreddits_for_comments} {user_filter}
),
user_subreddit_thread_stats AS ( SELECT * FROM (
  SELECT *, row_number() OVER ( PARTITION BY username ORDER BY thread_count DESC) AS subreddit_rank FROM 

    (SELECT subreddit, author_name AS username, sum(score) AS total_score, count(*) AS thread_count, sum(CASE WHEN score < 1 THEN 1 ELSE 0 END) AS total_negative_threads, 
   sum(CASE WHEN score > 1 THEN 1 ELSE 0 END) AS total_positive_threads, sum(gold) AS total_gold{gilding_injection}

  FROM {schema_1}.threads 
  GROUP BY subreddit, author_name
  ORDER BY thread_count DESC) i2 ) ii2
    WHERE subreddit_rank <= {n_top_subreddits_for_threads} {user_filter}
),
user_domain_stats AS ( SELECT * FROM (
SELECT *, row_number() OVER ( PARTITION BY username ORDER BY domain_count DESC) AS domain_rank FROM 

    (SELECT domain, author_name AS username, sum(score) AS total_score, count(*) AS domain_count, sum(CASE WHEN score < 1 THEN 1 ELSE 0 END) AS total_negative_threads, 
   sum(CASE WHEN score > 1 THEN 1 ELSE 0 END) AS total_positive_threads, sum(gold) AS total_gold{gilding_injection}

  FROM {schema_1}.threads
  GROUP BY domain, author_name
  ORDER BY domain_count DESC) i3 ) ii3
  WHERE domain_rank <= {n_top_domains_for_threads} {user_filter}
)


"""



gilding_injection = """, sum(silver) AS total_silver, sum(platinum) AS total_platinum"""
gilding_injection_user = """ submissions_silver AS historic_submissions_silver, submissions_gold AS historic_submissions_gold, submissions_platinum AS historic_submissions_platinum, comments_silver AS historic_comments_silver, comments_gold AS historic_comments_gold, comments_platinum AS historic_comments_platinum, """


# added id IS NOT NULL condition

base_template = """

SELECT *, CASE WHEN shadowban_flag = 1 or suspended_flag = 1 THEN 1 ELSE 0 END AS bad_flag 
FROM (SELECT username, comment_karma AS historic_comment_karma, post_karma AS historic_post_karma, is_mod AS historic_is_mod, account_created, is_gold AS historic_gold, {schema_1_gildings} timestamp AS historic_timestamps, shadowbanned_by_date AS historic_shadowbanned_by_date, suspended_by_date AS historic_suspended_by_date {account_age_part}  FROM {schema_1}.users WHERE id IS NOT NULL)  historic_user_data

LEFT JOIN (
    SELECT username, shadowbanned_by_date, suspended_by_date, CASE WHEN shadowbanned_by_date IS NOT NULL THEN 1 ELSE 0 END AS shadowban_flag, CASE WHEN suspended_by_date  IS NOT NULL THEN 1 ELSE 0 END AS suspended_flag, submissions_silver, submissions_gold, submissions_platinum, comments_silver, comments_gold, comments_platinum, timestamp AS followup_timestamp, is_mod, comment_karma, post_karma
    FROM {schema_2}.users
    ) modern_profile
USING(username)

-- these two are just to see if user hasn't posted in a while
LEFT JOIN (
    SELECT author_name AS username, max(created) AS last_comment_timestamp FROM {schema_1}.comments GROUP BY author_name
) last_comment
USING(username)
LEFT JOIN (
    SELECT author_name AS username, max(created) AS last_thread_timestamp FROM {schema_1}.threads GROUP BY author_name
) last_thread
USING(username)

LEFT JOIN (
  SELECT author_name AS username, sum(score) AS total_comment_score, count(*) AS comment_count, sum(CASE WHEN score < 1 THEN 1 ELSE 0 END) AS total_negative_comments, 
  sum(CASE WHEN score <= {strong_negative_comment_score_maximum} THEN 1 ELSE 0 END) AS total_strongly_negative_comments, sum(CASE WHEN score >= {strong_positive_comment_score_minimum} THEN 1 ELSE 0 END) AS total_strongly_positive_comments,
   sum(CASE WHEN score > 1 THEN 1 ELSE 0 END) AS total_positive_comments, sum(gold) AS historic_total_comment_gold, avg(length(text)) AS average_comment_length

  FROM {schema_1}.comments 
  GROUP BY author_name
) user_comment_stats 
USING(username)

LEFT JOIN (
SELECT author_name AS username, sum(score) AS total_thread_score, count(*) AS thread_count, sum(CASE WHEN score < 1 THEN 1 ELSE 0 END) AS total_negative_threads, 
sum(CASE WHEN score <= {strong_negative_thread_score_maximum} THEN 1 ELSE 0 END) AS total_strongly_negative_threads, sum(CASE WHEN score >= {strong_positive_thread_score_minimum} THEN 1 ELSE 0 END) AS total_strongly_positive_threads,
   sum(CASE WHEN score > 1 THEN 1 ELSE 0 END) AS total_positive_threads, sum(gold) AS historic_total_thread_gold

  FROM {schema_1}.threads 
  GROUP BY author_name
) user_thread_stats 
USING(username)
{user_filter}

"""

domain_rank_template = """

LEFT JOIN ( 
    SELECT domain AS domain_{n}, username, total_score AS domain_{n}_total_thread_score, domain_count AS domain_{n}_count, total_negative_threads AS domain_{n}_total_negative_threads,
    total_positive_threads AS domain_{n}_total_positive_threads, total_gold AS domain_{n}_thread_gold
    FROM user_domain_stats 
    WHERE domain_rank = {n} {user_filter}
) domains_{n}
USING(username)


"""


domain_rank_template_gilded = """

LEFT JOIN ( 
    SELECT domain AS domain_{n}, username, total_score AS domain_{n}_total_thread_score, domain_count AS domain_{n}_count, total_negative_threads AS domain_{n}_total_negative_threads,
    total_positive_threads AS domain_{n}_total_positive_threads, total_gold AS domain_{n}_thread_gold, total_silver AS domain_{n}_thread_silver, total_platinum AS domain_{n}_thread_platinum
    FROM user_domain_stats 
    WHERE domain_rank = {n} {user_filter}
) domains_{n}
USING(username)


"""


subreddit_thread_rank_template = """
LEFT JOIN (
    SELECT subreddit AS subreddit_{n}_threads, username, total_score AS subreddit_{n}_total_thread_score, thread_count AS subreddit_{n}_thread_count, total_negative_threads AS subreddit_{n}_total_negative_threads,
    total_positive_threads AS subreddit_{n}_total_positive_threads, total_gold AS subreddit_{n}_thread_gold
    FROM user_subreddit_thread_stats 
    WHERE subreddit_rank = {n} {user_filter}
) sub_threads_{n}
USING(username)

"""

subreddit_thread_rank_template_gilded = """
LEFT JOIN (
    SELECT subreddit AS subreddit_{n}_threads, username, total_score AS subreddit_{n}_total_thread_score, thread_count AS subreddit_{n}_thread_count, total_negative_threads AS subreddit_{n}_total_negative_threads,
    total_positive_threads AS subreddit_{n}_total_positive_threads, total_gold AS subreddit_{n}_thread_gold, total_silver AS subreddit_{n}_thread_silver, total_platinum AS subreddit_{n}_thread_platinum
    FROM user_subreddit_thread_stats 
    WHERE subreddit_rank = {n} {user_filter}
) sub_threads_{n}
USING(username)

"""

subreddit_comment_rank_template = """

LEFT JOIN (
    SELECT subreddit AS subreddit_{n}_comments, username, total_score AS subreddit_{n}_total_comment_score, comment_count AS subreddit_{n}_comment_count, total_negative_comments AS subreddit_{n}_total_negative_comments,
    total_positive_comments AS subreddit_{n}_total_positive_comments, total_gold AS subreddit_{n}_comment_gold, average_comment_length AS subreddit_{n}_average_comment_length
    FROM user_subreddit_comment_stats 
    WHERE subreddit_rank = {n} {user_filter}
) sub_comments_{n}
USING(username)

"""

subreddit_comment_rank_template_gilded = """

LEFT JOIN (
    SELECT subreddit AS subreddit_{n}_comments, username, total_score AS subreddit_{n}_total_comment_score, comment_count AS subreddit_{n}_comment_count, total_negative_comments AS subreddit_{n}_total_negative_comments,
    total_positive_comments AS subreddit_{n}_total_positive_comments, total_gold AS subreddit_{n}_comment_gold, average_comment_length AS subreddit_{n}_average_comment_length, total_silver AS subreddit_{n}_comment_silver, total_platinum AS subreddit_{n}_comment_platinum
    FROM user_subreddit_comment_stats 
    WHERE subreddit_rank = {n} {user_filter} 
) sub_comments_{n}
USING(username)

"""

drop_template = "DROP MATERIALIZED VIEW IF EXISTS {name}"

ACCOUNT_AGE_PART = ', EXTRACT(days FROM timestamp-account_created) AS account_age '


## execution
if __name__ == '__main__':
    options = get_options()

    main(options)
