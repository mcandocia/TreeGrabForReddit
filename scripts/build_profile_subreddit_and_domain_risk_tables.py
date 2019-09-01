## build_profilesubreddit_and_domain_risk_tables.py
# Max Candocia - mscandocia@gmail.com
# 08/30/2019
#
# script that summarizes data from build_profile.py to create
# domain, and subreddit comment & thread summary tables to use to build
# variables for users in database

from __future__ import print_function
import argparse
import os
import psycopg2
import db

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
        help='The schema to draw subreddit/domain stats from',
    )

    parser.add_argument(
        '--schema-2',
        help='The schema to draw suspension/banned stats from'
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
        '--variable-count-minimum',
        type=int,
        default=100,
        help='The minimum count for a given subreddit/domain required to be computed in the view. Default 100.'
    )

    parser.add_argument(
        '--drop-view',
        action='store_true',
        help='Drops the materialized view at the beginning'
    )

    parser.add_argument(
        '--export',
        help='File to export data to',
        required=False,
    )

    parser.add_argument(
        '--skip-creation',
        help='Do not create the table',
        action='store_true'
    )

    parser.add_argument(
        '--create-indexes',
        help='Create indexes on views for optimized use later',
        action='store_true'
    )
    


    args = parser.parse_args()

    options = vars(args)

    if options['schema_1'] is None or options['schema_2'] is None:
        raise ValueError('Must define both schema 1 and schema 2')

    return options



# for shadowbanned, suspended, any bad, non-bad, and aggregate, calculate
# average + overall count statistics
# using 
def main(options):
    subreddit_comment_query = subreddit_comment_query_template.format(**options)
    subreddit_thread_query = subreddit_thread_query_template.format(**options)
    domain_thread_query = domain_thread_query_template.format(**options)

    

    cur = db.Database(
        connect_only=True
    )


    if options['drop_view']:
        drop_queries = [drop_template.format(**options) for drop_template in drop_templates]
        for drop_query in drop_queries:
            print(drop_query)
            print('EXECUTING!')
            cur.execute(drop_query)
        cur.commit()

    if not options['skip_creation']:
        print('Executing queries')

        for query in [subreddit_comment_query, subreddit_thread_query, domain_thread_query]:
            print(query)
            cur.execute(query)
            cur.commit()

    if options['create_indexes']:

        index_queries = [
            db.standardized_index_query(
                '%s_%s' % (options['name'], suffix),
                column
            )
            for suffix, column in
            [
                ('subreddit_comment_agg', 'subreddit'),
                ('subreddit_thread_agg', 'subreddit'),
                ('domain_thread_agg', 'domain')
            ]
        ]
        for query in index_queries:
            print(query)
            cur.execute(query)

        cur.commit()

    print('Done!')
            




# templates
subreddit_comment_query_template = """
CREATE MATERIALIZED VIEW IF NOT EXISTS {name}_subreddit_comment_agg AS

SELECT 
  subreddit,
  count(*) AS count, 
  sum(shadowban_flag) AS shadowban_count, 
  sum(suspended_flag) AS suspended_count,
  sum(bad_flag) AS bad_count

  FROM (
    SELECT subreddit, username FROM (
    SELECT subreddit, username, row_number() OVER (PARTITION BY username ORDER BY comment_count DESC) AS subreddit_rank FROM
     (
       SELECT subreddit, author_name AS username, count(*) AS comment_count 
       FROM {schema_1}.comments
       GROUP BY subreddit, author_name
     ) i1
     WHERE 
      username NOT IN (select username FROM {schema_1}.users WHERE id IS NULL)
  ) t1a
  WHERE subreddit_rank <= {n_top_subreddits_for_comments} 
  GROUP BY subreddit, username
  ) t1
  LEFT JOIN 
  (
    SELECT username, 
     CASE WHEN shadowbanned_by_date IS NULL THEN 0 ELSE 1 END AS shadowban_flag,
     CASE WHEN suspended_by_date IS NULL THEN 0 ELSE 1 END AS suspended_flag,
     CASE WHEN id IS NULL THEN 1 ELSE 0 END AS bad_flag
     FROM {schema_2}.users
  ) t2
  USING(username)
  GROUP BY subreddit
  HAVING count(*) >= {variable_count_minimum}
  ORDER BY count DESC
"""

subreddit_thread_query_template = """
CREATE  MATERIALIZED VIEW IF NOT EXISTS {name}_subreddit_thread_agg AS

SELECT 
  subreddit,
  count(*) AS count, 
  sum(shadowban_flag) AS shadowban_count, 
  sum(suspended_flag) AS suspended_count,
  sum(bad_flag) AS bad_count

  FROM (
   SELECT username, subreddit FROM (
    SELECT subreddit, username, row_number() OVER (PARTITION BY username ORDER BY thread_count DESC) AS subreddit_rank FROM
     (
       SELECT subreddit, author_name AS username, count(*) AS thread_count 
       FROM {schema_1}.threads
       GROUP BY subreddit, author_name
     ) i1
     WHERE 
      username NOT IN (select username FROM {schema_1}.users WHERE id IS NULL)

    ) t1a
    WHERE subreddit_rank <= {n_top_subreddits_for_threads}
    GROUP BY subreddit, username
  ) t1
  LEFT JOIN 
  (
    SELECT username, 
     CASE WHEN shadowbanned_by_date IS NULL THEN 0 ELSE 1 END AS shadowban_flag,
     CASE WHEN suspended_by_date IS NULL THEN 0 ELSE 1 END AS suspended_flag,
     CASE WHEN id IS NULL THEN 1 ELSE 0 END AS bad_flag
     FROM {schema_2}.users
  ) t2
  USING(username)

  GROUP BY subreddit
  HAVING count(*) >= {variable_count_minimum}
  ORDER BY count DESC


"""


domain_thread_query_template = """
CREATE  MATERIALIZED VIEW IF NOT EXISTS {name}_domain_thread_agg AS

SELECT 
  domain,
  count(*) AS count, 
  sum(shadowban_flag) AS shadowban_count, 
  sum(suspended_flag) AS suspended_count,
  sum(bad_flag) AS bad_count

  FROM (
   SELECT username, domain FROM (
    SELECT domain, username, row_number() OVER (PARTITION BY username ORDER BY domain_count DESC) AS domain_rank FROM
     (
       SELECT domain, author_name AS username, count(*) AS domain_count 
       FROM {schema_1}.threads
       GROUP BY domain, author_name
     ) i1
     WHERE 
      username NOT IN (select username FROM {schema_1}.users WHERE id IS NULL)

    ) t1a
    WHERE domain_rank <= {n_top_domains_for_threads}     
    GROUP BY username, domain
  ) t1
  LEFT JOIN 
  (
    SELECT username, 
     CASE WHEN shadowbanned_by_date IS NULL THEN 0 ELSE 1 END AS shadowban_flag,
     CASE WHEN suspended_by_date IS NULL THEN 0 ELSE 1 END AS suspended_flag,
     CASE WHEN id IS NULL THEN 1 ELSE 0 END AS bad_flag
     FROM {schema_2}.users
  ) t2
  USING(username)
  GROUP BY domain
  HAVING count(*) >= {variable_count_minimum}
  ORDER BY count DESC

"""


drop_templates = [
    'DROP MATERIALIZED VIEW IF EXISTS {{name}}_{suffix}'.format(
        suffix=suffix
    )
    for suffix in ['subreddit_comment_agg','subreddit_thread_agg','domain_thread_agg']
]
        



# execution


if __name__=='__main__':
    options = get_options()
    main(options)
