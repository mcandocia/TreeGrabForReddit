def process_award_data(award_data, key={}):
    simple_data = [
        {
            'award_name': x['name'],
            'subreddit_id': x['subreddit_id'],
            'award_count': x['count']
        }
        for x in award_data
    ]

    # update fields with desired key (comment id/user id/thread id)
    [x.update(key) for x in simple_data]

    complete_data = {
        (x['name'], x['subreddit_id']): {
            field:x[field]
            for field in [
                    'name',
                    'coin_price',
                    'description',
                    'coin_reward',
                    'giver_coin_reward',
                    'subreddit_coin_reward',
                    'days_of_premium',
                    'award_sub_type',
                    'subreddit_id',
                    'awardings_required_to_grant_benefits',
                    'days_of_drip_extension',
                    'static_icon_url',
                    'award_type',
            ]
        }
        for x in award_data
    }

    return {
        'simple_data': simple_data,
        'complete_data': complete_data
    }
