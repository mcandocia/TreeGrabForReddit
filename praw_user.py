import praw
from praw_object_data import retry_if_broken_connection

client_id = ''
secret=''
username=''
p_assword=''
user_agent = ''

@retry_if_broken_connection
def scraper(token=None):
    if token:
        password = '%s:%s' % (p_assword, token)
    r = praw.Reddit(client_id=client_id,
                    client_secret=secret,
                    username=username,
                    password=p_assword,
                    user_agent=user_agent)
    return r


    
