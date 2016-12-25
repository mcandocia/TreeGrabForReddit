import praw
from praw_object_data import retry_if_broken_connection

client_id = "CLIENT ID"
secret="SECRET"
username=''
password='notapassword'
user_agent = 'tree grabber for reddit, made by /u/antirabit, run by someone else'

@retry_if_broken_connection
def scraper():
    r = praw.Reddit(client_id=client_id,
                    client_secret=secret,
                    username=username,
                    password=password,
                    user_agent=user_agent)
    return r


    
