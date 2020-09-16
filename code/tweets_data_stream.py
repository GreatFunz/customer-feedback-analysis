# -*- coding: utf-8 -*-
"""
Created on Fri Jul 31 14:00:06 2020

@author: haruna
"""
# import twitter credentials     
from keysecrets import twitter_secrets

from requests_oauthlib import OAuth1Session 
import os
import datetime
import json

num_stream = 1000
word_search = "put your product name"
#Set a search criteria
search_criteria= {"track":f"{word_search.replace('#', '').lower().strip('')}", "language":"en"}
url = "https://stream.twitter.com/1.1/statuses/filter.json"

query_url=f"{url}?{'&'.join(f'{i}={j}' for i,j in search_criteria.items())}"

# API authentication
auth_twitter = OAuth1Session(
    client_key = twitter_secrets.CONSUMER_KEY,
    client_secret = twitter_secrets.CONSUMER_SECRET,
    resource_owner_key = twitter_secrets.ACCESS_TOKEN,
    resource_owner_secret = twitter_secrets.ACCESS_SECRET
    )

# Create a parent directory in the working directory for the incoming tweets, name(hash of None)
try:
   data_path = os.makedirs(str(f"{os.getlogin()}"), exist_ok=True)
except Exception as e:
    print(e)
    print(f"Error creating/finding directory {data_path} for tweets data")
path_wdirectory=f"{os.getcwd()}/{str(os.getlogin())}"
#path_wdirectory=f"{os.getcwd()}/{(str(None.__hash__()).strip('-'))}"

with auth_twitter.get(query_url, stream=True) as query_response:
    print(f"Now Streaming {num_stream +1} tweets")
    #Good to specify chunck_size to save system memory, but may loose data if chunck_size is too small
    for i, raw_tweets in enumerate(query_response.iter_lines(chunk_size=100)):
        #Loading the bytes (raw_tweets) in to json as a dictionary gives flexibility to manupulate the data and its components
        try:
            raw_data = json.loads(raw_tweets)
        except Exception as e:
            print(e)
            continue
        try:
        #print the components of the raw_tweets prefer ie, user, create_at, text
            print(f"{i+1} / {num_stream +1}: {raw_data['text']} \n")
        except Exception as e:
            print(e)
            print(f"This {i}/{num_stream} is corrupted !!")
            continue
        if i == num_stream:
            break
        try:
            with open(f"{path_wdirectory}/{((datetime.datetime.now().timestamp()))}.json", "wb") as out_tweets:
                out_tweets.write(raw_tweets)
        except Exception as e:
            print(f"Oops! {e} \n")
