import io
import json

import boto3

from kinnara.gather.twitter import TwitterApiWrapper

class TwitterS3Gatherer(object):
    '''A class that gathers data from twitter and dumps it to s3 buckets'''
    def __init__(self, twitter_api_key, twitter_api_secret,
            twitter_access_token, twitter_access_token_secret,
            aws_access_key, aws_secret_access_key, s3_bucket_name):
        '''Initialize class with twitter and aws credentials'''

        self.twitter = TwitterApiWrapper(twitter_api_key, twitter_api_secret,
                twitter_access_token, twitter_access_token_secret)

        self.s3 = boto3.client('s3',
                aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_access_key)

        self.bucket_name = s3_bucket_name


    def store_tweets(self, user_id, key, num_tweets=200):
        '''Stores tweets for given user in s3 bucket

        user_id
            account to gather tweets from
        num_tweets
            number of tweets to gather from user.
            200 is max that can be gathered in one call.
            3200 is max that can be gathered for one user.
        '''
        tweets = self.twitter.get_tweets(user_id, max_tweets_returned=num_tweets)

        payload = json.dumps(tweets)
        f_obj = io.BytesIO(payload.encode())

        self.s3.upload_fileobj(f_obj, self.bucket_name, key)
