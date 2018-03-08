import io
import json
import logging
import uuid

import boto3

from kinnara.gather.twitter import TwitterApiWrapper


TWEETS_RATE_LIMIT = 900
USERS_RATE_LIMIT = 900
FOLLOWERS_RATE_LIMIT = 15

logger = logging.getLogger(__name__)



class S3Gatherer(object):
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


    def store_tweets(self, user_id, key, num_tweets=200, filter_retweets=True):
        '''Stores tweets for given user in s3 bucket

        user_id
            account to gather tweets from
        key
            key to store file under in s3 bucket
        num_tweets
            number of tweets to gather from user.
            200 is max that can be gathered in one call.
            3200 is max that can be gathered for one user.
        filter_retweets
            filter out all retweets
        '''
        tweets = self.twitter.get_tweets(user_id, max_tweets_returned=num_tweets)

        # filter out retweets if necessary
        if not filter_retweets:
            tweets = [t for t in tweets
                    if 'retweeted_status' not in t]

        payload = json.dumps(tweets)
        f_obj = io.BytesIO(payload.encode())

        self.s3.upload_fileobj(f_obj, self.bucket_name, key)


    def store_user_and_tweets(self, user_id, key, num_tweets=200, filter_retweets=False):
        '''Stores tweets and user object for a given user in s3 bucket

        user_id
            account to gather tweets from
        key
            key to store file under in s3 bucket
        num_tweets
            number of tweets to gather from user.
            200 is max that can be gathered in one call.
            3200 is max that can be gathered for one user.
        filter_retweets
            filter out all retweets
        '''
        logger.info('getting user {}'.format(user_id))
        user = self.twitter.get_user(user_id)
        # if user is none, just make a user obj with user_id
        if user is None:
            user = {'user_id': user_id}

        logger.info('getting tweets {}'.format(user_id))
        tweets = self.twitter.get_tweets(user_id, max_tweets_returned=num_tweets)
        logger.info('getting tweets {}'.format(user_id))

        # filter out retweets if necessary
        if filter_retweets:
            tweets = [t for t in tweets
                    if 'retweeted_status' not in t]

        payload = {
                'user': user,
                'tweets': tweets
                }

        payload = json.dumps(payload)
        f_obj = io.BytesIO(payload.encode())

        logger.info('uploading user and tweet obj for {}'.format(user_id))
        self.s3.upload_fileobj(f_obj, self.bucket_name, key)


    def store_tweets_via_followers(self, seed_user_id, num_tweets=200, filter_retweets=False,
            max_follower_ids=5000):
        '''Gather users and tweets from a seed user id.

        Currently defualt settings are optimized around twitters rate limits

        seed_user_id
            seed account to start gathering tweets from
        num_tweets
            number of tweets to gather per user.
            200 is max that can be gathered in one call.
            3200 is max that can be gathered for one user.
        filter_retweets
            filter out all retweets
        max_follower_ids
            max number of follower ids to gather for each user in the graph
            5000 is the max that can be gathered in one call
        '''

        # ids to gather tweets from
        tweets_user_ids_pool = self.twitter.get_follower_ids(seed_user_id,
                max_ids_returned=max_follower_ids)
        # ids to gather followers from
        followers_user_ids_pool = [x for x in tweets_user_ids_pool]
        # store already seen ids so double lookups don't happen
        already_seen = set()

        # store seed profile and tweets
        self.store_user_and_tweets(seed_user_id, '{}.json'.format(seed_user_id),
                num_tweets=num_tweets, filter_retweets=filter_retweets)

        while True:
            logger.info('length already seen: {}'.format(len(already_seen)))
            logger.info('length tweet pool: {}'.format(len(tweets_user_ids_pool)))
            logger.info('length followers pool: {}'.format(len(followers_user_ids_pool)))

            # grab 15 users from follower pool and get their followers
            follower_ids = followers_user_ids_pool[:FOLLOWERS_RATE_LIMIT - 1]
            followers_user_ids_pool = followers_user_ids_pool[FOLLOWERS_RATE_LIMIT - 1:]

            for f_id in follower_ids:
                logger.info('getting followers for {}'.format(f_id))
                try:
                    ids = self.twitter.get_follower_ids(f_id,
                            max_ids_returned=max_follower_ids)
                except Exception as e:
                    logger.exception('exception getting followers for {}\n{}'.format(
                        f_id, e))

                logger.debug('length of ids: {}'.format(len(ids)))
                #filter ids that have already been seen
                ids = [x for x in ids
                        if x not in already_seen]

                # add the new ids to the tweet pool and followers pool
                followers_user_ids_pool += ids
                tweets_user_ids_pool += ids

                # add ids to already seen
                already_seen.update(ids)

            # grab 900 users to get tweets for
            user_ids = tweets_user_ids_pool[:TWEETS_RATE_LIMIT - 1]
            tweets_user_ids_pool = tweets_user_ids_pool[TWEETS_RATE_LIMIT - 1:]

            for i, user_id in enumerate(user_ids):
                logger.info('{} - getting tweets and user for {}'.format(i, user_id))
                try:
                    self.store_user_and_tweets(user_id, '{}.json'.format(user_id),
                            num_tweets=num_tweets, filter_retweets=filter_retweets)
                except Exception as e:
                    logger.exception('exception getting tweets or user for {}\n{}'.format(
                        user_id, e))


    def store_tweet_stream(self, follow=[], track=[], locations=[],
            tweets_per_file=100, cutoff=None):
        '''Store tweets from a live stream into s3 bucket.
        Tweets are chunked into files with tweets_per_file tweets in each file.
        The keys the files are stored under are randomly generated uuids.

        follow
            list of screennames to follow
        track
            keywords to be included in tweets in live stream
        locations
            bounding boxes for tweets in live stream
        tweets_per_file
            number of tweets per file stored in s3 bucket
        cutoff
            disconnect from stream and stop gathering after this many tweets are gathered
        '''
        count = 0
        tweets = []
        for tweet in self.twitter.get_live_stream(follow=follow, track=track, locations=locations):
            count += 1
            tweets.append(tweet)

            if len(tweets) >= tweets_per_file:
                key = str(uuid.uuid4())
                payload = json.dumps(tweets)

                logger.info(payload)
                f_obj = io.BytesIO(payload.encode())

                self.s3.upload_fileobj(f_obj, self.bucket_name, key)
                tweets = []

                if cutoff is not None and count >= cutoff:
                    break
