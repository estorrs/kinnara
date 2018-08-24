import json
import logging
import time

import requests

from requests_oauthlib import OAuth1

logger = logging.getLogger(__name__)


class TwitterApiWrapper(object):
    '''A wrapper for assorted twitter developer api endpoints'''
    def __init__(self, api_key, api_secret, access_token, access_token_secret):
        self.auth = OAuth1(api_key, api_secret,
                access_token, access_token_secret)


    def get_follower_ids(self, user_id, max_ids_returned=5000, return_if_error=True):
        '''Get followers for the given user id.

        user_id
            user id of account to get folloer ids of
        max_ids_returned
            max number of follower ids of the given user to grab.
            default of 5000 is max number that can be grabbed in one call.
        '''
        if max_ids_returned > 5000:
            count = 5000
        else:
            count = max_ids_returned

        url = 'https://api.twitter.com/1.1/followers/ids.json'
        params = {
            'user_id': user_id, # can also use screen_name instead of user_id
            'cursor': -1,
            'count': count
        }

        follower_ids = []
        while True:
            r = requests.get(url, auth=self.auth, params=params)

            if r.status_code == 200:
                response_dict = r.json()
                follower_ids += response_dict['ids']

                if len(follower_ids) >= max_ids_returned or response_dict['next_cursor'] == 0:
                    break

                params['cursor'] = response_dict['next_cursor']
            else:
                logger.warning('Error getting twitter follower ids. Error code was {}. {} requests remaining'.format(
                        r.status_code, r.headers['x-rate-limit-remaining']))
                logger.warning('response content: {}'.format(r.content))

                # return empty if no retry expected
                if return_if_error:
                    logger.warning('returning empty list')
                    return []

            # sleep if rate limited
            if r.headers['x-rate-limit-remaining'] == '0':
                logger.info('rate limit reached, sleeping for {} seconds'.format(60*15))
                time.sleep(60*15)

        return follower_ids[:max_ids_returned]


    def get_user(self, user_id):
        '''Get user object, returns None if user not found'''
        url = 'https://api.twitter.com/1.1/users/show.json'

        if user_id.isdigit():
            params = {
                        'user_id': user_id
                    }
        else:
            params = {
                        'screen_name': user_id
                    }


        r = requests.get(url, auth=self.auth, params=params)

        if r.status_code != 200:
            return None

        return r.json()


    def get_tweets(self, user_id, max_tweets_returned=200, return_if_error=True):
        '''Get tweets for the given user id.
        Most recent tweets are returned first.

        user_id
            user id of account to get tweets of
        max_tweets_returned
            max number of tweets of the given user to grab.
            default of 200 is max number that can be grabbed in one call.
        '''
        # max number of tweets that can be returned for a timeline for basic api usage
        if max_tweets_returned > 3200:
            max_tweets_returned = 3200

        if max_tweets_returned > 200:
            count = 200
        else:
            count = max_tweets_returned

        url = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
        params = {
            'user_id': user_id,
            'count': count,
            'trim_user': 1,
            'tweet_mode': 'extended'
        }

        tweets = []
        while True:
            logger.info('getting tweets for {}'.format(user_id))
            r = requests.get(url, auth=self.auth, params=params)

            if r.status_code == 200:
                returned_tweets = r.json()
                tweets += returned_tweets

                # break if > max tweets returned or there are no more tweets to gather
                if len(tweets) >= max_tweets_returned or len(tweets) == 0 or len(tweets) % count != 0:
                    break

                params['max_id'] = tweets[-1]['id'] - 1 # need to subtract one so same tweet doesn't show up twice
            else:
                logger.warning('Error getting tweets. Error code was {}. {} requests remaining'.format(
                        r.status_code, r.headers['x-rate-limit-remaining']))
                if return_if_error and r.status_code!=429:
                    return tweets

            # sleep if rate limited
            if r.headers['x-rate-limit-remaining'] == '0':
                logger.info('rate limit reached, sleeping for {} seconds'.format(60*15))
                time.sleep(60*15)

        return tweets[:max_tweets_returned]


    def get_live_stream(self, follow=None, track=None, locations=None):
        '''Yields tweets that match the given filter criteria.

        follow
            users tweets should come from.
            if no user specified then tweets will be returned for all twitter users
        track
            keywords tweets should have
        locations
            bounding boxes tweets should be within
        '''
        if follow is None:
            follow = []
        if track is None:
            track = []
        if locations is None:
            locations = []

        url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        params = {
            'tweet_mode': 'extended',
            'follow': follow,
            'track': track,
            'locations': locations
        }

        r = requests.post(url, auth=self.auth, params=params, stream=True)

        for line in r.iter_lines(chunk_size=1, decode_unicode=True):
            if line:
                try:
                    tweet = json.loads(line)
                    yield tweet
                except json.JSONDecodeError:
                    logger.debug('Tweet from stream was not json compatible')
