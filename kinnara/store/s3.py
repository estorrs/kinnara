import boto3

from .twitter import TwitterApiWrapper

class TwitterS3Gatherer(object):
    '''A class that gathers data from twitter and dumps it to s3 buckets'''
    def __init__(self, 
