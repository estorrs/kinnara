from distutils.core import setup

setup(name='twitter_s3',
      version='0.0.1',
      description="A library integrating Twitter and AWS S-3 storage for data gathered from Twitter's developer API",
      author='Erik Storrs',
      install_requires=['requests==2.9.1', 'requests-oauthlib==0.8.0']
     )
