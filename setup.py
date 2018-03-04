from distutils.core import setup

setup(name='kinnara',
      version='0.0.1',
      description="A library for gathering, processing, and storing data from Twitter's developer API",
      author='Erik Storrs',
      install_requires=['requests==2.9.1',
            'requests-oauthlib==0.8.0',
            'boto3==1.4.7']
     )
