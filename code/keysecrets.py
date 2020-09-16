###Replace the values below with your own Twitter API Keys, Secrets, and Tokens####

#### Twitter Consumer API keys and Secret
CONSUMER_KEY    = " your consumer key"
CONSUMER_SECRET = "your comsumer secret"

### Twitter Access token and access token secret
ACCESS_TOKEN    = " your access token "
ACCESS_SECRET   = " your access secret"


class TwitterKeySecrets():

    def __init__(self):
        self.CONSUMER_KEY    = CONSUMER_KEY
        self.CONSUMER_SECRET = CONSUMER_SECRET
        self.ACCESS_TOKEN    = ACCESS_TOKEN
        self.ACCESS_SECRET   = ACCESS_SECRET
        
        for key, secret in self.__dict__.items():
            assert secret != "", f"Please provide a valid secret for: {key}"
    def __str__(self):
        return print(" this is an instance of TwitterKeySecrets class ")
    
    def __repr__(self):
        return print(" this is an instance of TwitterKeySecrets class ")
        
twitter_secrets = TwitterKeySecrets()
