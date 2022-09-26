# Importing Tweepy and time
import tweepy
import time
import json
import kafka


producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server
topic_name = "TWITTER-TWEETS-CS523"

# Credentials (INSERT YOUR KEYS AND TOKENS IN THE STRINGS BELOW)
api_key = "ksdSjNopx50diEc5tW57D9Ozt"
api_secret = "kvV1pdgfEFlDQVUM9Jwgm8lKT1PlBCPsaTqBbcFefh3ryh7oOz"
bearer_token = r"AAAAAAAAAAAAAAAAAAAAAOlBhQEAAAAAUoCSfczIr8Ou%2BEuL0oCzlifn7YI%3DkJxkRMFIxW1nKn5YVSGZxj8lBCAn5WAYCHyBLJIo6xtgmXNS3v"
access_token = "2975180000-BT6XcHLEk6qksLT3Hq8D7oxDhmkFNZUKWas9UCs"
access_token_secret = "veCIIHacEzP9lbpaUsTJgh7Ng2Xs8Ct2jXUTXFKW40R1y"

# Gainaing access and connecting to Twitter API using Credentials
client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

search_terms = ["python", "programming", "coding","Covid"]


# Bot searches for tweets containing certain keywords
class MyStream(tweepy.StreamingClient):

    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected")

    # This function gets called when a tweet passes the stream
    def on_tweet(self, tweet):
        # Displaying tweet in console
        if tweet.referenced_tweets == None:
            print(f" tweetid: {tweet.id} created_at: {tweet.created_at} ({tweet.author_id}): {tweet.text}")
            json_data = json.dumps({
                "text": tweet.text
            }, default=str)
            producer.send(topic_name,json_data.encode("ascii"))
            # Delay between tweets
            time.sleep(1.5)


# Creating Stream object
stream = MyStream(bearer_token=bearer_token)

# Adding terms to search rules
# It's important to know that these rules don't get deleted when you stop the
# program, so you'd need to use stream.get_rules() and stream.delete_rules()
# to change them, or you can use the optional parameter to stream.add_rules()
# called dry_run (set it to True, and the rules will get deleted after the bot
# stopped running).
for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))

# Starting stream
stream.filter(tweet_fields=["created_at"],expansions="author_id")
