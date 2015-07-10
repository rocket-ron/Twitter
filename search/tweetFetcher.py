import signal
import threading
import tweepy
#from s3ChunkedTweetSerializer import S3ChunkedTweetSerializer
from mongoTweetSerializer import MongoTweetSerializer

class TweetFetcher:

	def __init__(self):
		# Authentication tokens
		consumer_key = ""
		consumer_secret = "T"

		access_token = ""
		access_token_secret = ""

#		auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
		auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
#		auth.set_access_token(access_token, access_token_secret)
		self.api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

#		self.serializer = S3ChunkedTweetSerializer(200, 'W205-Assignment2-RC-test')
		self.serializer = MongoTweetSerializer('twitter_db','tweets')

		signal.signal(signal.SIGINT, self.interrupt)

		# Thanks to Vincent Chio for showing me how to use a mutex in Python
		self._lock = threading.RLock()

	def interrupt(self, signum, frame):
		print("CTRL-C caught, closing...")
		with self._lock:
			self.serializer.end()
		exit(1)

	def search(self, q):
		for tweet in tweepy.Cursor(self.api.search,q=q, count=1500).items():
			with self._lock:
				self.serializer.write(tweet)
		self.serializer.end()
