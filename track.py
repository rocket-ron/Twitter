from twitterutils.trackerListener import TrackerListener
from serializers.mongoTweetSerializer import MongoTweetSerializer
import argparse


parser = argparse.ArgumentParser(description = 'Track phrases in the Twitter stream API')
parser.add_argument('-db','--database',
					type=str,
					required=True,
					help='MongoDB database to store tweets')
parser.add_argument('-c', '--collection',
					type=str,
					required=True,
					help='MongoDB collection to store tweets')
parser.add_argument('-s', '--server',
					type=str,
					required=False,
					default='127.0.0.1',
					help='MongoDB server to backup. Default is 127.0.0.1')
parser.add_argument('-t', '--track',
					type=str,
					required=True,
					help='Twitter phrases to track')

args = parser.parse_args()


serializer = MongoTweetSerializer(args.server, args.database , args.collection)
listener   = TrackerListener(serializer)

listener.track(args.track)


