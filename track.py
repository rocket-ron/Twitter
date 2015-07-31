from twitterutils.trackerListener import TrackerListener
from serializers.mongoTweetSerializer import MongoTweetSerializer
from serializers.s3ChunkedTweetSerializer import S3ChunkedTweetSerializer
from serializers.consoleTweetSerializer import ConsoleTweetSerializer
import argparse


parser = argparse.ArgumentParser(description = 'Track phrases in the Twitter stream API')
parser.add_argument('-db','--database',
					type=str,
					required=False,
					default=None,
					help='MongoDB database to store tweets')
parser.add_argument('-c', '--collection',
					type=str,
					required=False,
					default=None,
					help='MongoDB collection to store tweets')
parser.add_argument('-s', '--server',
					type=str,
					required=False,
					default='127.0.0.1',
					help='MongoDB server host name or ip address')
parser.add_argument('-t', '--track',
					type=str,
					required=True,
					help='Twitter phrases to track')
parser.add_argument('-b','--bucket',
					type=str,
					required=False,
					default=None,
					help='Amazon S3 storage to store tweets')
parser.add_argument('-ck', '--chunksPerTweet',
					type=str,
					required=False,
					default=100,
					help='number of tweets per chunk for S3 buckets')

args = parser.parse_args()


fetchSize = 1500

# pick the serializer
if args.database:
	if (args.collection):
		serializer = MongoTweetSerializer(args.server, args.database , args.collection)
	else:
		print "MongoDB database and collection must be specified when using MongoDB server"
		sys.exit(1)
elif args.bucket:
	serializer = S3ChunkedTweetSerializer(args.chunksPerTweet, args.bucket)
else:
	print "Writing tweets to console..."
	serializer = ConsoleTweetSerializer()
	fetchSize = 10

listener   = TrackerListener(serializer)
listener.track(args.track)


