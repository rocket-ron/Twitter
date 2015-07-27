import sys
import datetime
import urllib
from twitterutils import tweetFetcher
from serializers.mongoTweetSerializer import MongoTweetSerializer
from serializers.s3ChunkedTweetSerializer import S3ChunkedTweetSerializer
from serializers.consoleTweetSerializer import ConsoleTweetSerializer
from twitterutils import apikeys
import argparse  

parser = argparse.ArgumentParser(description = 'Search the Twitter API')
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
					help='MongoDB server to serialize data')
parser.add_argument('-t', '--track',
					type=str,
					required=True,
					help='Twitter phrases to search')
parser.add_argument('-s3','--s3url',
					type=str,
					required=False,
					default=None,
					help='Amazon S3 storage to store tweets')
parser.add_argument('-si','--since',
					type=str,
					required=False,
					default=None,
					help='lower bound search date YYYY-MM-DD')
parser.add_argument('-u','--until',
					type=str,
					required=False,
					default=None,
					help='upper bound search date YYYY-MM-DD')

args = parser.parse_args()

fetchSize = 1500

# pick the serializer
if args.database:
	if (args.collection):
		serializer = MongoTweetSerializer(args.server, args.database , args.collection)
	else:
		print "MongoDB database and collection must be specified when using MongoDB server"
		sys.exit(1)
elif args.s3url:
	serializer = S3ChunkedTweetSerializer(args.s3url)
else:
	print "Writing tweets to console..."
	serializer = ConsoleTweetSerializer()
	fetchSize = 10

# time and date formats
xsdDatetimeFormat = "%Y-%m-%dT%H:%M:%S"
xsdDateFormat = "%Y-%m-%d"

def datetime_partition(start,end,duration):
   current = start
   while start==current or (end-current).days > 0 or ((end-current).days==0 and (end-current).seconds>0):
      yield current
      current = current + duration
      
def date_partition(start,end):
   return datetime_partition(start,end,datetime.timedelta(days=1))

# if __name__ == "__main__":
#   start = datetime.datetime.strptime(sys.argv[1],xsdDateFormat) # start date
#   end = datetime.datetime.strptime(sys.argv[2],xsdDateFormat)   # end date
   
#   for d in date_partition(start,end):
	
q = urllib.quote(args.track)

if (args.since):
	start = datetime.datetime.strptime(args.since,xsdDateFormat) # start date
	q = q + " since:" + start.strftime(xsdDateFormat)

if(args.until):
		end = datetime.datetime.strptime(args.until,xsdDateFormat)   # end date
		q = q + " until:" + end.strftime(xsdDateFormat)

print "Searching for: " + q

fetcher = tweetFetcher.TweetFetcher(serializer, apikeys.W205_ASSIGNEMNT3, fetchSize=fetchSize)
fetcher.search(q)