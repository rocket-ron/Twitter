from pymongo import MongoClient
import json
import StringIO

# tweet serializer class from the activities document
class MongoTweetSerializer():

	_collection = None
	_client = None

	def __init__(self, database, collection):
		self.initPersistance(database, collection)

	def write(self, tweet):
		if (self._collection == None):
			initPersistance()
		
		self._collection.insert_one(tweet._json)


	def initPersistance(self, database, collection):
		password = 'password'
		self._client = MongoClient('mongodb://testuser:' + password + '@192.168.194.167',port=27017)
		self._collection = self._client['database']['collection']
