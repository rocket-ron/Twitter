from pymongo import MongoClient
import json

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

	def end(self):
		if (self._client != None):
			self._client.close()

	def initPersistance(self, database, collection):
		self._client = MongoClient('127.0.0.1', port=27017)
		self._collection = self._client[str(database)][str(collection)]
