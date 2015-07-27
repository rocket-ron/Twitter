class ConsoleTweetSerializer():

	def __init__(self):
		pass

	def write(self, tweet):
		try:
			print '@%s: %s' % (tweet['user']['screen_name'], tweet['text'].encode('ascii','ignore'))
		except:
			pass

	def end(self):
		pass