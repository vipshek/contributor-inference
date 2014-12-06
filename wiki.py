import sys
from collections import Counter
import multiprocessing as mp

class User:
	def __init__(self, id, name=""):
		self.id = id
		self.name = name
		self.edits = set()
		self.articles = set()
		self.out_talks = set()

	"""
	def articles(self):
		result = set()
		for edit in edits:
			result.add(edit.article)
		return result
	"""

	def cocontributors(self):
		result = set()
		for article in self.articles:
			result.update(article.users())
		return result

	def common_with(self,other):
		return self.articles & other.articles

class Article:
	def __init__(self, id, title, category):
		self.id = id
		self.title = title
		self.category = category
		self.edits = set()
		self.users = set()

	"""
	def users(self):
		result = set()
		for edit in edits:
			result.add(edit.user)
		return result
	"""

class Edit:
	def __init__(self, id, user, article, timestamp, minor, wc):
		self.id = id
		self.user = user
		self.article = article
		self.timestamp = timestamp
		self.minor = minor
		self.wc = wc

class Graph:
	def __init__(self):
		self.users = {}
		self.articles = {}
		self.edits = {}

	def add_edit(self,article_id,edit_id,user_id,name,title,timestamp,category,minor,wc):
		if user_id not in self.users:
			self.users[user_id] = User(user_id,name)
		#if article_id not in self.articles:
		#	self.articles[article_id] = Article(article_id,title,category)

		u = self.users[user_id]
		#a = self.articles[article_id]

		#e = Edit(edit_id,u,a,timestamp,minor,wc)
		#self.edits[edit_id] = e

		#u.edits.add(e)
		u.articles.add(int(article_id[1:]))

		#a.edits.add(e)
		#a.users.add(u)

	def add_user_edge(self,user_id,other_user_id):
		# Ensure source exists
		if user_id not in self.users:
			self.users[user_id] = User(user_id)
		# Ensure sink exists
		if other_user_id not in self.users:
			self.users[other_user_id] = User(other_user_id)
		# Add edge from source to sink
		self.users[user_id].out_talks.add(self.users[other_user_id])

	def count_user_edges(self):
		count = 0
		for id,user in self.users.iteritems():
			count += len(user.out_talks)
		return count

def load_file(infile):
	g = Graph()
	with open(infile,"r") as f:
		for line in f:
			article_id,rev_id,user_id,username,title,timestamp,category,minor,word_count = line.split(' ')
			g.add_edit(article_id,rev_id,user_id,username,title,timestamp,category,minor,word_count)
	return g

def load_talk(infile):
	g = Graph()
	with open(infile,"r") as f:
		for line in f:
			if line[0] == "#":
				continue
			source,sink = line[:-1].split('\t')
			g.add_user_edge(int(source),int(sink))
	return g

def f(user_articles,i,folds,result_queue):
	triangles = Counter()
	for i in range(i,len(user_articles),folds):
		_, uarticles = user_articles[i]
		if i != len(user_articles):
			for _, varticles in user_articles[i+1:]:
				t = len(uarticles & varticles)
				triangles[t] += 1
	result_queue.put(triangles)

def main(infile,folds):
	g = load_file(infile)
	user_articles = {}
	for uid,user in g.users.iteritems():
		user_articles[uid] = user.articles
	user_articles = user_articles.items()

	result_queue = mp.Queue()
	jobs = [mp.Process(target = f, args = (user_articles,i,folds,result_queue)) for i in range(folds)]
	for job in jobs: job.start()
	for job in jobs: job.join()
	results = [result_queue.get() for job in jobs]
	c = reduce(lambda x,y: x+y,results,Counter())
	print c

if __name__ == '__main__':
	main(sys.argv[1],int(sys.argv[2]))