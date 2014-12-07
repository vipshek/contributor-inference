import sys
import multiprocessing as mp
from collections import Counter
import heapq
import random

class User:
	def __init__(self, id, name=""):
		self.id = id
		self.name = name
		self.edits = set()
		self.articles = set()
		self.out_talks = set()

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
		u.articles.add(article_id)

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
		self.users[user_id].out_talks.add(other_user_id)

	def count_user_edges(self):
		count = 0
		for id,user in self.users.iteritems():
			count += len(user.out_talks)
		return count

	def has_user_edge(self,user_id,other_user_id):
		if user_id not in self.users:
			return False
		return other_user_id in self.users[user_id].out_talks

	def get_user_article_dict(self):
		user_articles = {}
		for uid,user in self.users.iteritems():
			user_articles[uid] = user.articles
		return user_articles.items()


def load_file(infile):
	g = Graph()
	with open(infile,"r") as f:
		for line in f:
			article_id,rev_id,user_id,username,title,timestamp,category,minor,word_count = line.split(' ')
			g.add_edit(int(article_id[1:]),int(rev_id[1:]),int(user_id[1:]),username,title,timestamp,category,minor,word_count)
	return g

def load_talk(infile):
	g = Graph()
	with open(infile,"r") as f:
		for line in f:
			if line[0] == "#":
				continue
			source,sink = line[:-1].split('\t')
			g.add_user_edge(int(source),int(sink))
			g.add_user_edge(int(sink),int(source))
	return g

def train_process(user_articles,talk_g,i,folds,trq,talkrq):
	triangles = Counter()
	connections = Counter()

	for i in range(i,len(user_articles),folds):
		uid, uarticles = user_articles[i]
		if i != len(user_articles):
			for vid, varticles in user_articles[i+1:]:
				t = len(uarticles & varticles)
				triangles[t] += 1
				if talk_g.has_user_edge(uid,vid) or talk_g.has_user_edge(vid,uid):
					connections[t] += 1
	
	trq.put(triangles)
	talkrq.put(connections)

def train(meta_g,talk_g,folds):
	user_articles = meta_g.get_user_article_dict()

	trq = mp.Queue()
	talkrq = mp.Queue()
	jobs = [mp.Process(target = train_process, args = (user_articles,talk_g,i,folds,trq,talkrq)) for i in range(folds)]
	for job in jobs: job.start()
	for job in jobs: job.join()

	triangle_results = [trq.get() for job in jobs]
	talk_results = [talkrq.get() for job in jobs]

	triangle_counter = reduce(lambda x,y: x+y,triangle_results,Counter())
	talk_counter = reduce(lambda x,y: x+y,talk_results,Counter())

	with open("data/triangle_counts.txt","w") as f:
		for x,y in triangle_counter.most_common():
			f.write("%d %d\n" % (x,y))
	
	with open("data/talk_counts.txt","w") as f:
		for x,y in talk_counter.most_common():
			f.write("%d %d\n" % (x,y))

def test_process(users,k,i,folds,result_queue):
	heap = []
	for i in range(i,len(users),folds):
		uid,u = users[i]
		for vid,v in users[i+1:]:
			t = len(u.common_with(v))
			if len(heap) < k:
				heapq.heappush(heap,(t,uid,vid))
			else:
				heapq.heappushpop(heap,(t,uid,vid))
	result_queue.put(heap)


def test(meta_g,talk_g,folds,k=10):
	users = meta_g.users.items()

	result_queue = mp.Queue()
	jobs = [mp.Process(target = test_process, args = (users,k,i,folds,result_queue)) for i in range(folds)]
	for job in jobs: job.start()
	for job in jobs: job.join()

	results = [result_queue.get() for job in jobs]

	heap = []
	for result in results:
		for item in result:
			if len(heap) < k:
				heapq.heappush(heap,item)
			else:
				heapq.heappushpop(heap,item)
	
	correct = 0
	for (_,uid,vid) in heap:
		if talk_g.has_user_edge(uid,vid) or talk_g.has_user_edge(vid,uid):
			correct += 1

	print "%d correct / %d = %f" % (correct, k, correct / float(k))

def main(meta_infile,talk_infile,folds):
	g = load_file(meta_infile)
	talk_g = load_talk(talk_infile)
	print "Loaded graphs"

	test(g,talk_g,folds)

if __name__ == '__main__':
	main(sys.argv[1],sys.argv[2],int(sys.argv[3]))
