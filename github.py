import sys
import multiprocessing as mp
from collections import Counter
import heapq
import random

class User:
	def __init__(self, id):
		self.id = id
		self.commits = set()
		self.repos = set()
		self.out_follows = set()

	"""
	def repos(self):
		result = set()
		for edit in commits:
			result.add(edit.repo)
		return result
	"""

	def cocommitters(self):
		result = set()
		for repo in self.repos:
			result.update(repo.users())
		return result

	def common_with(self,other):
		return self.repos & other.repos

class Commit:
	def __init__(self, id):
		self.id = id
		self.commits = set()
		self.users = set()

	"""
	def users(self):
		result = set()
		for edit in commits:
			result.add(edit.user)
		return result
	"""

class Edit:
	def __init__(self, id, user, repo, timestamp):
		self.id = id
		self.user = user
		self.repo = repo
		self.timestamp = timestamp

class Graph:
	def __init__(self):
		self.users = {}
		self.repos = {}
		self.commits = {}

	def add_edit(self,repo_id,user_id):
		if user_id not in self.users:
			self.users[user_id] = User(user_id)
		#if repo_id not in self.repos:
		#	self.repos[repo_id] = Commit(repo_id)

		u = self.users[user_id]
		#a = self.repos[repo_id]

		#e = Edit(edit_id,u,a,timestamp)
		#self.commits[edit_id] = e

		#u.commits.add(e)
		u.repos.add(repo_id)

		#a.commits.add(e)
		#a.users.add(u)

	def add_user_edge(self,user_id,other_user_id):
		# Ensure source exists
		if user_id not in self.users:
			self.users[user_id] = User(user_id)
		# Ensure sink exists
		if other_user_id not in self.users:
			self.users[other_user_id] = User(other_user_id)
		# Add edge from source to sink
		self.users[user_id].out_follows.add(other_user_id)

	def count_user_edges(self):
		count = 0
		for id,user in self.users.iteritems():
			count += len(user.out_follows)
		return count

	def has_user_edge(self,user_id,other_user_id):
		if user_id not in self.users:
			return False
		return other_user_id in self.users[user_id].out_follows

	def get_user_repo_dict(self):
		user_repos = {}
		for uid,user in self.users.iteritems():
			user_repos[uid] = user.repos
		return user_repos.items()


def load_file(infile):
	g = Graph()
	with open(infile,"r") as f:
		for line in f:
			repo_id,user_id,timestamp,public = line[:-1].split(',')
			g.add_edit(int(repo_id),int(user_id))
	return g

def load_follow(infile):
	g = Graph()
	with open(infile,"r") as f:
		for line in f:
			source,sink = line[:-1].split()
			g.add_user_edge(int(source),int(sink))
			g.add_user_edge(int(sink),int(source))
	return g

def train_process(user_repos,follow_g,i,folds,trq,followrq):
	triangles = Counter()
	connections = Counter()

	for i in range(i,len(user_repos),folds):
		uid, urepos = user_repos[i]
		if i != len(user_repos):
			for vid, vrepos in user_repos[i+1:]:
				t = len(urepos & vrepos)
				triangles[t] += 1
				if follow_g.has_user_edge(uid,vid) or follow_g.has_user_edge(vid,uid):
					connections[t] += 1
	
	trq.put(triangles)
	followrq.put(connections)

def train(meta_g,follow_g,folds):
	user_repos = meta_g.get_user_repo_dict()

	trq = mp.Queue()
	followrq = mp.Queue()
	jobs = [mp.Process(target = train_process, args = (user_repos,follow_g,i,folds,trq,followrq)) for i in range(folds)]
	for job in jobs: job.start()
	for job in jobs: job.join()

	triangle_results = [trq.get() for job in jobs]
	follow_results = [followrq.get() for job in jobs]

	triangle_counter = reduce(lambda x,y: x+y,triangle_results,Counter())
	follow_counter = reduce(lambda x,y: x+y,follow_results,Counter())

	with open("data/github_triangle_counts.txt","w") as f:
		for x,y in triangle_counter.most_common():
			f.write("%d %d\n" % (x,y))
	
	with open("data/github_follow_counts.txt","w") as f:
		for x,y in follow_counter.most_common():
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

def cond_prob(t):
	# Distribution fitted to GitHub data
	return (1 - math.exp(-0.03777928734044 * t))

def infer_process(users,i,folds,result_queue):
	edges = []
	for i in range(i,len(users),folds):
		uid,u = users[i]
		for vid,v in users[i+1:]:
			t = len(u.common_with(v))
			p = cond_prob(t)
			if p > 0.5:
				edges.append((uid,vid))
	result_queue.put(edges)

def infer(meta_g,talk_g,folds):
	users = meta_g.users.items()

	result_queue = mp.Queue()
	jobs = [mp.Process(target = infer_process, args = (users,i,folds,result_queue)) for i in range(folds)]
	for job in jobs: job.start()
	for job in jobs: job.join()
	results = [result_queue.get() for job in jobs]

	correct, wrong, count = 0, 0, 0
	for result in results:
		for (uid,vid) in result:
			count += 1
			if talk_g.has_user_edge(uid,vid) or talk_g.has_user_edge(vid,uid):
				correct += 1
			else:
				wrong += 1
	precision = float(correct) / (correct + wrong)
	recall = float(correct) / talk_g.count_user_edges()

	print "Precision: %f" % precision
	print "Recall:    %f" % recall

def main(commit_infile,follow_infile,folds):
	g = load_file(commit_infile)
	follow_g = load_follow(follow_infile)
	print "Loaded graphs"

	infer(g,follow_g,folds)

if __name__ == '__main__':
	main(sys.argv[1],sys.argv[2],int(sys.argv[3]))
