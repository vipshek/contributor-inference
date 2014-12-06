import sys

class User:
	def __init__(self, id, name):
		self.id = id
		self.name = name
		self.edits = set()

	def articles(self):
		result = set()
		for edit in edits:
			result.add(edit.article)
		return result

	def cocontributors(self):
		result = set()
		for article in self.articles():
			result.update(article.users())
		return result

	def common_with(self,other):
		return self.articles() & other.articles()

class Article:
	def __init__(self, id, title, category):
		self.id = id
		self.title = title
		self.category = category
		self.edits = set()

	def users(self):
		result = set()
		for edit in edits:
			result.add(edit.user)
		return result

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
		if article_id not in self.articles:
			self.articles[article_id] = Article(article_id,title,category)

		u = self.users[user_id]
		a = self.articles[article_id]

		e = Edit(edit_id,u,a,timestamp,minor,wc)
		self.edits[edit_id] = e

		u.edits.add(e)
		a.edits.add(e)

def load_file(infile):
	g = Graph()
	with open(infile,"r") as f:
		for line in f:
			article_id,rev_id,user_id,username,title,timestamp,category,minor,word_count = line.split(' ')
			g.add_edit(article_id,rev_id,user_id,username,title,timestamp,category,minor,word_count)
	return g

def main(infile):
	g = load_file(infile)

if __name__ == '__main__':
	main(sys.argv[1])