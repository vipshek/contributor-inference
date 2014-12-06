import sys

def main():
	# String to format output
	format_string = "%s %s %s %s %s %s %s %s %s\n"
	while True:
		# Read 14 lines at a time from stdin for wikipedia dataset
		edit = [sys.stdin.readline() for i in range(14)]
		# Break if we've reached the end of stdin
		if edit[13] == "":
			break
		# Parse data from revision line
		revision = edit[0].split(' ')
		article_id,rev_id,title,timestamp,username,user_id = 'a'+revision[1],'e'+revision[2],revision[3],revision[4],revision[5],'u'+revision[6].strip()
		# Ignore anonymous edits
		if user_id.startswith('uip'):
			continue
		# Parse article category
		category_line = edit[1].split(' ')
		if len(category_line) != 1:
			category = category_line[1].strip()
		else:
			category = ""
		# Parse whether edit is minor and number of words edited
		minor = edit[11].split(' ')[1].strip()
		word_count = edit[12].split(' ')[1].strip()
		# Create output line and write to stdout
		outline = format_string % (article_id,rev_id,user_id,username,title,timestamp,category,minor,word_count)
		sys.stdout.write(outline)

if __name__ == '__main__':
	main()