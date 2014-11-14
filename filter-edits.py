import sys

def main(infile,outfile):
	with open(infile,"r") as inf, open(outfile,"w") as outf:
		# String to format output to outfile
		format_string = "%s %s %s %s %s %s %s %s\n"
		while True:
			# Read 14 lines at a time for wikipedia dataset
			edit = [inf.readline() for i in range(14)]
			# Break if we've reached the end of file
			if edit[13] == "":
				break
			# Parse data from revision line
			revision = edit[0].split(' ')
			article_id,rev_id,timestamp,username,user_id = 'a'+revision[1],'e'+revision[2],revision[4],revision[5],'u'+revision[6].strip()
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
			# Create output line and write to outfile
			outline = format_string % (article_id,rev_id,user_id,username,timestamp,category,minor,word_count)
			outf.write(outline)

if __name__ == '__main__':
	main(sys.argv[1],sys.argv[2])