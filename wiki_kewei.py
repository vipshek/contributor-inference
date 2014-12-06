from wiki import load_file
import sys

def load_users():
    users = set()
    with open('wiki-Talk.txt') as fp:
        for line in fp:
            if line[0] != '#':
                line = map(int,line[:-1].split())
                users.add(line[0])
                users.add(line[1])
    print len(users)
    return users

def main(infile):
    g = load_file(infile)


if __name__ == '__main__':
    g = main(sys.argv[1])