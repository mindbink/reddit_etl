import time
import requests
import mysql.connector

# Extract data from reddit news feed
source = 'https://www.reddit.com/r/all/new/.json'
headers = {'User-Agent': 'etl pipeline v.1'}

def extract():
	# put records into buffer to avoid duplicates
	buffer = []
	while True:
		time.sleep(1)
		# keep buffer limited size to avoid growing indefinitely
		buffer = buffer[-1000:]
		res = requests.get(source, headers=headers)
		for record in r.json()['data']['children']:
			record = record['data']
			if record['permalink'] not in buffer:
				buffer.append(record['permalink'])
				yield record

# Transform extracted data

# create field titles
titles = ['title', 'subreddit', 'author_fullname']
rename = {'author_fullname': 'author'}

def transform(record):
	# Clean entry
	key_list = list(record.keys())
	for key in key_list:
		if key not in titles:
			del record[key]

	# Rename dictionary keys/value
	for old_key, new_key in rename.items():
		record[new_key] = record.pop(old_key)

	return record

# Load data
def load(record, conn):
	a = str(record['title'])
	b = str(record['author'])
	c = str(record['subreddit'])
	
	add_submission = '''INSERT INTO COMPANY (TITLE, AUTHOR, SUBREDDIT) 
						VALUES (%s, %s, %s)
	'''
	cursor.execute(add_submission, (a, b, c))
	conn.commit()

# Create database schema
def create_database(conn):

	cursor.execute("CREATE DATABASE {}".format(conn))

	cursor.execute("USE {}".format(conn))
	
	cursor.execute('''
		CREATE TABLE COMPANY (
		ID INT AUTO_INCREMENT PRIMARY KEY,
		SUBREDDIT VARCHAR(255)  NOT NULL,
		TITLE     VARCHAR(1000) NOT NULL,
		AUTHOR    VARCHAR(255)  NOT NULL
		);
		''')

# Credentials to database connection
password = input('Enter password:\n')
config = {
	'user'    : 'root',
	'host'    : 'localhost',
	'password': password
}

def run(db_name):
	conn = mysql.connector.connect(**config)
	global cursor
	cursor = conn.cursor()
	create_database(db_name)
	count = 0
	for record in extract():
		record = transform(record)
		load(record, conn)
		count += 1
		print('Records inserted in database: {}'.format(count), end='\r')


if __name__ == "__main__":
	db_name = input('Enter database name:\n')
	run_etl = run(db_name)