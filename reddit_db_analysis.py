import mysql.connector
from mysql.connector import Error
import pandas as pd
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import nltk
from wordcloud import WordCloud, STOPWORDS
import re
import numpy as np
import matplotlib.pyplot as plt

# Connect to database
password = input('Enter password:\n')

def connect_database(db_name):
	# Credentials to database
	config = {
		'user'	  : 'root',
		'host'	  : 'localhost',
		'password': password,
		'database': db_name
	}

	try:
		conn = mysql.connector.connect(**config)
		if conn.is_connected():
			print('connected successfully')
		
			cursor = conn.cursor()
			cursor.execute("SELECT * FROM company;")
			all_rows = cursor.fetchall()

			# store in dataframe
			df = pd.DataFrame(all_rows, columns = ['id', 'title', 'subreddit', 'author'])

	except Error as e:
		print(e)

	cursor.close()
	conn.close()

	return df

def clean_data(df):
		'''
		Cleans raw data and prepares for analysis
		(remove stopwords, punctuation, lower case, html etc.)
		'''

		# text preprocessing
		# nltk.download("stopwords")
		# nltk.download('wordnet')
		stopword_list = stopwords.words('english')
		wordnet_lemmatizer = WordNetLemmatizer()
		df['clean_titles'] = None
		df['length'] = None
		for i in range(0, len(df['subreddit'])):
			# removes anything that is not a letter
			exclusion_list = ['[^a-zA-Z]', 'http']
			exclusions = '|'.join(exclusion_list)
			text = re.sub(exclusions, ' ', df['subreddit'][i])
			text = text.lower()
			words = text.split()
			words = [wordnet_lemmatizer.lemmatize(word) for word in words if not word in stopword_list]
			df['clean_titles'][i] = ' '.join(words)

		# Create column with data length
		df['length'] = np.array([len(subreddit) for subreddit in df['clean_titles']])

		return df

# formated data visual processing
def word_cloud(df):
	'''
	Takes in dataframe and plots a wordcloud using matplotlib
	'''
	plt.subplots(figsize = (12,10))
	wordcloud = WordCloud(
						background_color = 'green',
						width = 1000,
						height = 1000).generate(' '.join(df['clean_titles']))

	plt.imshow(wordcloud)
	plt.axis('off')
	plt.show()

if __name__ == "__main__":
	db_name = input('Enter database name:\n')
	connect_db = clean_data(connect_database(db_name))
	analysis = word_cloud(connect_db)
	print(analysis)