'''
	Add all ngrams into one JSON file for ALL Artificial Queries
'''

import os
import pickle
import json

# Set the directory path
filePath = './final-queries/bashir-khattak-queries.pickle'

# Load the Pickle file
with open(filePath, 'rb') as f:
    queries = pickle.load(f)
queries = list(queries)

# all queries in one list
all_queries = []

num = 20000000
all_queries = [' '.join(q) for q in queries[4*num:4*num+num]]

with open('./final-queries/bashir-all-queries-4.json', 'w') as f:
    json.dump(all_queries, f)

len(all_queries)
