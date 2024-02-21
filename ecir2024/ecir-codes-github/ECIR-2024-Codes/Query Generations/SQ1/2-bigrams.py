import os
import pickle
import json
from collections import Counter
from tqdm import tqdm

root_dir = './counters-dumps/'
step = 1000000
dir_names = [f'{l[0]}-{l[-1]}' for l in (range(6584626)[i:i+step] for i in range(6584626)[::step])]
paths = [os.path.join(root_dir, dir_name) for dir_name in dir_names]

def load_and_accumulate_bigram_counter(paths):
    total_bigram_counter = Counter()
    for path in tqdm(paths):
        with open(os.path.join(path, 'bigram_counter.pickle'), 'rb') as f:
            bigram_counter = pickle.load(f)
        top_bigrams = Counter({bigram:tf for bigram, tf in bigram_counter.most_common(10000000)})
        del bigram_counter
        # total_bigram_counter += bigram_counter
        total_bigram_counter += top_bigrams
    return total_bigram_counter

total_bigram_counter = load_and_accumulate_bigram_counter(paths)

with open('./counters-dumps/total/bigram_counter.pickle', 'wb') as f:
    pickle.dump(total_bigram_counter, f)

print('\ntotal_bigram_counter dumped!')

bigram_filtered_counter = Counter({bigram: tf for bigram, tf in tqdm(total_bigram_counter.items()) if tf >= 20})

del total_bigram_counter

print("len of tf>=20 filtered bigrams =", len(bigram_filtered_counter))

top_counter = bigram_filtered_counter.most_common(2000000)
top_ngrams = [query for query, _ in tqdm(top_counter)]

# Save as JSON
with open('./final-queries/bigram-queries.json', 'w') as f:
    json.dump(top_ngrams, f)

print('top 2 million bigrams dumped!')
