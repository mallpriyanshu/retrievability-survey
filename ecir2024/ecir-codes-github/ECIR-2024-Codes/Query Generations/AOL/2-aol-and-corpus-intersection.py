# imports
from tqdm import tqdm
import argparse

# lucene imports
import lucene
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher
from java.io import File
from org.apache.lucene.queryparser.classic import QueryParser

# lucene init Virtual Machine
lucene.initVM()

# function to check if word is in index
def is_word_in_index(searcher, analyzer, fieldname, word):
    escaped_word = QueryParser(fieldname, analyzer).escape(word)
    query = QueryParser(fieldname, analyzer).parse(escaped_word)
    top_docs = searcher.search(query, 1)
    return top_docs.totalHits.value > 0

# function to filter queries
def filter_query(searcher, analyzer, fieldname, query):
    for term in query.split():
        if not is_word_in_index(searcher, analyzer, fieldname, term):
            return False
    return True

# parallelize filter_query using multiprocessing.Pool
def filter_queries(queries, index_dir):
    # initializing lucene index searcher
    FIELDNAME = 'CONTENT'
    indexPath = File(index_dir).toPath()
    indexDir = FSDirectory.open(indexPath)
    reader = DirectoryReader.open(indexDir)
    searcher = IndexSearcher(reader)
    analyzer = EnglishAnalyzer()
    
    results = [filter_query(searcher,analyzer,FIELDNAME,query) for query in tqdm(queries)]
    return [query for query, result in tqdm(zip(queries, results)) if result]


def main(queries_path, index_dir, output_path):
    with open(queries_path, 'r') as f:
        unique_aol_queries = f.read().splitlines()
    
    # remove queries with dot(s) in them
    dot_clean_queries = [query for query in unique_aol_queries if '.' not in query]
    
    # only include queries with all terms present in the Wikipedia vocabulary
    clean_aol_queries = filter_queries(queries=dot_clean_queries, index_dir=index_dir)
    
    with open(output_path, 'w') as f:
        f.write('\n'.join(clean_aol_queries)+'\n')


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--queries", type=str, required=True, help="AOL queries txt file's path")
    parser.add_argument("-j", "--index_dir", type=str, required=True, help="Lucene index directory path")
    parser.add_argument("-o", "--output", type=str, required=True, help="Cleaned queries output path")
    args = parser.parse_args()
    
    main(args.queries, args.index_dir, args.output)