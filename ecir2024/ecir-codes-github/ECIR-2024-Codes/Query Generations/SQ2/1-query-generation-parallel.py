# imports
import pickle
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
import multiprocessing
import itertools
import os
from collections import Counter

# nltk imports
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.util import ngrams

# lucene imports
import lucene
from org.apache.lucene.search import IndexSearcher, BooleanClause, BooleanQuery, TermQuery
from org.apache.lucene.index import DirectoryReader, Term, PostingsEnum
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.util import BytesRefIterator
from org.apache.lucene.analysis.standard import StandardAnalyzer
from java.io import File


def generate_queries(indexPath, FIELDNAME):
    lucene.initVM()
    
    # Open the index directory
    directory = FSDirectory.open(File(indexPath).toPath())
    reader = DirectoryReader.open(directory)
    
    total_docs = reader.numDocs()
    queries = []
    
    en_stopwords = set(stopwords.words('english'))
    stemmer = PorterStemmer()
    stemmed_stopwords = [stemmer.stem(stopword) for stopword in en_stopwords]
    
    for doc_id in tqdm(range(total_docs)):
        eligible_terms = []
        terms = reader.getTermVector(doc_id, FIELDNAME)
        
        tterms = {}
        if terms is not None:
            terms_enum = terms.iterator()
            for term in BytesRefIterator.cast_(terms_enum):
                term = terms_enum.term()
                term_text = term.utf8ToString()
                # skip stopwords
                if term_text in stemmed_stopwords:
                    continue
                df = reader.docFreq(Term(FIELDNAME, term_text))    # docFreq of term,t
                # Remove terms with document frequency > 25% of collection size
                if df > int(total_docs * 0.25):
                    continue
                postings_enum = terms_enum.postings(None)
                while postings_enum.nextDoc() != PostingsEnum.NO_MORE_DOCS:
                    freq = postings_enum.freq()
                tterms[term_text] = freq
        
        eligible_terms = list(dict(Counter(tterms).most_common(5)).keys())
        
        # Generate 3-terms and 4-terms combinations
        combinations = list(itertools.chain.from_iterable(
            itertools.combinations(eligible_terms, r) for r in range(3, 5)
        ))
        queries.extend(combinations)
    
    # Remove duplicate queries
    unique_queries = set(queries)
    
    # Clean up
    reader.close()
    directory.close
    
    return unique_queries


def main():
    corpus = 'Wikipedia'
    FIELDNAME = 'CONTENT'       # Lucene index field name for content of the doc
    index_path = '../Wikipedia-pages/index-enwiki'   # Lucene index directory path
    
    queries = generate_queries(index_path, FIELDNAME)

    
    # dump the total counters for each ngrams for further processing and filtering
    # output_dir = corpus
    directory_name = 'final-queries'
    # directory_path = os.path.join(output_dir, directory_name)
    directory_path = directory_name
    os.makedirs(directory_path, exist_ok=True)
    with open(os.path.join(directory_path, 'bashir-khattak-queries.pickle'), 'wb') as f:
        pickle.dump(queries, f)


if __name__=='__main__':
    main()