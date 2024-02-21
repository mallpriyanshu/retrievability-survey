# imports
import pickle
import json
from tqdm import tqdm
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
import multiprocessing
import itertools
import os

# nltk imports
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.tokenize.regexp import blankline_tokenize
from nltk.corpus import stopwords
from nltk.util import ngrams

# lucene imports
import lucene
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.store import FSDirectory, SimpleFSDirectory
from java.io import File


# start lucene virtual machine
lucene.initVM()


# This class returns a corpus document field content generator (as an iterator)
class MyCorpus:
    def __init__(self, indexPath, fieldname, start_docid, end_docid):
        # Corpus documents directory path
        directory = FSDirectory.open(File(indexPath).toPath())
        self.indexReader = DirectoryReader.open(directory)
        self.numDocs = self.indexReader.numDocs()   # no. docs in English Wikipedia or its index
        self.FIELDNAME = fieldname
        self.bad_docid = 1053350
        self.start_docid = start_docid
        self.end_docid = end_docid
    
    def __iter__(self):
        # excluded bad_docid document 
        # which was throwing `SystemError: invalid maximum character passed to PyUnicode_New`
        range_without_bad_docid = (docid for docid in range(self.start_docid, self.end_docid+1) if docid != self.bad_docid)
        
        for luceneDocid in range_without_bad_docid:
            yield luceneDocid, self.indexReader.document(luceneDocid).get(self.FIELDNAME)


def get_directory_path(output_dir, docid):
    """
    Returns a directory path for a docid with directory name in a step of 1000 which encloses that docid.
    """
    directory_name = str((docid // 100000) * 100000) + '-' + str((((docid // 100000) + 1) * 100000)-1)
    directory_path = os.path.join(output_dir, directory_name)
    return directory_path

def get_json_docid_range(docid):
    """
    Returns a directory path for a docid with directory name in a step of 1000 which encloses that docid.
    """
    file_range = str((docid // 1000) * 1000) + '-' + str((((docid // 1000) + 1) * 1000)-1)
    return file_range


def batch_buffer_save(batch_buffer):
    # write to disk every batch_size documents
    for docid, doc_ngrams in batch_buffer:
        output_dir = './doc-query-dumps'
        output_dir = get_directory_path(output_dir, docid)
        os.makedirs(output_dir, exist_ok=True)
        docid_range = get_json_docid_range(docid)
        dict_of_docid = {}
        for ngram_type, ngram_counter in doc_ngrams:
            dict_of_docid[ngram_type] = ngram_counter
        with open(os.path.join(output_dir, f'counters_docs_{docid_range}.json'), 'a') as f:
            json.dump({docid: dict_of_docid}, f)
            f.write('\n')


def process_one_document(doc):
    # text pre-processing:
    # blankline tokenization, then sentence tokenization, then word tokenization
    sents_nested = [sent_tokenize(ss) for ss in blankline_tokenize(doc)]
    sents = [sent for sublist in sents_nested for sent in sublist]
    tokenized_sents = [word_tokenize(s) for s in sents]

    # POS tagging
    tagged_sents = nltk.tag.pos_tag_sents(tokenized_sents, tagset='universal')

    # to store ngrams of the given doc with their frequencies
    doc_unigram_counter = Counter()
    doc_bigram_counter = Counter()
    doc_trigram_counter = Counter()
    doc_quadgram_counter = Counter()

    # sampling ngrams from each sentence
    for tagged_sent in tagged_sents:
        unigrams = ngrams(tagged_sent, 1)
        bigrams = ngrams(tagged_sent, 2)
        trigrams = ngrams(tagged_sent, 3)
        quadgrams = ngrams(tagged_sent, 4)
        
        # non-alphabetical ngram removal
        unigrams = [ele[0] for ele in unigrams if ele[0][0].isalpha()]
        bigrams = [bigram for bigram in bigrams if all(term.isalpha() for term,tag in bigram)]
        trigrams = [trigram for trigram in trigrams if all(term.isalpha() for term,tag in trigram)]
        quadgrams = [quadgram for quadgram in quadgrams if all(term.isalpha() for term,tag in quadgram)]
        
        # collocation POS filters
        unigram_tags = ['NOUN']
        bigram_tags = [('ADJ','NOUN'),('NOUN','NOUN')]
        trigram_tags = [('ADJ','ADJ','NOUN'),('ADJ','NOUN','NOUN'),('NOUN','ADJ','NOUN'), \
            ('NOUN','NOUN','NOUN'),('NOUN','ADP','NOUN')]
        quadgram_tags = [('NOUN','VERB','ADP','NOUN'),('NOUN','VERB','NOUN','NOUN'),('ADJ','NOUN','ADJ','NOUN'), \
            ('ADV','ADJ','NOUN','NOUN'),('NOUN','ADP','ADJ','NOUN'), \
            ('ADJ','NOUN','VERB','NOUN'),('NOUN','NOUN','ADP','NOUN'),('NOUN','ADJ','NOUN','NOUN')]
        
        # doing POS filteration and lowercasing
        unigrams = [unigram.lower() for unigram,tag in unigrams if any(tag==ut for ut in unigram_tags)]
        bigrams = [' '.join(term.lower() for term,tag in bigram) for bigram in bigrams if any([all(btgs[i]==bigram[i][1] for i in range(len(bigram))) for btgs in bigram_tags])]
        trigrams = [' '.join(term.lower() for term,tag in trigram) for trigram in trigrams if any([all(ttgs[i]==trigram[i][1] for i in range(len(trigram))) for ttgs in trigram_tags])]
        quadgrams = [' '.join(term.lower() for term,tag in quadgram) for quadgram in quadgrams if any([all(qtgs[i]==quadgram[i][1] for i in range(len(quadgram))) for qtgs in quadgram_tags])]
        
        # Add to doc ngrams Counters
        doc_unigram_counter.update(unigrams)
        doc_bigram_counter.update(bigrams)
        doc_trigram_counter.update(trigrams)
        doc_quadgram_counter.update(quadgrams)
        
    return doc_unigram_counter, doc_bigram_counter, doc_trigram_counter, doc_quadgram_counter


def process_documents(docs):
    # related to saving doc counters
    batch_buffer = []
    
    # list of counters for this batch of documents
    batch_unigram_counter = Counter()
    batch_bigram_counter = Counter()
    batch_trigram_counter = Counter()
    batch_quadgram_counter = Counter()
    
    for luceneDocid, doc in docs:
        doc_unigram_counter, doc_bigram_counter, doc_trigram_counter, doc_quadgram_counter = process_one_document(doc)
        
        # save these ngrams counters for later evaluation of normalization factor of this doc (for normalized retrievability)
        this_doc_ngrams = []
        this_doc_ngrams.append(('unigram', doc_unigram_counter))
        this_doc_ngrams.append(('bigram', doc_bigram_counter))
        this_doc_ngrams.append(('trigram', doc_trigram_counter))
        this_doc_ngrams.append(('quadgram', doc_quadgram_counter))
        
        # add to the batch_buffer
        batch_buffer.append((luceneDocid,this_doc_ngrams))
        
        # adding into batch counters
        batch_unigram_counter += doc_unigram_counter
        batch_bigram_counter += doc_bigram_counter
        batch_trigram_counter += doc_trigram_counter
        batch_quadgram_counter += doc_quadgram_counter
    
    # write to disk the ngram sets of this batch of docs
    batch_buffer_save(batch_buffer)
    
    return batch_unigram_counter, batch_bigram_counter, batch_trigram_counter, batch_quadgram_counter


def parallel_process_documents(corpus, num_workers, batch_size, numDocs):
    # ngrams and their frequencies for all corpus docs
    total_unigram_counter = Counter()
    total_bigram_counter = Counter()
    total_trigram_counter = Counter()
    total_quadgram_counter = Counter()
    
    # numDocs = corpus.numDocs
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        tasks = {}
        corpus_iter = iter(corpus)

        # Submit initial tasks
        num_tasks = min(num_workers, numDocs // batch_size + 1)
        for i in range(num_tasks):
            # start_docid = main_start_docid + i*batch_size
            # end_docid = min(start_docid + batch_size, numDocs)
            docs = list(itertools.islice(corpus_iter, batch_size))
            start_docid = docs[0][0]
            end_docid = docs[-1][0]
            future = executor.submit(process_documents, docs)
            tasks[future] = (start_docid, end_docid)

        with tqdm(total=numDocs) as progress_bar:
            while tasks:
                done, _ = wait(tasks, return_when=FIRST_COMPLETED)
                for future in done:
                    batch_unigram_counter, batch_bigram_counter, batch_trigram_counter, batch_quadgram_counter = future.result()

                    # Add the queries counters from this single doc to the global counter
                    total_unigram_counter += batch_unigram_counter
                    total_bigram_counter += batch_bigram_counter
                    total_trigram_counter += batch_trigram_counter
                    total_quadgram_counter += batch_quadgram_counter

                    del tasks[future]
                    progress_bar.update(batch_size)
                    
                    # Submit a new task
                    # start_docid = max_end_docid_in_tasks
                    # end_docid = min(start_docid + batch_size, numDocs)
                    docs = list(itertools.islice(corpus_iter, batch_size))
                    if len(docs) > 0:
                        start_docid = docs[0][0]
                        end_docid = docs[-1][0]
                        future = executor.submit(process_documents, docs)
                        tasks[future] = (start_docid, end_docid)

    return total_unigram_counter, total_bigram_counter, total_trigram_counter, total_quadgram_counter


def main():
    FIELDNAME = 'CONTENT'       # Lucene index field name for content of the doc
    index_path = './Wikipedia-pages/index-enwiki'   # Lucene index directory path

    # start_docid, end_docid = 0, 999999
    # start_docid, end_docid = 1000000, 1999999
    # start_docid, end_docid = 2000000, 2999999
    # start_docid, end_docid = 3000000, 3999999
    # start_docid, end_docid = 4000000, 4999999
    # start_docid, end_docid = 5000000, 5999999
    start_docid, end_docid = 6000000, 6584625

    # enwiki doc generator object
    enwiki_corpus = MyCorpus(index_path, FIELDNAME, start_docid, end_docid)
    numDocs = end_docid - start_docid + 1
    # one doc less in the range having one bad_doc
    if start_docid <= enwiki_corpus.bad_docid <= end_docid:
        numDocs -= 1
    
    # specify batch_size for no. of docs to each worker thread
    # note: keep batch_size in steps of size of one json file to avoid simultaneous writing to the same file
    batch_size = 1000

    num_workers = multiprocessing.cpu_count()-1     # You can adjust this based on your machine's capabilities
    unigram_counter, bigram_counter, trigram_counter, quadgram_counter = parallel_process_documents(enwiki_corpus, num_workers, batch_size=batch_size, numDocs=numDocs)
    
    # dump the total counters for each ngrams for further processing and filtering
    output_dir = './counters-dumps'
    directory_name = f'{start_docid}-{end_docid}'
    directory_path = os.path.join(output_dir, directory_name)
    os.makedirs(directory_path, exist_ok=True)
    with open(os.path.join(directory_path, 'unigram_counter.pickle'), 'wb') as f:
        pickle.dump(unigram_counter, f)
    with open(os.path.join(directory_path, 'bigram_counter.pickle'), 'wb') as f:
        pickle.dump(bigram_counter, f)
    with open(os.path.join(directory_path, 'trigram_counter.pickle'), 'wb') as f:
        pickle.dump(trigram_counter, f)
    with open(os.path.join(directory_path, 'quadgram_counter.pickle'), 'wb') as f:
        pickle.dump(quadgram_counter, f)


if __name__=='__main__':
    main()