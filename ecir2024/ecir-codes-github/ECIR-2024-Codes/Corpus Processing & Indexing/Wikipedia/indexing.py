# imports
import os
import argparse
import re
from tqdm import tqdm

# lucene imports
import lucene
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory
import org.apache.lucene.document as document
from java.io import File
from org.apache.lucene.document import FieldType
from org.apache.lucene.index import IndexOptions

# lucene init Virtual Machine
lucene.initVM()

# ------------------------------------------------------------------

def filesPaths(input_dir):
    filePaths = []
    dirs = os.listdir(input_dir)
    for d in dirs:
        files = os.listdir(os.path.join(input_dir,d))
        for f in files:
            filePaths.append(os.path.join(input_dir, d, f))
    return filePaths


def yieldOneDoc(filePath):
    """ Generates a doc from the file given unless exhausted """
    
    with open(filePath, 'r', encoding='utf-8') as f:
        for line in f:
            # look for the header line
            if line.startswith('<doc id="'):
                # extract the id, url, title from the header line
                match = re.match('<doc id="(.*)" url="(.*)" title="(.*)">', line)
                page_id = int(match.group(1))
                title = str(match.group(3))
                
                # create an empty string to store the text for this doc
                text = ''
            # look for the closing </doc> line
            elif line.startswith('</doc>'):
                yield page_id, title, text.rstrip()
            # if this is not a header or closing line, it must be part of the text
            else:
                text += line


def createLuceneIndex(filePaths, index_dir):
    # define index directory and lucene index writer
    indexPath = File(index_dir).toPath()
    indexDir = FSDirectory.open(indexPath)
    analyzer = EnglishAnalyzer()        # Analyzer --> English Analyzer
    writerConfig = IndexWriterConfig(analyzer)
    writer = IndexWriter(indexDir, writerConfig)

    
    # setting field type setting for lucene for what infos to store
    fieldType = FieldType()
    fieldType.setStored(True)
    fieldType.setTokenized(True)
    fieldType.setStoreTermVectors(True)
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
    
    # iterate over all the docs and add them into index
    for filePath in tqdm(filePaths):
        for page_id, title, content in yieldOneDoc(filePath):
            doc = document.Document()
            doc.add(document.Field('PAGE-ID', page_id, fieldType))
            doc.add(document.Field('TITLE', title, fieldType))
            doc.add(document.Field('CONTENT', content, fieldType))
            writer.addDocument(doc)
    writer.commit()
    writer.close()
    print('\nIndexing completed successfully!')


def main(corpus_dir, index_dir):
    # get file paths of all the wiki text files in numerous subdirs of the main corpus directory
    filePaths = filesPaths(corpus_dir)
    
    # create lucene-8.8.1 index
    createLuceneIndex(filePaths, index_dir)


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--corpus_dir", type=str, required=True, help="Corpus directory path")
    parser.add_argument("-o", "--index_dir", type=str, required=True, help="Output Lucene index directory path")
    args = parser.parse_args()
    
    main(args.corpus_dir, args.index_dir)
