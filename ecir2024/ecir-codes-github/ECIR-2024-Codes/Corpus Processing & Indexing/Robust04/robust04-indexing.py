"""
    Creates Lucene Index of corpus
    
    Used to process the TREC 678 corpus and create an inverted-index in Lucene format using PyLucene.
"""

import os
import re

# ======== PyLucene imports ========
import lucene
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory
import org.apache.lucene.document as document
from java.io import File

# init lucene VM
lucene.initVM()

# ===================================

# Corpus documents directory path
dirPath = './trec678_corpus/documents'
fileNames = os.listdir(dirPath)
filePaths = [f'{dirPath}/{f}' for f in fileNames]

indexPath = File("index/").toPath()
indexDir = FSDirectory.open(indexPath)

analyzer = EnglishAnalyzer()
writerConfig = IndexWriterConfig(analyzer)
writer = IndexWriter(indexDir, writerConfig)


tag_exp = re.compile('<.*?>')

docCount = 0

def cleanTag(rawDoc):
    cleanDoc = re.sub(tag_exp, '', rawDoc)
    return cleanDoc

def process(oneDoc):
    global docCount
    docCount += 1
    print(docCount)
    return cleanTag(oneDoc)

# this function needs to be called for each of the files in the directory
def processFile(filePath):
    with open(filePath, 'r', encoding='ISO-8859-1') as f:
        inDoc = False
        docid,oneDoc = "",""
        docids,contents = [],[]     # will store all the docs (docIDs, Contents) of a single file in a list
                                    # with docid and contents in one-to-one list index-wise correspondence
        for line in f:
            if inDoc:
                if line.startswith("<DOCNO>"):
                    m = re.search('<DOCNO>(.+?)</DOCNO>', line)
                    docid = m.group(1)
                    continue
                elif line.strip() == "</DOC>":
                    inDoc = False
                    contents.append(process(oneDoc))
                    docids.append(docid.strip())
                    oneDoc = ""
                else:
                    oneDoc += line

            elif line.strip() == "<DOC>":
                inDoc = True
        return docids,contents


from org.apache.lucene.document import FieldType
from org.apache.lucene.index import IndexOptions

ft = FieldType()
ft.setStored(True)
ft.setTokenized(True)
ft.setStoreTermVectors(True)
# ft.setStoreTermVectorOffsets(True)
# ft.setStoreTermVectorPositions(True)
ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

# Main Indexer function
def make_inverted_index(filePaths, fieldType):
    for filePath in filePaths:
        docids,contents = processFile(filePath)
        for i in range(len(docids)):
            doc = document.Document()
            doc.add(document.Field('DOCID', docids[i], fieldType))
            doc.add(document.Field('CONTENT', contents[i], fieldType))
            writer.addDocument(doc)
    writer.close()
    print('Indexing completed successfully!')

make_inverted_index(filePaths, ft)
