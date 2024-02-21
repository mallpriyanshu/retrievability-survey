'''
    - Remove text after References, See alos, Notes etc like sections at the end of the articles, from the cirrus-text.
    - Add headers of the articles from extractor-text into the end of the cirrus-text - to preserve header texts as
    bag-of-words equivalence.
    - Do it for whole Wiki dump.
'''

import argparse
import os, sys
from tqdm import tqdm
import re
import pickle
import difflib
import multiprocessing


def readCirrusIndex(pickle_path):
    with open(pickle_path, 'rb') as f:
        return pickle.load(f)

def filesPaths(input_dir, output_dir):
    filePaths = []
    dirs = os.listdir(input_dir)
    for d in dirs:
        files = os.listdir(os.path.join(input_dir,d))
        if files is not None:
            os.makedirs(os.path.join(output_dir, d), exist_ok=True)
        for f in files:
            filePaths.append((os.path.join(input_dir, d, f), os.path.join(output_dir, d, f)))
    return filePaths


def getCirrusWikiText(id_, index):
    file_path, pos = index[id_]
    file_path = './Wikipedia-Cirrus/' + file_path
    with open(file_path, 'r') as f:
        f.seek(pos)
        for line in f:
            # look for the header line
            if line.startswith('<doc id="'):
                # extract the id, url, title, language, and revision from the header line
                match = re.match('<doc id="(.*)" url="(.*)" title="(.*)">', line)
                id_cirrus = int(match.group(1))
                
                # Exception handling: id mismatch --> fatal error; shouldn't go silently
                if int(id_cirrus) != id_:
                    print("The two ids didn't match while retrieving cirrus text")
                    print(f'extractor_id = {id_}, cirrus_id = {id_cirrus}, cirrus_filePath = {file_path}\n')
                    raise 
                
                # create an empty string to store the text for this doc
                text = ''
            
            # look for the closing </doc> line
            elif line.startswith('</doc>'):
                return text.rstrip()
            # if this is not a header or closing line, it must be part of the text
            else:
                text += line


def remove_and_insert(text_with_noise, clean_text):
    # Extract headings and their positions in the noisy text
    matches = re.finditer('\n#+\s', text_with_noise)
    headings = []
    for match in matches:
        start_index = match.start()
        end_index = match.end()
        heading_end_index = text_with_noise.find('.\n', end_index)
        if heading_end_index == -1:
            heading_end_index = len(text_with_noise)
        result_text = text_with_noise[end_index:heading_end_index]
        headings.append((start_index, end_index, result_text))
    
    # heading names of which content has to be excluded
    last_headings = ['References', 'Explanatory notes', 'Citations', 'General and cited sources', 'Primary sources' \
        'Secondary sources', 'Tertiary sources', 'See also', 'Notes', 'Footnotes', 'Subnotes', 'Other notes' \
            'Bibliography', 'Sources', 'Further reading']
    
    headers_list_to_add = []
    if_present = 0
    for start,end,heading in headings:
        if heading in last_headings:
            prev = 500
            if start < prev:
                prev = start
            remove_after_text = text_with_noise[start-prev:start]
            if_present = 1
            break
        headers_list_to_add.append(heading)
    
    # remove [citation needed] in clean_text
    clean_text = clean_text.replace('[citation needed]', '')
    
    result = clean_text
    
    if if_present == 1:
        # remove formula_## numbers from the remove_after_text
        remove_after_text = re.sub(r'formula_\d+', '', remove_after_text)
        
        if remove_after_text != '':
            # Use difflib to find the longest common subsequence between the two texts
            matcher = difflib.SequenceMatcher(None, remove_after_text, clean_text, autojunk=False)
            matching_blocks = matcher.get_matching_blocks()

            # remove unnecessary contents of last_headings
            match = matching_blocks[-2]
            if clean_text[match[1]+match[2]-1] in '.:?':
                result = clean_text[:match[1]+match[2]]
            else:
                dot_index = clean_text.find('.', match[1]+match[2])
                result = clean_text[:dot_index+1]
        else:
            print()
            print('`remove_after_text` blank')
            print(f'Heading: {heading}')
            title = clean_text.split('\n')[0]
            print(f"Article title: {title}")
    
    # Append headings at the end of the clean_text (i.e. result variable now)
    joined_headings = ' '.join([h+'.' for h in headers_list_to_add])
    result += '\n\n' + joined_headings + '\n\n'
    
    return result


def new_text(extractor_text, cirrus_text):
    cirrus_text = remove_and_insert(extractor_text, cirrus_text)
    return cirrus_text


def process_file(file_paths):
    input_file_path, output_file_path = file_paths
    with open(input_file_path, 'rt') as input_file, \
        open(output_file_path, 'w', encoding='utf-8') as output_file:
        text = ''
        id_ = 0
        for line in input_file:
            # look for the header line
            if line.startswith('<doc id="'):
                # extract the id, url, title, language, and revision from the header line
                match = re.match('<doc id="(.*)" url="(.*)" title="(.*)">', line)
                id_ = int(match.group(1))
                url = match.group(2)
                title = match.group(3)
                
                # create an empty string to store the text for this doc
                text = ''
                
            # look for the closing </doc> line
            elif line.startswith('</doc>'):
                if id_ in cirrus_ids:
                    # get Cirrus cleaned text for this id_
                    cirrus_text = getCirrusWikiText(id_, index)
                    # add headers and remove references etc from the cirrus text and returns it in place of given text
                    text = new_text(text, cirrus_text)
                    # write the article to output_file in the same format as input_file
                    header = '<doc id="%s" url="%s" title="%s">\n' % (id_, url, title)
                    page = header + text + '\n</doc>\n'
                    output_file.write(page.encode('utf-8').decode())
            # if this is not a header or closing line, it must be part of the text
            else:
                text += line


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_dir", type=str, required=True, help="path of extractor output dir")
    parser.add_argument("-o", "--output_dir", type=str, required=True, help='path of the dir to create with the outputs')
    args = parser.parse_args()
    
    index = readCirrusIndex('./Wikipedia-Cirrus/index-cirrus-clean.pickle')
    cirrus_ids = index.keys()
    
    files = filesPaths(args.input_dir,args.output_dir)
    
    # Create a multiprocessing pool with the maximum number of processes
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count()-1)
    
    # Use tqdm to show a progress bar for the whole program
    with tqdm(total=len(files), desc='Main loop', position=0) as pbar:
        # Iterate over the files and process them in parallel
        for _ in pool.imap_unordered(process_file, files):
            pbar.update()