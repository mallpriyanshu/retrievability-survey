import argparse
import os
from tqdm import tqdm
import re, regex

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


def clean_displaystyle(text):
    cleaned_text = ''
    start_index = 0

    while True:
        match = regex.search(r'\{\\displaystyle', text[start_index:])
        if match:
            cleaned_text += text[start_index:start_index + match.start()]
            start_index += match.end()

            open_brackets = 1
            for idx, char in enumerate(text[start_index:]):
                if char == '{':
                    open_brackets += 1
                elif char == '}':
                    open_brackets -= 1

                if open_brackets == 0:
                    start_index += idx + 1
                    break
        else:
            cleaned_text += text[start_index:]
            break

    return cleaned_text


def clean_text(text, title):
    # remove {\displaystyle ...}
    text = clean_displaystyle(text)
    
    # remove [...]
    text = re.sub(r"\[\d+?\]", "", text)
    # remove {{...}}
    text = re.sub(r"\{\{+[^{}]+?\}\}+", "", text)
    # remove navigation from title
    text = re.sub(r"^.+? \> " + re.escape(title), "", text)
    
    return text

def main(args):
    for input_file, output_file in tqdm(filesPaths(args.input_dir,args.output_dir), desc='Main loop', position=0):
        with open(input_file, 'rt') as input_file, \
            open(output_file, 'w', encoding='utf-8') as output_file:
            # for line in tqdm(input_file, desc='file', position=1, leave=False):
            for line in input_file:
                # look for the header line
                if line.startswith('<doc id="'):
                    # extract the id, url, title, language, and revision from the header line
                    match = re.match('<doc id="(.*)" url="(.*)" title="(.*)" language="(.*)" revision="(.*)">', line)
                    id_ = match.group(1)
                    url = match.group(2)
                    title = match.group(3)
                    
                    # create an empty string to store the text for this doc
                    text = ''
                
                # look for the closing </doc> line
                elif line.startswith('</doc>'):
                    # clean the text
                    text = clean_text(text, title)
                    # write the article to output_file in the same format as input_file
                    header = '<doc id="%s" url="%s" title="%s">\n' % (id_, url, title)
                    page = header + text + '\n</doc>\n'
                    output_file.write(page.encode('utf-8').decode())
                # if this is not a header or closing line, it must be part of the text
                else:
                    text += line


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()
    main(args)