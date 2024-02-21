import argparse
import os
from tqdm import tqdm
import re
import pickle

def readCirrusIndex(pickle_path):
    with open(pickle_path, 'rb') as f:
        return pickle.load(f)

def filesPaths(input_dir):
    # Cirrus dump Output-clean's files paths
    filePaths = []
    dirs = os.listdir(input_dir)
    for d in dirs:
        files = os.listdir(os.path.join(input_dir,d))
        for f in files:
            filePaths.append(os.path.join(input_dir, d, f))
    return filePaths


def main(input_dir):
    index = {}
    for input_file in tqdm(filesPaths(input_dir), desc='Main loop', position=0):
        with open(input_file, 'rb') as file:
            pos = 0
            for line in file:
                # look for the header line
                if line.startswith(b'<doc id="'):
                    # extract the id, url, title, language, and revision from the header line
                    match = re.match(b'<doc id="(.*)" url="(.*)" title="(.*)">', line)
                    id_ = match.group(1)
                    # url = match.group(2)
                    # title = match.group(3)

                    # store binary position of the start of doc with id_ in the index
                    index[int(id_.decode())] = (input_file, pos)

                pos += len(line)
    
    with open('index-cirrus-clean.pickle', 'wb') as f:
        pickle.dump(index, f, protocol=pickle.HIGHEST_PROTOCOL)


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_dir", type=str, required=True, help="Cirrus extracted output directory path")
    # parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()
    main(args.input_dir)