from bs4 import BeautifulSoup as BSoup
import os
import re
import json
from ktools.fn import extract_num_from_filename
from config import WORK_DIR

"""
Turns out the reason for BS not seeing all/any divs was an extra,
out-of-place tag - <div class="mdl-grid"> (right after <body>) instead of 
indentation and newlines. Thought BeautifulSoup was supposed to handle that 
just fine, but maybe not or not in all cases. 
"""

WORK_DIR = os.path.join(WORK_DIR, 'takeout_data')
os.chdir(WORK_DIR)
watch_url = re.compile(r'watch\?v=')


def from_divs_to_dict(path, silent=False):
    """Retrieves video ids and timestamps (when they were watched) and
    returns them in a dict"""
    with open(path, 'r', encoding='utf-8') as json_watched:
        soup = BSoup(json_watched, 'lxml')

    occ_dict = {}
    divs = soup.find_all(
        'div',
        class_='content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1')

    last_watched_now = None
    last_watched = None
    for str_ in divs[0].stripped_strings:
        last_watched_now = str_.strip()
    cur_file_num = extract_num_from_filename(path, True)

    with open('first_videos_in_list.json', 'r') as json_watched:
        json_watched = json_watched.read()
        if json_watched != '':
            last_watched_dict = json.loads(json_watched)
            if str(cur_file_num-1) in last_watched_dict:
                last_watched = last_watched_dict[str(cur_file_num-1)]
        else:
            last_watched_dict = {}
    with open('first_videos_in_list.json', 'w') as new_json_watched:
        last_watched_dict[str(cur_file_num)] = last_watched_now
        json.dump(last_watched_dict, new_json_watched, indent=4)
    for div in divs:

        url = div.find(href=watch_url)
        if url:
            video_id = url['href'][url['href'].find('=') + 1:]
            id_end = video_id.find('&t=')
            if id_end > 0:
                video_id = video_id[:id_end]
            occ_dict.setdefault(video_id, [])
            watched_on = ''
            for str_ in div.stripped_strings:
                watched_on = str_.strip()
            if watched_on == last_watched:
                print('!')
                break

            occ_dict[video_id].append(watched_on)

    from pprint import pprint
    pprint(sorted([[k, v] for k, v in occ_dict.items()],
                  key=lambda entry: len(entry[1]))[-1])
    if not silent:
        print('Total videos watched:', sum(
            [len(vals) for vals in occ_dict.values()]))
        print('Unique, new videos watched:', len(occ_dict))

    del divs
    return occ_dict


def clean_up_tags(path: str, new_path: str):
    """Removes the erroneous tag which stops BS from parsing properly as
    well as removing some of the unnecessary data and tags"""
    with open(path, 'r') as file, open(new_path, 'w') as new_file:
        start = '<div class="mdl-grid">'
        end = '</body>'
        start_done = False
        ind = 0
        new_str = ''
        while True:  # skips everything up to and including, start
            piece = file.read(1)
            if not piece:
                break
            if piece == start[ind]:
                if not start_done:
                    new_str += start[ind]
                    ind += 1
                    if new_str == start:
                        new_str = ''
                        ind = 0
                        break
            else:
                new_str = ''
                ind = 0

        while True:  # writes up to, but not including, end
            piece = file.read(1)
            if not piece:
                break
            if piece == end[ind]:
                new_str += end[ind]
                if new_str == end:
                    break
                ind += 1
            else:
                if new_str:
                    new_file.write(new_str)
                    new_str = ''
                    ind = 0
                new_file.write(piece)
    os.remove(path)
    os.rename(new_path, path)

# todo convert this to work on multiple files? Populate into DB
# adapt functions using this
# remove watched_on column from videos table, replace with watched
# (number of # times)
# create a table that lists all times watched for each video (one-to-many)


def add_newlines(path: str):
    with open(path, 'rb') as file, open(
            path[:path.find('.')] + '_newlined.html', 'wb') as new_file:
        while True:
            read_piece = file.read(1)
            if not read_piece:
                break
            # if read_piece == b'<':
            #     new_file.write(b'\n')
            #     new_file.write(read_piece)
            new_file.write(read_piece)
            if read_piece == b'>':
                new_file.write(b'\n')


if __name__ == '__main__':
    # from_divs_to_dict(os.path.join(WORK_DIR, 'watch-history_001.html'))
    # from_divs_to_dict(os.path.join(WORK_DIR, 'watch-history_002.html'))
    pass
    add_newlines(os.path.join(WORK_DIR, 'watch-history_002.html'))
