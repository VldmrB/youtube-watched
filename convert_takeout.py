from bs4 import BeautifulSoup as BSoup
import os
import re
import json
from ktools.fn import extract_num_from_filename
from config import WORK_DIR
from ktools.utils import timer


"""
Turns out the reason for BS not seeing all/any divs was an extra,
out-of-place tag - <div class="mdl-grid"> (right after <body>) instead of 
indentation and newlines. Thought BeautifulSoup was supposed to handle that 
just fine, but maybe not or not in all cases. 
"""
join = os.path.join
WORK_DIR = os.path.join(WORK_DIR, 'takeout_data')
os.chdir(WORK_DIR)
watch_url = re.compile(r'watch\?v=')


def get_video_id(url):
    video_id = url['href'][url['href'].find('=') + 1:]
    id_end = video_id.find('&t=')
    if id_end > 0:
        video_id = video_id[:id_end]

    return video_id


@timer
def from_divs_to_dict(path, silent=False):
    """Retrieves video ids and timestamps (when they were watched) and
    returns them in a dict"""
    with open(path, 'r', encoding='utf-8') as json_watched:
        soup = BSoup(json_watched, 'lxml')

    occ_dict = {}
    divs = soup.find_all(
        'div',
        class_='awesome_class')
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
            video_id = get_video_id(url)
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
        print('Total videos watched:', len(divs))
        print('Total videos watched:', sum(
            [len(vals) for vals in occ_dict.values()]))
        print('Unique, new videos watched:', len(occ_dict))

    del divs
    return occ_dict


@timer
def keep_the_divs(path: str):
    with open(path, 'r') as file:
        file = file.read()
    done_ = '<span id="Done">'
    if file.startswith(done_):
        return
    file = file[file.find('<body>')+6:file.find('</body>')-6]
    fluff = [  # the order should not be changed
        ('<div class="mdl-grid">', ''),
        ('<div class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp">',
         ''),
        ('<div class="header-cell mdl-cell mdl-cell--12-col">'
         '<p class="mdl-typograp'
         'hy--title">YouTube<br></p></div>', ''),
        ('"content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"',
         '"awesome_class"'),
        (('<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--bo'
          'dy-1' ' mdl-typography--text-right"></div><div class="content-cell m'
          'dl-cell mdl' '-cell--12-col mdl-typography--caption"><b>Products:</b'
          '><br>&emsp;YouTube'
          '<br></div></div></div>'), ''),
        ('<br>', ''),
        ('<', '\n<'),
        ('>', '>\n')
    ]

    for piece in fluff:
        file = file.replace(piece[0], piece[1])
    file = done_ + '\n' + file
    with open(path, 'w') as new_file:
        new_file.write(file)


# todo convert this to work on multiple files? Populate into DB
# adapt functions using this
# remove watched_on column from videos table, replace with watched
# (number of # times)
# create a table that lists all times watched for each video (one-to-many)

if __name__ == '__main__':
    keep_the_divs('watch-history_001.html')
    from_divs_to_dict(os.path.join(WORK_DIR, 'watch-history_001.html'))
