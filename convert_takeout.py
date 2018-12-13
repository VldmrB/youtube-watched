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

Update:
Perhaps the above tag is not out of place, but is paired with a seemingly 
out-of-place </div> at the every end of the file. Its presence still doesn't 
make sense as divs with the same class also wrap every video entry individually.
"""

os.chdir(os.path.join(WORK_DIR, 'takeout_data'))
watch_url = re.compile(r'watch\?v=')


def get_video_id(url):
    video_id = url[url.find('=') + 1:]
    id_end = video_id.find('&t=')
    if id_end > 0:
        video_id = video_id[:id_end]

    return video_id


@timer
def from_divs_to_dict(path, silent=False, occ_dict=None):
    """Retrieves video ids and timestamps (when they were watched) and
    returns them in a dict"""

    last_watched_timestamp_now = None
    last_watched_timestamp = None
    cur_file_num = extract_num_from_filename(path, True)

    with open('first_videos_in_list.json', 'r') as json_watched:
        json_watched = json_watched.read()
        if json_watched != '':
            last_watched_dict = json.loads(json_watched)
            if str(cur_file_num-1) in last_watched_dict:
                last_watched_timestamp = last_watched_dict[str(cur_file_num-1)]
        else:
            last_watched_dict = {}
    content = clean_and_trim_html(path)
    if last_watched_timestamp:
        content = content[:content.find(last_watched_timestamp)]
        content = content[:content.rfind('<div')]  # since the previous
        # find includes half a div from the previous watch-history.html
    soup = BSoup(content, 'lxml')
    count = 0
    if occ_dict is None:
        occ_dict = {}
    divs = soup.find_all(
        'div',
        class_='awesome_class')
    for str_ in divs[0].stripped_strings:
        last_watched_timestamp_now = str_.strip()
    with open('first_videos_in_list.json', 'w') as new_json_watched:
        last_watched_dict[str(cur_file_num)] = last_watched_timestamp_now
        json.dump(last_watched_dict, new_json_watched, indent=4)
    for div in divs:
        url = div.find(href=watch_url)
        if url:
            video_id = get_video_id(url['href'])
            occ_dict.setdefault(video_id, [])
            watched_on = ''  # simply so PyCharm doesn't freak out,
            # as if a video has a url, it'll also have a timestamp
            for str_ in div.stripped_strings:
                watched_on = str_

            occ_dict[video_id].append(watched_on)
        count += 1

    if not silent:
        print('Total videos watched:', count)
        print('Videos watched, with urls:', sum(
            [len(vals) for vals in occ_dict.values()]))
        print('Unique videos watched:', len(occ_dict))

    return occ_dict


def clean_and_trim_html(path: str):
    with open(path, 'r') as file:
        file = file.read()
    done_ = '<span id="Done">'
    if file.startswith(done_):
        return file
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

    return file


def get_all_vids():
    watch_files = [file for file in os.listdir('.')
                   if file.startswith('watch-history_')]
    o_dict = {}
    for file in watch_files:
        from_divs_to_dict(file, occ_dict=o_dict)


# todo Populate into DB?
# adapt functions using this
# remove watched_on column from videos table, replace with watched
# (number of # times)
# create a table that lists all times watched for each video (one-to-many)

if __name__ == '__main__':
    pass
    get_all_vids()
