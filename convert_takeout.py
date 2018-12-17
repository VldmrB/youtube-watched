from bs4 import BeautifulSoup as BSoup
import os
import re
from utils import get_video_id


"""
Only get_all_records should be used directly. Other functions exist separately 
for easier code management and potential future changes.

-----

In addition to seemingly only returning an oddly even number of records 
(20300 the first time, 18300 the second), Takeout also seems to only return 
about 4 years worth of videos. In addition to that, when compared to the list 
you'd get from scraping your YouTube History web page directly (which seems to 
go back to the very start of your account), it's missing a number of videos for 
every year, even the current. The current one is only missing 15, but the 
number increases the further back you go in years, ending up in hundreds.

If a video has its url as the title, that usually means the account which had 
the video was terminated, though sometimes the video is still up. That's 9 out 
of 1350, however.

Turns out the reason for BS not seeing all/any divs was an extra,
out-of-place tag - <div class="mdl-grid"> (right after <body>) instead of 
indentation and newlines. Thought BeautifulSoup was supposed to handle that 
just fine, but maybe not or not in all cases. 

Update:
Perhaps the above tag is not out of place, but is paired with a seemingly 
out-of-place </div> at the every end of the file. Its presence still doesn't 
make sense as divs with the same class also wrap every video entry individually.
"""

watch_url = re.compile(r'watch\?v=')


def get_watch_history_files(takeout_path: str = None):
    os.chdir(takeout_path)
    dir_contents = os.listdir(takeout_path)
    dir_list = ('Takeout', 'YouTube', 'history', 'watch-history.html')
    watch_histories = []
    for path in dir_contents:
        if path in ['Takeout', 'YouTube']:
            if path == 'Takeout':
                full_path = os.path.join(*dir_list)
            else:
                full_path = os.path.join(*dir_list[1:])
            if os.path.exists(full_path):
                watch_histories.append(full_path)
                return watch_histories  # assumes only one folder
        if path.startswith('watch-history'):
            watch_histories.append(path)

    if watch_histories:
        return watch_histories  # assumes only one watch-history.html file is
    # present or there's multiple ones (with nums appended to their ends) in
    # the same directory

    for path in dir_contents:
        if path.startswith('takeout-2') and path[-5:-3] == 'Z-':
            full_path = os.path.join(path, 'Takeout', 'YouTube',
                                     'history', 'watch-history.html')
            if os.path.exists(full_path):
                watch_histories.append(full_path)

    if not watch_histories:
        raise SystemExit(
            'No watch-history.html files found.\n' +
            '-'*79 + '\n' +
            'This only locates watch-history.html files if any of the '
            'following is in the provided directory:\n\n - watch-history '
            'file(s) (numbers may be appended at the end)\n'
            ' - Takeout directory, extracted from the archive downloaded from '
            'Google Takeout\n'
            ' - Directory of the download archive, extracted with the same '
            'name as the archive e.g. "takeout-20181120T163352Z-001"\n\n'
            'The search will become confined to one of these types after the '
            'first match, i.e. if a watch-history file is found, it\'ll '
            'continue looking for those within the same directory, '
            'but not in Takeout directories.\n\n'
            'If you have or plan to have multiple watch-history files, the '
            'best thing to do is manually move them into one directory while '
            'adding a number to the end of each name, e.g. '
            'watch-history_001.html, from oldest to newest.\n' + '-'*79)
    return watch_histories


def _from_divs_to_dict(path: str, write_changes=False, occ_dict: dict = None):
    """
    Retrieves timestamps and video ids (if present); returns them in a dict
    If multiple watch-history.html files are present, get_all_records should be
    used instead.
    """
    with open(path, 'r') as file:
        content = file.read()
    done_ = '<span id="Done">'
    if not content.startswith(done_):  # cleans out all the junk for faster
        # BSoup processing, in addition to fixing an out-of-place-tag which
        # stops BSoup from working completely

        content = content[content.find('<body>')+6:content.find('</body>')-6]
        fluff = [  # the order should not be changed
            ('<div class="mdl-grid">', ''),
            ('<div class="outer-cell mdl-cell mdl-cell--12-col '
             'mdl-shadow--2dp">', ''),
            ('<div class="header-cell mdl-cell mdl-cell--12-col">'
             '<p class="mdl-typography--title">YouTube<br></p></div>', ''),
            ('"content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"',
             '"awesome_class"'),
            (('<div class="content-cell mdl-cell mdl-cell--6-col '
              'mdl-typography--body-1 mdl-typography--text-right"></div><div '
              'class="content-cell mdl-cell mdl' '-cell--12-col mdl-typography'
              '--caption"><b>Products:</b><br>&emsp;YouTube'
              '<br></div></div></div>'), ''),
            ('<br>', ''),
            ('<', '\n<'),
            ('>', '>\n')
        ]

        for piece in fluff:
            content = content.replace(piece[0], piece[1])
        content = done_ + '\n' + content
        if write_changes:
            print('Rewrote', path, '(trimmed junk HTML).')
            with open(path, 'w') as new_file:
                new_file.write(content)

    last_record_found = occ_dict['last_record_found']
    last_record_found_now = None
    if last_record_found:
        content = content[:content.find(last_record_found)]
        content = content[:content.rfind('<div')]  # since the previous
        # find includes half a div from the previous watch-history.html
    soup = BSoup(content, 'lxml')
    if occ_dict is None:
        occ_dict = {}
    occ_dict.setdefault('videos', {})
    occ_dict.setdefault('total_count', 0)
    divs = soup.find_all(
        'div',
        class_='awesome_class')
    if len(divs) == 0:
        return
    removed_string = 'Watched a video that has been removed'
    removed_string_len = len(removed_string)
    story_string = 'Watched story'
    for str_ in divs[0].stripped_strings:
        last_record_found_now = str_.strip()
        if last_record_found_now.startswith(removed_string):
            last_record_found_now = last_record_found_now[removed_string_len:]
    occ_dict['last_record_found'] = last_record_found_now
    for div in divs:
        all_text = div.get_text().strip()
        if all_text.startswith(removed_string):
            video_id = 'removed'
            watched_on = all_text[removed_string_len:]
        elif all_text.startswith(story_string):
            video_id = 'story'
            watched_on = all_text.splitlines()[-1].strip()
            if '/watch?v=' in watched_on:
                watched_on = watched_on[57:]

        else:
            url = div.find(href=watch_url)
            video_id = get_video_id(url['href'])
            watched_on = all_text.splitlines()[-1].strip()

        occ_dict['videos'].setdefault(video_id, [])
        occ_dict['videos'][video_id].append(watched_on)
        occ_dict['total_count'] += 1

    return occ_dict


def get_all_records(takeout_path: str = None, write_changes=False,
                    dump_json=False, silent=False):
    """Should be used instead of other functions in practically any case;
    The others are mostly kept separately in case of future changes"""
    if not takeout_path:
        takeout_path = '.'
    os.chdir(takeout_path)
    watch_files = get_watch_history_files(takeout_path)
    occ_dict = {}
    occ_dict.setdefault('last_record_found', None)
    for file in watch_files:
        _from_divs_to_dict(file, write_changes=write_changes,
                           occ_dict=occ_dict)

    if not silent:
        print('Total videos:', occ_dict['total_count'])
        print('Unique videos with ids:', len(occ_dict['videos']) - 2)
        # ^ minus two for removed and story keys
        print('A video won\'t have an ID if it\'s been taken down, or if it '
              'was watched as a "story", in which case it will list one or '
              'more YouTube channels instead.')
    if dump_json:
        import json
        with open('watch-history.json', 'w') as file:
            json.dump(occ_dict['videos'], file, indent=4)

    return occ_dict['videos']
