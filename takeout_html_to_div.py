from bs4 import BeautifulSoup as BSoup
import os
import re
import json
from ktools.env import *

"""
Had to first add some newlines and indentation for BeautifulSoup to be 
able to see all 20300 records. It only saw 28 prior to that, when all the 
data divs were on one line

UPDATE:
Turns out the reason for the above was an out-of-place tag instead of 
indentation and newlines.. Thought BeautifulSoup was supposed to handle that 
just fine, but maybe not or not in all cases. 
However, adding newlines is still useful as they allow for easy splitting of
data within each div when creating the JSON js_file.
"""
# todo make sure everything works correctly after changing dirs
dir_path = G_PYTON_PATH + r'\youtube_watched_data'
os.chdir(dir_path)

original_html = dir_path + r'\watch-history.html'
newline_html = dir_path + r'\watch-history-newlined.html'
indented_html = dir_path + r'\watch-history-indented.html'
jsonified = dir_path + r'\divs.json'
pat = re.compile(r'</[a-zA-Z0-9]+?>')


def by_char(path=original_html, write_to_path=newline_html):
    with open(path, 'rb') as file, open(write_to_path, 'wb') as new_file:
        """Adds newlines between all tags"""
        while True:
            chunk = file.read(1)
            if not chunk:
                break
            else:
                if chunk == b'>':
                    chunk += b'\n'

                new_file.write(chunk)


def by_line(path=newline_html, write_to_path=indented_html):
    """Indents where necessary to make the js_file more readable, only works on a
    product of the by_char function"""
    count = 0
    with open(path, 'r') as file, open(write_to_path, 'w') as new_file:
        while True:
            additional = 0
            line = file.readline()
            if line:
                if pat.search(line):
                    count -= 1
                elif '<br>' not in line and len(line) > 2 and '<' in line:
                    count += 1
                else:
                    additional = 1
            else:
                break
            new_file.write(line.strip() + '\n' + '    ' * (count + additional))


def from_divs_to_json(path=indented_html, write_to_path=jsonified):
    """Retrieves all the divs with data and dumps said date as JSON"""
    with open(path, 'rb') as file:
        soup = BSoup(file, 'lxml')

    divs = soup.find_all(
        'div',
        class_='content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1')
    print(len(divs))
    div_list = []
    for div in divs:
        appendee = [obj.strip() for obj in div.get_text().split('\r\n')]  # [2:]

        print(appendee)
        div_list.append(appendee)
    del divs
    print(len(div_list))

    with open(write_to_path, 'w') as file:
        json.dump({'divs': div_list}, file, indent=4)
