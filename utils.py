def is_video_url(candidate_str: str):
    return True if 'youtube.com/watch?' in candidate_str else False


def get_video_id(url):
    video_id = url[url.find('=') + 1:]
    id_end = video_id.find('&t=')
    if id_end > 0:
        video_id = video_id[:id_end]

    return video_id


def convert_duration(duration_iso8601: str):
    duration = duration_iso8601.split('T')
    duration = {'P': duration[0][1:], 'T': duration[1]}
    int_value = 0
    for key, value in duration.items():
        new_value = ''
        if not value:
            continue
        for element in value:
            if element.isnumeric():
                new_value += element
            else:
                new_value += element + ' '
        split_vals = new_value.strip().split(' ')
        for val in split_vals:
            if val[-1] == 'Y':
                int_value += int(val[:-1]) * 31_536_000
            elif val[-1] == 'M':
                if key == 'P':
                    int_value += int(val[:-1]) * 2_592_000
                else:
                    int_value += int(val[:-1]) * 60
            elif val[-1] == 'W':
                int_value += int(val[:-1]) * 604800
            elif val[-1] == 'D':
                int_value += int(val[:-1]) * 86400
            elif val[-1] == 'H':
                int_value += int(val[:-1]) * 3600
            elif val[-1] == 'S':
                int_value += int(val[:-1])

    return int_value
