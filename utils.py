from typing import Union


def check_if_url(candidate_str: str):
    return True if 'youtube.com' in candidate_str else False


def get_final_keys(
        obj_to_check: Union[dict, list, tuple], final_keys: list = None):
    """Returns all the final, terminating keys.
    Any duplicates (they're possible since the object may have
    nested dictionaries) are eliminated via sets"""
    if final_keys is None:
        final_keys = []
    if isinstance(obj_to_check, dict):
        for i in obj_to_check:
            key_added = False
            if isinstance(obj_to_check[i], dict):
                get_final_keys(obj_to_check[i], final_keys)
            elif isinstance(obj_to_check[i], (list, tuple)):
                for sub_i in obj_to_check[i]:
                    if isinstance(sub_i, (dict, list, tuple)):
                        get_final_keys(sub_i, final_keys)
                    else:
                        if not key_added:  # a hacky way of adding keys with
                            # terminating list/tuple values
                            key_added = True
                            final_keys.append(i)
            else:
                final_keys.append(i)
    elif isinstance(obj_to_check, (list, tuple)):
        for i in obj_to_check:
            if isinstance(i, (dict, list, tuple)):
                get_final_keys(i, final_keys)
            else:
                final_keys.append(i)
    return sorted(list(set(final_keys)))


def get_key_value(
        obj: Union[dict, list, tuple], key: str, key_value: list = None):
    """Will find multiple values if duplicates of the key are present within
    nested dictionaries"""
    if key_value is None:
        key_value = []
    if isinstance(obj, dict):
        if key in obj.keys():
            key_value.append(obj[key])
        for key_, values in obj.items():
            if isinstance(values, (dict, list, tuple)):
                get_key_value(values, key, key_value)
    elif isinstance(obj, (list, tuple)):
        for value in range(len(obj)):
            if isinstance(obj[value], (list, dict)):
                get_key_value(obj[value], key, key_value)
    return key_value


def get_missing_keys(
        obj: Union[dict, list, tuple], keys_to_check_for: list):
    final_keys = get_final_keys(obj)
    tags_missing = [tag for tag in keys_to_check_for if tag not in final_keys]

    return tags_missing


def get_final_key_paths(
        obj: Union[dict, list, tuple], cur_path: str, paths: list = None):
    if paths is None:
        paths = []

    if isinstance(obj, (dict, list, tuple)):
        if isinstance(obj, dict):
            for key in obj:
                new_path = cur_path + f'[\'{key}\']'
                if isinstance(obj[key], dict):
                    get_final_key_paths(
                        obj[key], new_path, paths)
                elif isinstance(obj[key], (list, tuple)):
                    get_final_key_paths(obj[key], new_path, paths)
                else:
                    paths.append(new_path)
        else:
            tag_added = False  # same as in get_final_keys function
            for i in range(len(obj)):
                if isinstance(obj[i], (dict, tuple, list)):
                    get_final_key_paths(
                        obj[i], cur_path + f'[{i}]', paths)
                else:
                    if not tag_added:
                        paths.append(cur_path)
                        tag_added = True

    return paths
