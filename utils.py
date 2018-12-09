from typing import Union


def check_if_url(candidate_str: str):
    return True if 'youtube.com' in candidate_str else False


def get_final_keys(
        obj: Union[dict, list, tuple], black_list: list = None,
        final_keys: list = None):
    """
    Returns all the final, terminating keys.
    Any duplicates (they're possible since the object may have
    nested dictionaries) are eliminated via sets

    :param obj:
    :param black_list: keys which will be skipped and not descended into
    :param final_keys: list of all found terminating keys, no need to pass
    anything
    :return:
    """
    if final_keys is None:
        final_keys = []
    if isinstance(obj, dict):
        for key in obj:
            if black_list is not None and key in black_list:
                continue
            key_added = False
            if isinstance(obj[key], dict):
                get_final_keys(obj[key], black_list, final_keys)
            elif isinstance(obj[key], (list, tuple)):
                for sub_item in obj[key]:
                    if isinstance(sub_item, (dict, list, tuple)):
                        get_final_keys(sub_item, black_list, final_keys)
                    else:
                        if not key_added:  # a hacky way of adding keys with
                            # terminating list/tuple values
                            key_added = True
                            final_keys.append(key)
            else:
                final_keys.append(key)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            if isinstance(item, (dict, list, tuple)):
                get_final_keys(item, black_list, final_keys)
            else:
                final_keys.append(item)
    return sorted(list(set(final_keys)))


def get_key_value(
        obj: Union[dict, list, tuple], key_to_find: str,
        black_list: list = None, key_value: list = None):
    """Will find multiple values if duplicates of the key are present within
    nested dictionaries"""
    if key_value is None:
        key_value = []
    if isinstance(obj, dict):
        if key_to_find in obj.keys():
            key_value.append(obj[key_to_find])
        for key in obj:
            if black_list is not None and key in black_list:
                continue
            if isinstance(obj[key], (dict, list, tuple)):
                get_key_value(obj[key], key_to_find, black_list, key_value)
    elif isinstance(obj, (list, tuple)):
        for value in range(len(obj)):
            if isinstance(obj[value], (list, dict)):
                get_key_value(obj[value], key_to_find, black_list, key_value)
    return key_value


def get_missing_keys(
        obj: Union[dict, list, tuple], keys_to_check_for: list):
    final_keys = get_final_keys(obj)
    tags_missing = [tag for tag in keys_to_check_for if tag not in final_keys]

    return tags_missing


def get_final_key_paths(
        obj: Union[dict, list, tuple], cur_path: str = '',
        append_values: bool = False,
        paths: list = None, black_list: list = None):
    """
    Returns Python ready, full key paths as strings

    :param obj:
    :param cur_path: name of the variable that's being passed as the obj can be
    passed here to create eval ready key paths
    :param append_values: return corresponding key values along with the keys
    :param paths: the list that will contain all the found key paths, no need
    to pass anything
    :param black_list: dictionary keys which will be ignored (not paths)
    :return:
    """
    if paths is None:
        paths = []

    if isinstance(obj, (dict, list, tuple)):
        if isinstance(obj, dict):
            for key in obj:
                new_path = cur_path + f'[\'{key}\']'
                if isinstance(obj[key], dict):
                    if black_list is not None and key in black_list:
                        continue
                    get_final_key_paths(
                        obj[key], new_path, append_values, paths, black_list)
                elif isinstance(obj[key], (list, tuple)):
                    get_final_key_paths(obj[key], new_path,
                                        append_values, paths, black_list)
                else:
                    if append_values:
                        to_append = [new_path, obj[key]]
                    else:
                        to_append = new_path
                    paths.append(to_append)
        else:
            key_added = False  # same as in get_final_keys function
            for i in range(len(obj)):
                if isinstance(obj[i], (dict, tuple, list)):
                    get_final_key_paths(
                        obj[i], cur_path + f'[{i}]', append_values,
                        paths, black_list)
                else:
                    if not key_added:
                        if append_values:
                            to_append = [cur_path, obj]
                        else:
                            to_append = cur_path
                        paths.append(to_append)
                        key_added = True

    return paths
