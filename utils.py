def check_if_url(element): return True if 'youtube.com' in element else False


def get_keys_with_non_dict_values(dict_to_check: dict):
    count = 0
    final_keys = []

    def recurse(dict_: dict):
        """Returns keys whose values are not dictionaries"""
        # print(dict_.keys())
        for i in dict_.keys():
            if isinstance(dict_[i], dict):
                recurse(dict_[i])
            else:
                final_keys.append(i)
                nonlocal count
                count += 1
    recurse(dict_to_check)

    return final_keys


def get_missing_keys_from_dict(dict_to_check: dict, keys_to_check_for: list):
    final_keys = get_keys_with_non_dict_values(dict_to_check)
    tags_missing = [tag for tag in keys_to_check_for if tag not in final_keys]

    return tags_missing
