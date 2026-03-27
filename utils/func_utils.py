def slice(a_dict, keys):
    return {key: a_dict[key] for key in keys}


def squish(text):
    return ' '.join(text.split())
