from re import sub


def camel_case(s):
    """Transform snakeCase string to camel_case."""
    s = sub(r"(_|-)+", " ", s).title().replace(" ", "")
    return "".join([s[0].lower(), s[1:]])
