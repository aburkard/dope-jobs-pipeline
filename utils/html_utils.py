import html
import re


def remove_html_markup(text, double_unescape=False):
    text = html.unescape(text)
    # Greenhouse requires double-escaping
    if double_unescape:
        text = html.unescape(text)

    clean_text = re.sub(r'<[^>]+>', '', text)

    return clean_text
