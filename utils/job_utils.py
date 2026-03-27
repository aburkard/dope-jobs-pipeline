import requests


# TODO: Make this better, possibly a static list
def get_company_name(board_token):
    return board_token.title().replace('-', ' ')
