import requests
import os
import json

bearer_token = "AAAAAAAAAAAAAAAAAAAAAM2TdAEAAAAAKUGOGvaR8IBCRbybdpHTl2oyikU%3D02O8fWBJH5mpoSAckfjpBkQmkLo2JYOUC5cOu4MWwwpOSBPrDs"

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
        
def set_rules(sample_rules):

    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
def get_stream():
    params = {'tweet.fields': 'created_at,source'}
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth,params=params, stream=True,
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            print(json.dumps(json_response, indent=4, sort_keys=True))
            print('--------------------')


sample_rules = [
        {"value": "élections lang:fr", "tag": "election"},
        {"value": "élection lang:fr", "tag": "election2"},
        {"value": "élections législatives lang:fr", "tag": "election3"},
        {"value": "législative lang:fr", "tag": "election4"}
    ]

rules = get_rules()
delete = delete_all_rules(rules)
set1 = set_rules(sample_rules)
get_stream()