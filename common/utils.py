import requests
from third_party import xml2json
import json

def get_url_resource(url):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    json_string = xml2json.xml2json(r.text)
    try:
        result = json.loads(json_string)
    except:
        print("failed to decode json string")
        return None
    return result
