import sys     
import pymongo
import requests
from third_party import xml2json
import json

MUNI_ROUTE_LIST_URL = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeList&a=sf-muni'
MUNI_ROUTE_CONFIG_URL = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a=sf-muni&r=%s'

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

def get_route_list():
    routes = get_url_resource(MUNI_ROUTE_LIST_URL)
    if not (routes and routes['body'] and routes['body']['route']):
        return None
    return routes 

def get_route_config(route):
    route_configs = get_url_resource(MUNI_ROUTE_CONFIG_URL % route)
    if not (route_configs and route_configs['body'] and route_configs['body']['route']
        and route_configs['body']['route']['stop']):
        return None
    return route_configs 

if __name__ == "__main__":
    #m = pymongo.Connection(MONGO_URL)
    m = pymongo.Connection()
    db = m['hd_project']
    routes_collection = db['muni_routes']
    route_configs_collection = db['muni_route_configs']

    # clean up 
    routes_collection.drop()
    route_configs_collection.drop()

    routes = get_route_list()
    if not routes:
        print("no routes")
        sys.exit(-1)
    
    for route in routes['body']['route']:
        routes_collection.insert(route)
        route_configs = get_route_config(route['@tag'])
        if route_configs:
            # map stop tag to direction
            stop_direction_dict = {}
            for direction in route_configs['body']['route']['direction']:
                # handle the case where there is only one stop
                try:
                    if isinstance(direction['stop'], list):
                        for direction_stop in direction['stop']:
                            stop_direction_dict[direction_stop['@tag']] = direction['@name']
                    elif isinstance(direction['stop'], str):
                        direction_stop = direction['stop'] 
                        stop_direction_dict[direction_stop['@tag']] = direction['@name']
                    else:
                        print("unexpected type of direction['stop']: %s" % direction['stop'])
                except:
                    print("ERROR!!!!!!!!!!!! %s" % direction)

            # store information
            for stop in route_configs['body']['route']['stop']:
                # store route into the route config
                stop['route'] = route['@tag']
                if stop_direction_dict.get(stop['@tag']):
                    stop['direction'] = stop_direction_dict[stop['@tag']]
                    print("saving %s" % stop)
                    route_configs_collection.insert(stop)
                else:
                    print("not saving %s" % stop)

