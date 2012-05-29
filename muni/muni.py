import sys     
import pymongo
from common import utils
from common import config

MUNI_ROUTE_LIST_URL = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeList&a=sf-muni'
MUNI_ROUTE_CONFIG_URL = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a=sf-muni&r=%s'

def get_route_list():
    routes = utils.get_url_resource(MUNI_ROUTE_LIST_URL)
    if not (routes and 'body' in routes and 'route' in routes['body']):
        return None
    return routes['body']['route'] 

def get_route_config(route):
    route_configs = utils.get_url_resource(MUNI_ROUTE_CONFIG_URL % route)
    if not (route_configs and 'body' in route_configs and 
        'route' in route_configs['body'] and
        'stop' in route_configs['body']['route']):
        return None
    return route_configs['body']['route'] 

if __name__ == "__main__":
    m = pymongo.Connection(config.MONGO_URL)
    #m = pymongo.Connection()
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
    
    for route in routes:
        routes_collection.insert(route)
        route_configs = get_route_config(route['@tag'])
        if route_configs:
            # map stop tag to direction
            stop_direction_dict = {}
            for direction in route_configs['direction']:
                # handle the case where there is only one stop
                try:
                    if isinstance(direction['stop'], list):
                        for direction_stop in direction['stop']:
                            stop_direction_dict[direction_stop['@tag']] = (
                                {'direction_tag': direction['@tag'], 
                                'direction_name': direction['@name']})
                    elif isinstance(direction['stop'], str):
                        direction_stop = direction['stop'] 
                        stop_direction_dict[direction_stop['@tag']] = (
                            {'direction_tag': direction['@tag'], 
                            'direction_name': direction['@name']})
                    else:
                        print("unexpected type of direction['stop']: %s" % direction['stop'])
                except:
                    print("ERROR!!!!!!!!!!!! %s" % direction)

            # store information
            for stop in route_configs['stop']:
                # store route into the route config
                stop['route'] = route['@tag']
                # store location and remove @lon and @lat 
                stop['location'] = (float(stop['@lon']), float(stop['@lat']))
                del stop['@lon'], stop['@lat']
                if stop_direction_dict.get(stop['@tag']):
                    stop.update(stop_direction_dict[stop['@tag']])
                    print("saving %s" % stop)
                    route_configs_collection.insert(stop)
                else:
                    print("not saving %s" % stop)

    # generate geospatial indices
    route_configs_collection.ensure_index([('location', pymongo.GEO2D)], unique=False)
