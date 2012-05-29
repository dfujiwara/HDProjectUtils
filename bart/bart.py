import sys     
import pymongo
from common import utils
from common import config

BART_STATIONS_URL = 'http://api.bart.gov/api/stn.aspx?cmd=stns&key=MW9S-E7SL-26DU-VV8V'
BART_STATION_URL = 'http://api.bart.gov/api/stn.aspx?cmd=stninfo&orig=%s&key=MW9S-E7SL-26DU-VV8V' 
BART_ROUTE_URL = 'http://api.bart.gov/api/route.aspx?cmd=routeinfo&route=all&key=MW9S-E7SL-26DU-VV8V'

def get_stations():
    """ get the list of all stations """
    stations = utils.get_url_resource(BART_STATIONS_URL)
    if not (stations and 'root' in stations and 
        'stations' in stations['root'] and 'station' in stations['root']['stations']):
        return None
    return stations['root']['stations']['station']

def get_station_info(station_id):
    """ get the information of the given station """
    station_info = utils.get_url_resource(BART_STATION_URL % station_id)
    if not (station_info and 'root' in station_info and
        'stations' in station_info['root'] and 'station' in station_info['root']['stations']):
        return None
    return station_info['root']['stations']['station']

def get_routes_info():
    """ get the information of the all routes """
    routes = utils.get_url_resource(BART_ROUTE_URL)
    if not (routes and 'root' in routes and
        'routes' in routes['root'] and 'route' in routes['root']['routes']):
        return None
    return routes['root']['routes']['route']

if __name__ == "__main__":
    #m = pymongo.Connection(config.MONGO_URL)
    m = pymongo.Connection()
    db = m['hd_project']
    stations_collection = db['bart_stations']

    # clean up 
    stations_collection.drop()

    stations = get_stations()
    if not stations:
        print("no stations")
        sys.exit(-1)

    station_dict = {}
    for station in stations:
        # get each station's information which includes id, name, address, and geo-location
        # and store in the dictinary which is keyed by the station's id/abbr
        station_info = station_dict.setdefault(station['abbr'], {})
        station_info['id'] = station['abbr']
        station_info['name'] = station['name'] 
        station_info['address'] = "%s %s %s %s" % (station['address'], station['city'], 
            station['state'], station['zipcode'])
        station_info['location'] = (float(station['gtfs_longitude']), 
            float(station['gtfs_latitude']))

    routes = get_routes_info()
    if not routes:
        print("no stations")
        sys.exit(-1)

    # retrieve all routes information
    # each route info is associated with its origin and destination marked by respective id and name
    route_dict = {}
    for route in routes:
        route_info = route_dict.setdefault(route['routeID'], {})
        route_info['origin'] = ({'id': route['origin'], 
            'name': station_dict[route['origin']]['name']})
        route_info['destination'] = ({'id': route['destination'], 
            'name': station_dict[route['destination']]['name']})
        
    # helper function to get the information from route_dict
    # returns a dictionary mapping of route id and route desintation name
    def getRouteInfo(route_id):
        return {'id': route, 'destination': route_dict[route]['destination']['name']}

    # traverse station dict once again to back-fill the route information of each station
    # this has to be done way since route information depends on the station information
    for station_key, station_value in station_dict.iteritems():
        # for each station, get the routes that go through it
        station_info = get_station_info(station_key)
        if not station_info:
            print("no station info for %s" % station_key)
            sys.exit(-1)

        for direction in ('north_routes', 'south_routes'):
            if direction in station_info and station_info[direction]:
                routes = station_info[direction]['route']
                route_list = station_value.setdefault(direction, [])
                if isinstance(routes, list):
                    for route in routes:
                        route_list.append(getRouteInfo(route))
                else:
                    route = routes
                    route_list.append(getRouteInfo(route))
                    
        # insert the finalized station data 
        stations_collection.insert(station_value)

    # generate geospatial indices
    stations_collection.ensure_index([('location', pymongo.GEO2D)], unique=False)
