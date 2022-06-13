import geocoder
from loguru import logger
import geopandas as gp
from shapely.geometry import Point
import requests


def geocode(address: str):
    data = gp.tools.geocode(address)
    logger.debug(data['geometry'][0])
    geometry = data['geometry'][0]
    if geometry.geom_type == 'Point':
        return {
            'lat': geometry.y,
            'lng': geometry.x,
        }
    else:
        return None

def create_geoseries(l) -> gp.GeoDataFrame:
    points = map(lambda latlng: Point(latlng[1], latlng[0]), l)
    return gp.GeoDataFrame(points)

def geocode_geoapify(address: str, apiKey: str):
    try:
        response = requests.get('https://api.geoapify.com/v1/geocode/search?text={}&apiKey={}'.format(address, apiKey))
        json = response.json()
        results = json['results']
        if len(results) == 0:
            logger.warning('No geoapify results for address: {}'.format(address))
            return None
        return {
            'lat': results[0]['lat'],
            'lng': results[0]['lon'],
        }
    except e:
        logger.error(e)
        return None