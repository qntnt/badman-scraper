import geocoder
from loguru import logger
import geopandas as gp
from shapely.geometry import Point


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