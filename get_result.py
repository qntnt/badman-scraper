import requests
import tabula as tb
import os
from loguru import logger
import platform

TMP_DIR = '.tmp'

def noneOrEmpty(s: str) -> bool:
    return type(s) is str and (s is None or len(s.strip()) == 0)

def result_is_invalid(result):
    return (
        'property_address' not in result or
        'owner_address' not in result or
        'owner' not in result or
        'parcel_number' not in result or
        'property_class' not in result
    )

def get_result_with_context(broken_parcels: set):
    def try_get_result(parcel_id: str):
        result = get_result(parcel_id)
        if 'owner' not in result:
            broken_parcels.add(result['parcel_number'])
        return result
    return try_get_result

def get_result(parcel_id: str):
    pdf_name = '{}/{}.pdf'.format(TMP_DIR, parcel_id)
    result = {
        'parcel_number': parcel_id
    }
    try:
        r = requests.get(
            'https://maps.indy.gov/AssessorPropertyCards/handler/proxy.ashx?https%3A//maps.indy.gov/AssessorPropertyCards.Reports.Service/Service.svc/PropertyCard/{0}'.format(parcel_id),
        )
        with open(pdf_name, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        table = tb.read_pdf(pdf_name, pages='all', silent=True, guess=False)
        sheet = table[0]

        system = platform.system()

        # logger.trace('"sheet":\n{}', sheet)
        # logger.trace('"Ownership" col:\n{}', sheet['Ownership'])
        # logger.trace('"Parcel Number" col:\n{}', sheet['Parcel Number'])
        # logger.trace('"Unnamed: 0" col:\n{}', sheet['Unnamed: 0'])
        if system == 'Darwin': # Mac
            result['county'] = sheet['Ownership'][1]
            result['township'] = sheet['Ownership'][2]
            result['property_address'] = sheet['Parcel Number'][13]
            if result['property_address'] == 'Property Address':
                result['property_address'] = sheet['Parcel Number'][14]
            result['owner'] = sheet['Unnamed: 0'][1]
            result['owner_address'] = sheet['Unnamed: 0'][7]
            result['alt_parcel_number'] = sheet['Ownership'][7]
            result['tax_district'] = sheet['Ownership'][9]
            result['neighborhood'] = sheet['Ownership'][10]
            result['property_class'] = sheet['Ownership'][8]
            result['property_subclass'] = sheet['Ownership'][29]
        elif system == 'Windows':
            result['county'] = sheet['Ownership'][1]
            result['township'] = sheet['Ownership'][2]
            result['property_address'] = sheet['Parcel Number'][13]
            if result['property_address'] == 'Property Address':
                result['property_address'] = sheet['Parcel Number'][14]
            result['owner'] = sheet['Unnamed: 0'][1]
            result['owner_address'] = sheet['Unnamed: 0'][7]
            result['alt_parcel_number'] = sheet['Ownership'][7]
            result['tax_district'] = sheet['Ownership'][9]
            result['neighborhood'] = sheet['Ownership'][10]
            result['property_class'] = sheet['Ownership'][8]
            result['property_subclass'] = sheet['Ownership'][29]
            
        if result_is_invalid(result):
            logger.warning('{} is invalid', parcel_id)
            logger.trace('"Ownership":\n{}', sheet['Ownership'][0:30])
            logger.trace('"Parcel Number":\n{}', sheet['Ownership'][0:30])
            logger.trace('"Unnamed: 0":\n{}', sheet['Unnamed: 0'][0:30])
            logger.trace('{}', result)
    finally:
        for key in result:
            if type(result[key]) is str:
                result[key] = result[key].replace('\r', ' ')
                result[key] = result[key].replace('\n', ' ')
                result[key] = result[key].replace('\t', ' ')
        if pdf_name in os.listdir(TMP_DIR):
            os.remove(pdf_name)
        return result