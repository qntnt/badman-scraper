import requests
import tabula as tb
import os
from loguru import logger

TMP_DIR = '.tmp'

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

        logger.debug('{}', sheet['Parcel Number'][0:20])
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
    finally:
        if pdf_name in os.listdir(TMP_DIR):
            os.remove(pdf_name)
        logger.debug('{}', result)
        return result