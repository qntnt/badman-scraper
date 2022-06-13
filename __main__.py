#!/usr/bin/env python3
import pandas as pd
import csv
from multiprocessing.pool import ThreadPool
from geocode import create_geoseries, geocode
from get_result import get_result_with_context
from alive_progress import alive_bar
import argparse
import os
from loguru import logger
import sys
from textutils import similarity, clean_address
import tkinter as tk
from tkinter import filedialog, simpledialog
import geopandas as gp
from shapely.geometry import Point

root = tk.Tk()

def noneOrEmpty(s: str):
    return s is None or len(s.strip()) == 0 

field_names = [
    'parcel_number', 
    'alt_parcel_number',
    'county',
    'township',
    'property_address',
    'owner', 
    'owner_address',
    'tax_district',
    'neighborhood',
    'property_class',
    'property_subclass',
]

def post_process_row(row):
    processed_row = row
    processed_row['property_address'] = clean_address(processed_row['property_address'])
    processed_row['owner_address'] = clean_address(processed_row['owner_address'])
    processed_row['property_address_owner_address_similarity'] = similarity(row['property_address'], row['owner_address'])
    return processed_row

def geocode_row(processed_row, geoapify_key=None):
    if 'owner_address_lat' not in processed_row or 'owner_address_lng' not in processed_row:
        try:
            geodata = None
            if geoapify_key is None:
                geodata = geocode(processed_row['owner_address'])
            else:
                geodata = geocode_geoapify(processed_row['owner_address'], geoapify_key)
            if geodata is not None:
                processed_row['owner_address_lat'] = geodata['lat']
                processed_row['owner_address_lng'] = geodata['lng']
        except Exception as e:
            logger.error(e)

    if 'property_address_lat' not in processed_row or 'property_address_lng' not in processed_row:
        try:
            geodata = None
            if geoapify_key is None:
                geodata = geocode(processed_row['property_address'])
            else:
                geodata = geocode_geoapify(processed_row['property_address'], geoapify_key)
            if geodata is not None:
                processed_row['property_address_lat'] = geodata['lat']
                processed_row['property_address_lng'] = geodata['lng']
        except Exception as e:
            logger.error(e)

def post_process_output_file(output_file, post_processed_output_filename, geocode=False, concurrency=1, geoapify_key=None):
    logger.info('Post-processing output to {file}', file=post_processed_output_filename)
    rows = []
    with open(output_file, 'r', newline='') as output_file:
        out_reader = csv.DictReader(output_file)
        for row in out_reader:
            rows.append(row)
    with open(post_processed_output_filename, 'w', newline='') as outfile:
        post_processed_field_names = field_names + [ 
            'property_address_owner_address_similarity', 
            'owner_address_lat', 
            'owner_address_lng',
            'property_address_lat',
            'property_address_lng',
        ]
        outwriter = csv.DictWriter(outfile, fieldnames=post_processed_field_names)
        outwriter.writeheader() 

        processed_rows = []
        logger.info('Post processing parcels')
        with alive_bar(len(rows)) as progress, ThreadPool(concurrency) as pool:
            for result in pool.map(post_process_row, rows):
                processed_rows.append(result)
                progress()

        if geocode:
            logger.info('Geocoding parcels')
            with alive_bar(len(processed_rows)) as progress:
                for row in processed_rows:
                    geocode_row(row, geoapify_key)
                    progress()
        
        logger.info('Writing post-processed output')
        for row in processed_rows:
            outwriter.writerow(row)
        outfile.flush()

def generate_shapefile(post_processed_output_filename, shapefilename):
    data = {
        'owner': [],
        'geometry': [],
    }
    with open(post_processed_output_filename, 'r', newline='') as output_file:
        out_reader = csv.DictReader(output_file)
        for row in out_reader:
            if row['owner_address_lat'] != '' and row['owner_address_lng'] != '':
                data['owner'].append(row['owner'])
                data['geometry'].append(Point(float(row['owner_address_lng']), float(row['owner_address_lat'])))
    df = pd.DataFrame(data)
    gdf = gp.GeoDataFrame(df)
    logger.debug(shapefilename)
    gdf.to_file(shapefilename)

def handle_subcommand(subcommand: str, args):
    if subcommand == 'post-process':
        output_file = args.output_file
        if output_file is None:
            root.withdraw()
            output_file = filedialog.askopenfilename(
                title='Select output file to post-process', 
                filetypes=[('Csv files', '.csv',)],
            )
        concurrency = args.concurrency
        if concurrency is None:
            root.withdraw()
            concurrency = simpledialog.askinteger(
                'Set concurrency', 
                'Set the number of documents to request and process at the same time.',
                minvalue=1,
                maxvalue=200,
                initialvalue=20
            )
            concurrency = max(1, min(200, concurrency))
        path, ext = os.path.splitext(output_file)
        post_processed_output_filename = path + '_post_processed' + ext
        post_process_output_file(output_file, post_processed_output_filename, args.geocode, concurrency, args.geoapify_key)
        return
    elif subcommand == 'generate-shapefile':
        post_processed_output_filename = args.post_processed_output_file
        if post_processed_output_filename is None:
            root.withdraw()
            post_processed_output_filename = filedialog.askopenfilename(
                title='Select post-processed output file', 
                filetypes=[('Csv files', '.csv',)],
            )
        root.withdraw()
        shapefile = filedialog.asksaveasfilename(
            title='Save output as',
            filetypes=[('Map files', '.geojson .json')],
            initialfile='owners.geojson',
        )
        generate_shapefile(post_processed_output_filename, shapefile)
        return

    # subcommand == 'all' or undefined
    try:
        input_file = args.input_file
        if input_file is None:
            root.withdraw()
            input_file = filedialog.askopenfilename(title='Select input Excel file', filetypes=[('Excel files', '.xlsx .xls',)])

        output_file = args.output_file
        if output_file is None:
            root.withdraw()
            output_file = filedialog.asksaveasfilename(
                title='Save output as', 
                filetypes=[('Csv files', '.csv',)],
                initialfile='output.csv'
            )

        concurrency = args.concurrency
        if concurrency is None:
            root.withdraw()
            concurrency = simpledialog.askinteger(
                'Set concurrency', 
                'Set the number of documents to request and process at the same time.',
                minvalue=1,
                maxvalue=200,
                initialvalue=20
            )
            concurrency = max(1, min(200, concurrency))


        df = pd.read_excel(input_file)
        parcels = {}
        unprocessed_parcels = set()
        parcel_ids = df['PARCEL_C']

        # Find unprocessed parcels
        if os.path.exists(output_file):
            logger.info('Checking input for unprocessed parcels')
            with open(output_file, 'r', newline='') as out_csv:
                out_reader = csv.DictReader(out_csv)
                with alive_bar(len(parcel_ids)) as progress:
                    for row in out_reader:
                        parcel_id = row['parcel_number']
                        owner = row['owner']
                        property_address = row['property_address']
                        if (not noneOrEmpty(owner) and 'property address' not in property_address.lower()) and parcel_id not in parcels:
                            parcels[parcel_id] = row
                        
                            
                    for parcel_id in parcel_ids:
                        if str(parcel_id) not in parcels:
                            unprocessed_parcels.add(parcel_id)
                        progress()
        else:
            unprocessed_parcels = parcel_ids

        logger.info('{count} unprocessed parcels', count=len(unprocessed_parcels))
        broken_parcels = set()

        # Process unprocessed parcels
        logger.info('Processing parcels. Writing to {file}. This may take a while...', file=output_file)
        with open(output_file, 'w', newline='') as out_csv:        
            out_writer = csv.DictWriter(out_csv, fieldnames=field_names)
            # Set up file
            out_writer.writeheader()
            for processed_parcel in parcels.values():
                out_writer.writerow(processed_parcel)
            out_csv.flush()

            with alive_bar(len(unprocessed_parcels)) as progress, ThreadPool(concurrency) as pool:
                for result in pool.imap(get_result_with_context(broken_parcels), unprocessed_parcels):
                    try:
                        out_writer.writerow(result)
                        out_csv.flush()
                        progress()
                    except Exception as e:
                        print(e)
                        progress()

        broken_file = None
        # Report broken parcels
        if len(broken_parcels) > 0:
            path, file = os.path.split(output_file)
            filename = os.path.join(path, 'broken_parcels.csv')
            logger.warning('Some parcels are broken. Writing their ids to \"{file}\"', file=filename)
            with open(filename, 'w', newline='') as broken_csv:
                out_writer = csv.DictWriter(broken_csv, fieldnames=['broken_parcel_id'])
                out_writer.writeheader()
                for parcel_id in broken_parcels:
                    out_writer.writerow({ 'broken_parcel_id': parcel_id })
            broken_file = filename

        # Clean up files
        logger.info('Cleaning temporary files')
        for rootpath, dirs, files in os.walk(TMP_DIR, topdown=False):
            for name in files:
                os.remove(os.path.join(rootpath, name))
            for name in dirs:
                os.rmdir(os.path.join(rootpath, name))
        os.rmdir(TMP_DIR)

        # Post-process output
        path, ext = os.path.splitext(output_file)
        post_processed_output_filename = path + '_post_processed' + ext
        post_process_output_file(output_file, post_processed_output_filename, args.geocode, concurrency)
        broken_message = ''
        if not noneOrEmpty(broken_file):
            broken_message = '\nBroken parcels written to file: {}'.format(broken_file)
        tk.messagebox.showinfo(
            'badman-scraper', 
            '''Processing complete!

Processed {} files
Output to files: "{}" and "{}"{}'''.format(
                len(parcel_ids), 
                output_file, 
                post_processed_output_filename, 
                broken_message
            ),
        )
    except Exception as e:
        logger.error(e)
        raise e

if __name__ == '__main__':

    TMP_DIR = '.tmp'
    if TMP_DIR not in os.listdir():
        os.makedirs(TMP_DIR)

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Find detailed parcel data based on xlsx formatted parcel listings.', allow_abbrev=True)
    parser.add_argument('-input_file', type=str, required=False, help='Path to parcel listings .xlsx file',)
    parser.add_argument('-concurrency', type=int, required=False, help='The number of concurrent threads to process parcels (be careful not to set this too high)')
    parser.add_argument('-output_file', type=str, required=False, help='The output .csv file')
    parser.add_argument('-post_processed_output_file', type=str, required=False, help='The post-processed output .csv file')
    parser.add_argument(
        '-log', 
        type=str, 
        required=False,
        help='The log level for output',
        choices=('CRITICAL', 'ERROR','WARNING', 'SUCCESS', 'INFO', 'DEBUG', 'TRACE'),
        default='INFO'
    )
    parser.add_argument(
        '-subcommand', 
        type=str, 
        required=False, 
        default='all', 
        help='The subcommand to run',
        choices=('all', 'process', 'post-process', 'generate-shapefile'),
    )
    parser.add_argument(
        '-geocode',
        type=bool,
        required=False,
        default=False,
        help='Include geocoding in post-processed results. Very slow...',
    )
    parser.add_argument(
        '-geoapify_key',
        type=str,
        required=False,
        help='Geoapify Api Key for geocoding',
    )
    args = parser.parse_args()

    logger.configure(
        handlers = [
            { 
                'sink': sys.stdout,
                'format':'<green>{time:YYYY:MM:DD HH:mm:ss zz}</green> | <level>{level}: {message}</level>',
                'level': args.log,
            },
        ],
    )

    handle_subcommand(args.subcommand, args)
    root.withdraw()
