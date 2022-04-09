#!/usr/bin/env python3
import pandas as pd
import csv
from multiprocessing.pool import ThreadPool
from get_result import get_result_with_context
from alive_progress import alive_bar
import argparse
import os
from loguru import logger
import sys
from textutils import similarity, clean_address
import tkinter as tk
from tkinter import filedialog, simpledialog

if __name__ == '__main__':
    def noneOrEmpty(s: str):
        return s is None or len(s.strip()) == 0 
    root = tk.Tk()
    try:
        TMP_DIR = '.tmp'
        if TMP_DIR not in os.listdir():
            os.makedirs(TMP_DIR)

        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Find detailed parcel data based on xlsx formatted parcel listings.', allow_abbrev=True)
        parser.add_argument('-input_file', type=str, required=False, help='Path to parcel listings .xlsx file',)
        parser.add_argument('-concurrency', type=int, required=False, help='The number of concurrent threads to process parcels (be careful not to set this too high)')
        parser.add_argument('-output_file', type=str, required=False, help='The output .csv file')
        parser.add_argument(
            '-log', 
            type=str, 
            required=False,
            help='The log level for output',
            choices=('CRITICAL', 'ERROR','WARNING', 'SUCCESS', 'INFO', 'DEBUG', 'TRACE'),
            default='INFO'
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
        logger.info('Post-processing output to {file}', file=post_processed_output_filename)
        rows = []
        with open(output_file, 'r', newline='') as output_file:
            out_reader = csv.DictReader(output_file)
            for row in out_reader:
                rows.append(row)
        with open(post_processed_output_filename, 'w', newline='') as outfile:
            post_processed_field_names = field_names + [ 'property_address_owner_address_similarity' ]
            outwriter = csv.DictWriter(outfile, fieldnames=post_processed_field_names)
            outwriter.writeheader() 
            with alive_bar(len(rows)) as progress:
                for row in rows:
                    processed_row = row
                    processed_row['property_address'] = clean_address(processed_row['property_address'])
                    processed_row['owner_address'] = clean_address(processed_row['owner_address'])
                    processed_row['property_address_owner_address_similarity'] = similarity(row['property_address'], row['owner_address'])
                    outwriter.writerow(processed_row)
                    outfile.flush()
                    progress()
        broken_message = ''
        if not noneOrEmpty(broken_file):
            broken_message = '\nBroken parcels written to file: {}'.format(broken_file)
        tk.messagebox.showinfo(
            'badman-scraper', 
            '''Processing complete!

Processed {} files
Output to files: "{}" and "{}"{}'''.format(
                len(parcel_ids), 
                output_file.name, 
                post_processed_output_filename, 
                broken_message
            ),
        )
    except Exception as e:
        root.withdraw()
        tk.messagebox.showerror('badman-scraper', '{}'.format(e))
        raise e
