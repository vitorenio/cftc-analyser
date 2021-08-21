import csv
from datetime import datetime
import urllib.request
import yfinance as yf
from typing import Dict
import pandas as pd
import numpy as np
import zipfile
import os
from pandas.core.frame import DataFrame
from alive_progress import alive_bar

YF_GOLD_TICKER = 'GC=F'
YF_CLOSE = 'Close'

MAX_SHEETNAME_LENGTH = 31
DISALLOWED_CHARS = "\/*?:[]"
SUPPORTED_FILE_1 = "F_Disagg06_16.txt"
SUPPORTED_FILE_2 = "f_year.txt"

OUTPUT_FILE = "./output/cftc_report.xlsx"

# csv header: asset_alias,asset_code,yf_ticker
ASSET_MAP_INPUT_FILENAME = 'input.csv'
ASSET_ALIAS_COL = 'asset_alias'
ASSET_CODE_COL = 'asset_code'
ASSET_YF_TICKER = 'yf_ticker'

# CFTC data columns
REPORT_DATE_AS_YYYY_MM_DD = 'Report_Date_as_YYYY-MM-DD'

# output column names
OUTPUT_DATE_COL = 'Date'
OUTPUT_NET_LONGS_COL = 'Net longs'
OUTPUT_PRICE_COL = 'Gold Price'

# Indexes
ASSET_NAME_IDX = 0
DATE_IDX = 2
CODE_IDX = 3
PRODUCER_MERCHANT_PROCESSOR_USER_ALL_LONG = 8
PRODUCER_MERCHANT_PROCESSOR_USER_ALL_SHORT = 9
SWAP_DEALERS_ALL_LONG = 10
SWAP_DEALERS_ALL_SHORT = 11
MANAGED_MONEY_ALL_LONG = 13
MANAGED_MONEY_ALL_SHORT = 14
OTHER_REPORTABLES_ALL_LONG = 16
OTHER_REPORTABLES_ALL_SHORT = 17
NONREPORTABLE_POSITIONS_ALL_LONG = 21
NONREPORTABLE_POSITIONS_ALL_SHORT = 22

def b():
    print('=' * 80)


def get_header_array():
    header_array = []
    with open('headers.csv', newline='') as csvfile:
        header_reader = csv.reader(csvfile, delimiter=',')
        # read the first line only
        for row in header_reader:
            header_array += row
            break
    return header_array


def get_report(report_url):
    response = urllib.request.urlopen(report_url)
    lines = [l.decode('utf-8') for l in response.readlines()]

    cr = csv.reader(lines)

    result = []
    for row in cr:
        result += [row]

    return result


def assert_int(val):
    assert '.' not in val  # should be an int


def get_int(idx, row):
    val = row[idx]
    assert_int(val)
    return int(val)

def get_accumulators(cats, suffix):
    accumulators = []
    for cat in cats:
        cat_name = cat + suffix
        if cat_name in globals():
            accumulators.append(globals()[cat_name])
    
    return accumulators

def get_accumulators_long(cats):
    return get_accumulators(cats, '_LONG')


def get_accumulators_short(cats):
    return get_accumulators(cats, '_SHORT')


# method to get the price of gold
def get_asset_price_history(asset_map_filter):
    asset_price_history = {}
    print('Collect price history...')
    with alive_bar(asset_map_filter.shape[0], bar='classic2') as bar:
        for asset_code, asset_ticker in zip(asset_map_filter[ASSET_CODE_COL], asset_map_filter[ASSET_YF_TICKER]):
            print(f"Collecting price history for {asset_ticker}")
            ticker = yf.Ticker(asset_ticker) 
            asset_price_history[asset_code] = ticker.history(start='2000-01-01', end=datetime.today())
            bar()
    return asset_price_history
      

def process_zip_file(file, df_map: Dict[str, DataFrame], args, asset_map_filter, asset_map_price_history, error_msgs) -> None:
    print(f'processing zip file {file}')
    zf = zipfile.ZipFile(file) 
    
    fname = None
    for f in zf.filelist:
        if f.filename in [SUPPORTED_FILE_1, SUPPORTED_FILE_2]:
            fname = f.filename
    if fname is None:
        #print(zf.filelist)
        raise Exception("No supported file found. Aborting.")
    df = pd.read_csv(zf.open(fname))

    asset_map = {}
    
    print('compute net longs... ')
    with alive_bar(df.index.size, bar='classic2') as bar:
        for idx in df.index:
            asset_code = df['CFTC_Contract_Market_Code'][idx]
            asset_name = df['Market_and_Exchange_Names'][idx]

            # proces only assets presents in the input file
            sel_indices = np.where(asset_map_filter[ASSET_CODE_COL] == asset_code)
            if len(sel_indices[0]) >= 1:
                if len(sel_indices[0]) > 1:
                    raise Exception("duplicated assets found in " + ASSET_MAP_INPUT_FILENAME)
                
                asset_filter_idx = sel_indices[0][0]
                asset_alias = asset_map_filter[ASSET_ALIAS_COL][asset_filter_idx]
                sheet_name = asset_alias + ' - ' + asset_code
                date = df[REPORT_DATE_AS_YYYY_MM_DD][idx]
                try:
                    asset_price = asset_map_price_history[asset_code][YF_CLOSE][date]
                except:
                    error_msgs.append(f'{asset_alias} price for date {date} not found. Using the last price found: {asset_price}')
                
                accumulator_long = 0
                accumulator_short = 0    
                
                for col in get_accumulators_long(args.position_categories):
                    accumulator_long += df.at[idx, df.columns[col]]

                for col in get_accumulators_short(args.position_categories):
                    accumulator_short += df.at[idx, df.columns[col]]

                net_long = accumulator_long - accumulator_short
                if sheet_name in asset_map:
                    asset_map[sheet_name] += [[date, net_long, asset_price]]
                else:
                    asset_map[sheet_name] = [[date, net_long, asset_price]]
            bar()

    print('mount the sheets... ')
    for tab in asset_map.items():
        asset_name = tab[0]
        #print('processing ' + asset_name)
        #print(f'key={asset_name}')
        tuples = []
        for values in tab[1]:
            date = values[0]
            positions = values[1]
            prices = values[2]
            
            tuples.append([date, positions, prices])

            #print(f'  {date}, {positions}')

        # each dataframe is considered a sheet. We need to incrementally write in the dataframe, 
        # because sheets can't be overwritten in panda
        # CAUTION: this map can get huuuuge
        if asset_name in df_map: 
            # if there is already a dataframe for this asset, append the data
            df = df_map[asset_name]

            df_new_data = pd.DataFrame(tuples, columns=[OUTPUT_DATE_COL, OUTPUT_NET_LONGS_COL, OUTPUT_PRICE_COL])
            df_map[asset_name] = df.append(df_new_data, ignore_index=True)
        else:
            # otherwise, create the data for this asset for the first time
            df = pd.DataFrame(tuples, columns=[OUTPUT_DATE_COL, OUTPUT_NET_LONGS_COL, OUTPUT_PRICE_COL])
            df_map[asset_name] = df
        #print(df)


def process_cftc_report(args):
    if os.path.isfile(OUTPUT_FILE):
        print("clean up...")
        os.remove(OUTPUT_FILE)

    directory = "./downloads/"
   
    dt = datetime.now()
    timestamp = datetime.timestamp(dt)
    with pd.ExcelWriter(f"./output/test_cftc_report_{timestamp}.xlsx") as writer:
        df_map = {}
        asset_map_filter = pd.read_csv(ASSET_MAP_INPUT_FILENAME, dtype=str)
        asset_map_price_history = get_asset_price_history(asset_map_filter)
        error_msgs = []
        for filename in os.listdir(directory):
            f = os.path.join(directory, filename)
            # checking if it is a file
            if os.path.isfile(f):
                print("starting process...")
                process_zip_file(f, df_map, args, asset_map_filter, asset_map_price_history, error_msgs)

        if 0 == len(df_map):
            raise Exception("empty result. aborting.")
        if 0 < len(error_msgs):
            for msg in error_msgs:
                print(msg)

        print("\n\n\nfinish him...\n")
        with alive_bar(len(df_map.items()), bar='classic2') as bar:
            for asset_name, df in df_map.items():
                df.sort_values(by=[OUTPUT_DATE_COL], inplace=True)
                sheet_name = (asset_name[:MAX_SHEETNAME_LENGTH]) if len(asset_name) > MAX_SHEETNAME_LENGTH else asset_name
                sheet_name = sheet_name.translate({ ord(c): None for c in DISALLOWED_CHARS })

                print(f"writing sheet {sheet_name}")
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                bar()