import csv
import datetime
import urllib.request
from datetime import timedelta
import yfinance as yf
from typing import Dict
import pandas as pd
import zipfile
import os
from pandas.core.frame import DataFrame

YF_GOLD_TICKER = 'GC=F'
YF_CLOSE = 'Close'

MAX_SHEETNAME_LENGTH = 31
DISALLOWED_CHARS = "\/*?:[]"
SUPPORTED_FILE_1 = "F_Disagg06_16.txt"
SUPPORTED_FILE_2 = "f_year.txt"

OUTPUT_FILE = "./output/cftc_report.xlsx"

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
def get_gold_price_history(start_date):
    gcf = yf.Ticker(YF_GOLD_TICKER)
    return gcf.history(start=start_date, end=datetime.datetime.today())
      

def process_zip_file(writer, file, df_map: Dict[str, DataFrame], args, gold_price_history) -> None:
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

    for idx in df.index:
        val = df['Market_and_Exchange_Names'][idx]
        date = df['Report_Date_as_YYYY-MM-DD'][idx]
        try:
            gold_price = gold_price_history[YF_CLOSE][date]
        except:
            print(f'gold price for date {date} not found. Using the last gold price found: {gold_price}')
        
        accumulator_long = 0
        accumulator_short = 0    
        
        for col in get_accumulators_long(args.position_categories):
            accumulator_long += df.at[idx, df.columns[col]]

        for col in get_accumulators_short(args.position_categories):
            accumulator_short += df.at[idx, df.columns[col]]

        net_long = accumulator_long - accumulator_short
        if val in asset_map:
            asset_map[val] += [[date, net_long, gold_price]]
        else:
            asset_map[val] = [[date, net_long, gold_price]]   


    for tab in asset_map.items():
        asset_name = tab[0]
        #print('processing ' + asset_name)
        #print(f'key={asset_name}')
        tuples = [[]]
        for values in tab[1]:
            date = values[0]
            positions = values[1]
            gold_prices = values[2]
            
            tuples.append([date, positions, gold_prices])

            #print(f'  {date}, {positions}')

        # each dataframe is considered a sheet. We need to incrementally write in the dataframe, 
        # because sheets can't be overwritten in panda
        # CAUTION: this map can get huuuuge
        if asset_name in df_map: 
            # if there is already a dataframe for this asset, append the data
            df = df_map[asset_name]
            
            df_new_data = pd.DataFrame(tuples, columns=['Date', 'Net longs', 'Gold Price'])
            df_map[asset_name] = df.append(df_new_data, ignore_index=True)
        else:
            # otherwise, create the data for this asset for the first time
            df = pd.DataFrame(tuples, columns=['Date', 'Net longs', 'Gold Price'])
            df_map[asset_name] = df
        #print(df)


def process_cftc_report(args):
    if os.path.isfile(OUTPUT_FILE):
        print("clean up...")
        os.remove(OUTPUT_FILE)

    directory = "./downloads/"
    with pd.ExcelWriter("./output/test_2010.xlsx") as writer:
        df_map = {}
        gold_price_history = get_gold_price_history("2005-01-01")
        for filename in os.listdir(directory):
            f = os.path.join(directory, filename)
            # checking if it is a file
            if os.path.isfile(f):
                print("starting process...")
                process_zip_file(writer, f, df_map, args, gold_price_history)

        for asset_name, df in df_map.items():
            sheet_name = (asset_name[:MAX_SHEETNAME_LENGTH]) if len(asset_name) > MAX_SHEETNAME_LENGTH else asset_name
            sheet_name = sheet_name.translate({ ord(c): None for c in DISALLOWED_CHARS })

            print(f"writing sheet {sheet_name}")
            df.to_excel(writer, sheet_name=sheet_name, index=False)