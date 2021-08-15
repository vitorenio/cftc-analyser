import argparse
from datetime import datetime
from report_utils import *


# Reports Dated August 3, 2021 - Current Disaggregated Reports:
REPORT_1_URL = 'https://www.cftc.gov/dea/newcot/f_disagg.txt'


def process_report_old(args):
    header_array = get_header_array()
    idx_array = []
    b()
    print('Fields available ([index] - name)')
    b()
    for idx, val in enumerate(header_array):
        if (val != ''):
            idx_array.append(idx)
            print(f'[{idx}] - {val}')
    b()

    gold_price = None
    date = None
    for row in get_report(REPORT_1_URL):
        accumulator_long = 0
        accumulator_short = 0

        assert_name = row[ASSET_NAME_IDX]

        if date is None:
            date = datetime.fromisoformat(row[DATE_IDX])
        if gold_price is None:
            assert date != None
            gold_price = get_gold_price(date)
            print(f'Gold close price on {date} = ${gold_price}')

        code = row[CODE_IDX]

        for idx in get_accumulators_long(args.position_categories):
            accumulator_long += get_int(idx, row)

        for idx in get_accumulators_short(args.position_categories):
            accumulator_short += get_int(idx, row)

        net_long = accumulator_long - accumulator_short

        print(f'{code},{assert_name},{date},{net_long},{gold_price}')

        # break  # FOR TEST ONLY: PROCESS ONLY ONE LINE


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Commitments of Traders Data Analysis Tool", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(dest='position_categories',
                        metavar='--net_long',
                        type=str,
                        nargs='?',
                        help='compute net long for the given position categories. Allowed values are: %(choices)s',
                        choices=['PRODUCER_MERCHANT_PROCESSOR_USER_ALL',
                                 'SWAP_DEALERS_ALL',
                                 'MANAGED_MONEY_ALL',
                                 'OTHER_REPORTABLES_ALL',
                                 'NONREPORTABLE_POSITIONS_ALL'],

                        default=[
                            #'PRODUCER_MERCHANT_PROCESSOR_USER_ALL',
                            #'SWAP_DEALERS_ALL',
                            'MANAGED_MONEY_ALL',
                            'OTHER_REPORTABLES_ALL',
                            #'NONREPORTABLE_POSITIONS_ALL'
                        ]

                        )
    args = parser.parse_args()
    print(args)
    # process_report_old(args)
    process_cftc_report(args)
    print("\n\n\nfinish.\n")

    """

    tesla = TA_Handler(
    symbol="GC1!",
    screener="america",
    exchange="COMEX",
    interval=Interval.INTERVAL_1_DAY
    )

    ans = tesla.get_indicators()["close"]

    print(f'Close: {ans}')
    print(tesla.get_analysis().summary)
    """
