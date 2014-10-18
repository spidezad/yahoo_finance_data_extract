"""
    Consolidated stocks information gather.
    Conolidiated script for making decision.

    auto generate the temp file.

    TODO:
        combine prelim stock filter --> add in relevant data than secondary filter.
        Remove some of the word descriptions
        storing data to noSQL database
        how to print to text
        add in modified parametesr
        joined the filename
        add in global data

"""

import re, sys, os, time, datetime
import pandas

from Basic_data_filter import InfoBasicFilter
from yahoo_finance_data_extract import YFinanceDataExtr
from direct_yahoo_finance_scaping import YFinanceDirectScrape
from yahoo_finance_historical_data_extract import YFHistDataExtr

if __name__ == '__main__':
    choice  = 1
    partial_run = ['a','b','c','d']
    partial_run = ['c']

    if choice == 1:

        ## parameters
        final_store_filename = r'c:\data\full_oct16.csv'      
        #full_stock_data_df = object()


        ## Initial stage, getting most raw data.
        if 'a' in partial_run:
            data_ext = YFinanceDataExtr()
            data_ext.set_stock_sym_append_str('')
            
            ## running stock information -- select type to watch -- get all the stock info
            data_ext.set_stock_retrieval_type('all') #'all', watcher
            data_ext.load_stock_symbol_fr_file()
            
            ##comment below if running the full list.
            #data_ext.set_full_stocklist_to_retrieve(['S58.SI','S68.SI'])
            data_ext.get_cur_quotes_fr_list()
            #print data_ext.temp_full_data_df

            ## save to temp file for enable filtering
            data_ext.temp_full_data_df.to_csv(r'c:\data\temp\temp_stockdata.csv')

        ## basic filtering to remove those irrelvant stocks.
        ## the criteria set is very loose based on below.
        if 'b' in partial_run:
            ss =  InfoBasicFilter(r'c:\data\temp\temp_stockdata.csv')
            ss.set_criteria_type('basic')
            ss.get_all_criteria_fr_file()
            ss.process_criteria()
            print "stocks left after basic filtering: ", len(ss.modified_df)
            #ss.modified_df.to_csv('c:\data\temp\temp_stockdata_basic_filter.csv', index = False)

        if 'c' in partial_run:
        ## 3 days trends data
            print "Getting Trends data"
            trend_ext = YFHistDataExtr()
            trend_ext.set_interval_to_retrieve(365*5)
            trend_ext.enable_save_raw_file = 0
            trend_ext.set_multiple_stock_list(list(ss.modified_df['SYMBOL']))
            trend_ext.get_trend_data()
            trend_ext.process_dividend_hist_data()

            full_stock_data_df = pandas.merge(ss.modified_df, trend_ext.price_trend_data_by_stock, on = 'SYMBOL')
            full_stock_data_df = pandas.merge(full_stock_data_df, trend_ext.all_stock_consolidated_div_df, on = 'SYMBOL')
            
            full_stock_data_df.to_csv(r'c:\data\full_oct18.csv', index = False)

        if 'd' in partial_run:
            ## direct scraping for more data
            # the SI problem.. sometimes is there but sometimes is nto
            dd = YFinanceDirectScrape()
            dd.set_stock_sym_append_str('')
            dd.set_multiple_stock_list(list(full_stock_data_df['SYMBOL']))
            dd.obtain_multiple_stock_data()
            print dd.all_stock_df
            
            ## will have to merge the two data set
            full_stock_data_df = pandas.merge(full_stock_data_df, dd.all_stock_df, on = 'SYMBOL')

            ## store all the data
            full_stock_data_df.to_csv(final_store_filename, index = False)

        if 'e' in partial_run:
            """Further filtering"""
            ## further filtering.
            BasicFilter =  InfoBasicFilter(final_store_filename)
            BasicFilter.loop_criteria()

    if  choice ==2:
        """do filtering and ranking on the filtering --> rank by priority"""





