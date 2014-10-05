"""
    Consolidated stocks information gather.
    Conolidiated script for making decision.

    auto generate the temp file.

"""

import re, sys, os, time, datetime
import pandas

from yahoo_finance_data_extract import YFinanceDataExtr
from direct_yahoo_finance_scaping import YFinanceDirectScrape

if __name__ == '__main__':
    choice  = 1

    if choice == 1:

        ## parameters
        full_stock_data_df = object()

        data_ext = YFinanceDataExtr()
        data_ext.set_stock_sym_append_str('')
        
        ## running stock information -- select type to watch
        data_ext.set_stock_retrieval_type('all') #'all', watcher
        data_ext.load_stock_symbol_fr_file()
        
        ##comment below if running the full list.
        #data_ext.set_full_stocklist_to_retrieve(['S58.SI','S68.SI'])
        data_ext.get_cur_quotes_fr_list()
        #print data_ext.temp_full_data_df

        ## save to temp file for enable filtering
        data_ext.temp_full_data_df.to_csv(r'c:\data\temp\temp_stockdata.csv')

        ## filtering
        ss =  InfoBasicFilter(r'c:\data\temp\temp_stockdata.csv')
        ss.set_criteria_type('potential')
        ss.get_all_criteria_fr_file()
        ss.process_criteria()
        ss.modified_df.to_csv('c:\data\potential_low_PE.csv', index = False)

        sys.exit()

        ## direct scraping for more data
        ss = YFinanceDirectScrape()
        ss.set_multiple_stock_list(data_ext.full_stocklist_to_retrieve)
        ss.obtain_multiple_stock_data()
        print ss.all_stock_df
        
        ## will have to merge the two data set
        full_stock_data_df = pandas.merge(data_ext.temp_full_data_df, ss.all_stock_df, on = 'SYMBOL')

        ## store all the data
        full_stock_data_df.to_csv(r'c:\data\full_oct02.csv', index = False)


    if  choice ==2:
        """do filtering and ranking on the filtering --> rank by priority"""
