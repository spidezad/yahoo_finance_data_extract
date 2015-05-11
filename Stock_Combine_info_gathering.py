"""
    Consolidated stocks information gather.
    Conolidiated script for making decision.

    Updates:
        Mar 14 2015: Auto set the filename with current date
        Feb 18 2015: Fast run with data get from database and allow storage of com data.

    TODO:

        Need to include PE for industry
        join the sector avegate to the industry

        add in stock analysis.
        add in the morning star data.

        May set it to run every day.


        need create folder
        need to reorder the list

    Learning:
        http://stackoverflow.com/questions/12329853/how-to-rearrange-pandas-column-sequence

    Bugs:
        company data is not working --> YQL might be broken

"""

import re, sys, os, time, datetime
import pandas

from Basic_data_filter import InfoBasicFilter
from Yahoo_finance_YQL_company_data import YComDataExtr
from yahoo_finance_data_extract import YFinanceDataExtr
from yahoo_finance_historical_data_extract import YFHistDataExtr
from hist_data_storage import FinanceDataStore
from Stock_tech_analysis import TechAnalysisAdd
from SGX_stock_announcement_extract import SGXDataExtract 

def set_last_desired_date( num_days = 0):
    """ Return the last date in which the results will be displayed.
        It is set to be the current date - num of days as set by users.
        Affect only self.print_feeds function.
        Kwargs:
            num_days (int): num of days prior to the current date.
            Setting to 0 will only retrieve the current date
        Returns:
            (int): datekey as yyyyymmdd.
    """
    last_eff_date_list = list((datetime.date.today() - datetime.timedelta(num_days)).timetuple()[0:3])

    if len(str(last_eff_date_list[1])) == 1:
        last_eff_date_list[1] = '0' + str(last_eff_date_list[1])

    if len(str(last_eff_date_list[2])) == 1:
        last_eff_date_list[2] = '0' + str(last_eff_date_list[2])

    return str(last_eff_date_list[0]) + str(last_eff_date_list[1]) + str(last_eff_date_list[2])

def get_filename(dir_path, filename_prefix, offset_to_cur_date = 0, file_ext = '.csv'):
    """ Generate the filename based on current date.
        Args:
            dir_path (str): full dir path
            filename_prefix (str): filename prefix before the date
            offset_to_cur_date (int): num of days offset to current
        Kwargs:
            file_ext (str): extension of the file.
        Return:
            (str): file path
    """
    return os.path.join(dir_path, filename_prefix + set_last_desired_date(offset_to_cur_date)+ file_ext )

if __name__ == '__main__':

    ## list of parameters
    db_full_path = r'C:\data\stock_sql_db\stock_hist.db'

    for n in range(2): print
    print time.ctime()


    choice  = 1
    partial_run = ['a2','b','c_pre','c','d_pre','e','f', 'g']#e is storing data
    #partial_run = ['a','b','c_pre','c','d','e','f', 'g']#e is storing data
    #partial_run = ['a2']


    if choice == 1:

        ## parameters
        final_store_filename = get_filename(r'c:\data\compile_stockdata', 'full_')  
        full_stock_data_df = object()

        if 'a2' in partial_run:
            print "Getting Data from the SGX instead of YUI."
            print "-------------------------------------"
            data_ext = SGXDataExtract()
            data_ext.process_all_data()
            temp_full_data_df = data_ext.sgx_curr_plus_company_df
            temp_full_data_df["SYMBOL"]  = temp_full_data_df["SYMBOL"] + '.SI'
            
            ## save to temp file for enable filtering
            temp_full_data_df.to_csv(r'c:\data\temp\temp_stockdata.csv')
            print "Getting Main dataset from YUI -- Done"
            print 


        ## Initial stage, getting most raw data.
        if 'a' in partial_run:
            print "Getting Main dataset from YUI."
            print "-------------------------------------"
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
            print "Getting Main dataset from YUI -- Done"
            print 

        ## basic filtering to remove those irrelvant stocks.
        ## the criteria set is very loose based on below.
        if 'b' in partial_run:
            print "Basic Filtering of stock data."
            ss =  InfoBasicFilter(r'c:\data\temp\temp_stockdata.csv')
            ss.set_criteria_type('basic')
            ss.get_all_criteria_fr_file()
            ss.process_criteria()
            print "stocks left after basic filtering: ", len(ss.modified_df)
            ss.modified_df.to_csv(final_store_filename, index = False)
            print "Basic Filtering of stock data -- Done. \n"

        if 'c_backup' in partial_run:
            ## testing, to remove after this
            #ss.modified_df = ss.modified_df.head()

            ## 3 days trends data
            print "Getting Trends data"
            trend_ext = YFHistDataExtr()
            trend_ext.set_interval_to_retrieve(365*5)
            trend_ext.enable_save_raw_file = 0
            trend_ext.set_multiple_stock_list(list(ss.modified_df['SYMBOL']))
            trend_ext.run_all_hist_data()
            full_stock_data_df = pandas.merge(ss.modified_df, trend_ext.all_stock_combined_post_data_df, on = 'SYMBOL', how ='left')
            
            print "Getting Trends data -- Done. \n"
            #full_stock_data_df.to_csv(r'c:\data\full_oct21.csv', index = False)

        if 'c_pre' in partial_run:
            """ Update the database """
            print 'Updating the database with latest hist price.'
            datastore = FinanceDataStore(db_full_path)
            stock_list = list(ss.modified_df['SYMBOL'])
            datastore.scan_and_input_recent_prices(stock_list,5)
            print 'Updating the database with latest hist price -- Done\n'

        if 'c' in partial_run:
            ## using database to retrieve the data
            datastore = FinanceDataStore(db_full_path)
            datastore.retrieve_hist_data_fr_db()

            ## 3 days trends data
            print "Getting Trends data"
            trend_ext = YFHistDataExtr()
            trend_ext.set_bypass_data_download()
            trend_ext.set_raw_dataset(datastore.hist_price_df, datastore.hist_div_df)
            trend_ext.run_all_hist_data()
            full_stock_data_df = pandas.merge(ss.modified_df, trend_ext.all_stock_combined_post_data_df, on = 'SYMBOL', how ='left')
            
            print "Getting Trends data -- Done. \n"
            full_stock_data_df.to_csv(final_store_filename, index = False)

        if 'd_pre' in partial_run:
            """ Skip the company data info and just join based on store data."""
            print 'Use backup data for company data.'
            store_com_path = r'C:\data\stock_sql_db\company_data.csv'
            com_data_df = pandas.read_csv(store_com_path)
            full_stock_data_df = pandas.merge(full_stock_data_df, com_data_df, on = 'SYMBOL', how ='left')

        if 'd' in partial_run:
            ## Replaced with the com data scraping using YQL --> some problem for this
            print "Getting company data from YF using YQL"
            print
            print 'len of stock dataframe', len(full_stock_data_df)
            dd = YComDataExtr()
            dd.set_stock_sym_append_str('')
            dd.set_full_stocklist_to_retrieve(list(full_stock_data_df['SYMBOL']))
            dd.retrieve_all_results()
           
            ## will have to merge the two data set
            full_stock_data_df = pandas.merge(full_stock_data_df, dd.com_data_allstock_df, on = 'SYMBOL', how ='left')

            ## save the company data so next time only need to read back and append
            store_com_path = r'C:\data\stock_sql_db\company_data.csv'
            dd.com_data_allstock_df.to_csv(store_com_path,index = False)

        if 'e' in partial_run:
            ## tech analysis
            print 'Tech analysis '
            sym_list = list(full_stock_data_df['SYMBOL'])
            w = TechAnalysisAdd(sym_list)
            w.enable_pull_fr_database()
            w.retrieve_hist_data()
            w.add_analysis_parm()
            w.get_most_current_dataset()
            w.add_response_trigger()

            ## will have to merge the two data set
            full_stock_data_df = pandas.merge(full_stock_data_df, w.processed_histdata_combined, on = 'SYMBOL', how ='left')

        if 'f' in partial_run:
            
            # reorder the data before storing
            def set_column_sequence(dataframe, seq):
                """ Takes a dataframe and a subsequence of its columns, returns dataframe with seq as first columns
                    From stackoverfrow:
                    http://stackoverflow.com/questions/12329853/how-to-rearrange-pandas-column-sequence
                """
                cols = seq[:] # copy so we don't mutate seq
                for x in dataframe.columns:
                    if x not in cols:
                        cols.append(x)
                return dataframe[cols]

            full_stock_data_df = set_column_sequence(full_stock_data_df, ['CompanyName', 'SYMBOL', 'OPEN', 'DailyVolume', 'PERATIO',\
                                                                          'PRICEBOOK', 'TotalDebtEquity', 'eps',\
                                                                          'TRAILINGANNUALDIVIDENDYIELDINPERCENT', 'NumDividendperYear',\
                                                                          'NumYearPayin4Yr','industry', 'industryGroup', 'marketCap',\
                                                                          'basicEpsIncl', 'beta5Yr'])

            ## store all the data
            full_stock_data_df.to_csv(final_store_filename, index = False)
            print "Getting additional data from YF -- Done"

        if 'g' in partial_run:
            """Further filtering"""
            print "Final filtering of data according to criteria"
            print
            ## further filtering.
            BasicFilter =  InfoBasicFilter(final_store_filename)
            BasicFilter.loop_criteria()
            print "Final filtering of data according to criteria -- Done."

    if  choice ==2:
        """do filtering and ranking on the filtering --> rank by priority"""

    print time.ctime()
    raw_input()


