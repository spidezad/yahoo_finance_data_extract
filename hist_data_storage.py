"""
    Input Historical prices to SQLite Database.
    For fast retrieval of historical data.
        
    Module required.
    
    Take note for the stocks that are not found, may have to run all the data
    Get the last date entry of the target stock fromt the db
    Index the db according to stockname??

    after which can use the yahoo YQL to get the stocks history which is much faster.

    may need the time in for the test

    Updates:
        Mar 11 2015: Resolve bug in repeated entries in setup_db_for_hist_prices_storage by re-initialize variables.
        Mar 04 2015: Add in function to get stock list from database
        Feb 22 2015: Add limit to the date retrieval --> affect self.hist_price_df


    TODO:
        set the dataset retrieval in df form
        may need to index the sqlite with the symbol
        should remove the index when storing...
        put in company information --> YSQL
        add in new entry

        should change the replace to append??

        function to get particular symbol.

        would still need to update the dividend data.

        retrieve only up to certain date ()

        Convert date column to date time objc??

        make use of the select all function

        or make use of datekey generator when extract as a dataframe then use date as comparsion

        need to get only those stocks that is not input

"""


import re, sys, os, time, datetime, csv
import pandas
import sqlite3 as lite
from yahoo_finance_historical_data_extract import YFHistDataExtr
from Yahoo_finance_YQL_company_data import YComDataExtr #use for fast retrieval of data.

class FinanceDataStore(object):
    """ For storing and retrieving stocks data from database.
 
    """
    def __init__(self, db_full_path):
        """ Set the link to the database that store the information.
            Args:
                db_full_path (str): full path of the database that store all the stocks information.

        """
        self.con = lite.connect(db_full_path)
        self.cur = self.con.cursor()
        self.hist_data_tablename = 'histprice' #differnt table store in database
        self.divdnt_data_tablename = 'dividend'

        ## set the date limit of extracting.(for hist price data only)
        self.set_data_limit_datekey = '' #set the datekey so 

        ## output data
        self.hist_price_df = pandas.DataFrame()
        self.hist_div_df = pandas.DataFrame()

    def close_db(self):
        """ For closing the database. Apply to self.con
        """
        self.con.close()
        
    def break_list_to_sub_list(self,full_list, chunk_size = 45):
        """ Break list into smaller equal chunks specified by chunk_size.
            Args:
                full_list (list): full list of items.
            Kwargs:
                chunk_size (int): length of each chunk.
            Return
                (list): list of list.
        """
        if chunk_size < 1:
            chunk_size = 1
        return [full_list[i:i + chunk_size] for i in range(0, len(full_list), chunk_size)]

    def setup_db_for_hist_prices_storage(self, stock_sym_list):
        """ Get the price and dividend history and store them to the database for the specified stock sym list.
            The length of time depends on the date_interval specified.
            Connection to database is assuemd to be set.
            For one time large dataset (where the hist data is very large)
            Args:
                stock_sym_list (list): list of stock symbol.

        """

        ## set the class for extraction
        histdata_extr = YFHistDataExtr()
        histdata_extr.set_interval_to_retrieve(360*5)# assume for 5 years information
        histdata_extr.enable_save_raw_file = 0

        for sub_list in self.break_list_to_sub_list(stock_sym_list):

            ## re -initalize the df
            histdata_extr.all_stock_df = pandas.DataFrame()
            histdata_extr.processed_data_df = pandas.DataFrame()
            histdata_extr.all_stock_div_hist_df = pandas.DataFrame()

            print 'processing sub list', sub_list
            histdata_extr.set_multiple_stock_list(sub_list)
            histdata_extr.get_hist_data_of_all_target_stocks()
            histdata_extr.removed_zero_vol_fr_dataset()

            ## save to one particular funciton 
            #save to sql -- hist table
            histdata_extr.processed_data_df.to_sql(self.hist_data_tablename, self.con, flavor='sqlite',
                                    schema=None, if_exists='append', index=True,
                                    index_label=None, chunksize=None, dtype=None)

            #save to sql -- div table
            histdata_extr.all_stock_div_hist_df.to_sql(self.divdnt_data_tablename, self.con, flavor='sqlite',
                                    schema=None, if_exists='append', index=True,
                                    index_label=None, chunksize=None, dtype=None)

        self.close_db()

    def scan_and_input_recent_prices(self, stock_sym_list, num_days_for_updates = 10 ):
        """ Another method to input the data to database. For shorter duration of the dates.
            Function for storing the recent prices and set it to the databse.
            Use with the YQL modules.
            Args:
                stock_sym_list (list): stock symbol list.
            Kwargs:
                num_days_for_updates: number of days to update. Cannot be set too large a date.
                                    Default 10 days.

        """
       
        w = YComDataExtr()
        w.set_full_stocklist_to_retrieve(stock_sym_list)
        w.set_hist_data_num_day_fr_current(num_days_for_updates)
        w.get_all_hist_data()

        ## save to one particular funciton 
        #save to sql -- hist table
        w.datatype_com_data_allstock_df.to_sql(self.hist_data_tablename, self.con, flavor='sqlite',
                                schema=None, if_exists='append', index=True,
                                index_label=None, chunksize=None, dtype=None)


    def retrieve_stocklist_fr_db(self):
        """ Retrieve the stocklist from db
            Returns:
                (list): list of stock symbols.
        """
        command_str = "SELECT DISTINCT SYMBOL FROM %s "% self.hist_data_tablename
        self.cur.execute(command_str)
        rows = self.cur.fetchall()
        return [n[0].encode() for n in rows]

    def retrieve_hist_data_fr_db(self, stock_list=[], select_all =1):
        """ Retrieved a list of stocks covering the target date range for the hist data compute.
            Need convert the list to list of str
            Will cover both dividend and hist stock price
            Kwargs:
                stock_list (list): list of stock symbol (with .SI for singapore stocks) to be inputted.
                                    Will not be used if select_all is true.
                select_all (bool): Default to turn on. Will pull all the stock symbol
          
        """
        stock_sym_str = ''.join(['"' + n +'",' for n in stock_list])
        stock_sym_str = stock_sym_str[:-1]
        #need to get the header
        command_str = "SELECT * FROM %s where symbol in (%s)"%(self.hist_data_tablename,stock_sym_str)
        if select_all: command_str = "SELECT * FROM %s "%self.hist_data_tablename
        self.cur.execute(command_str)
        headers =  [n[0] for n in self.cur.description]
        
        rows = self.cur.fetchall() # return list of tuples
        self.hist_price_df = pandas.DataFrame(rows, columns = headers) #need to get the header?? how to get full data from SQL

        ## dividend data extract
        command_str = "SELECT * FROM %s where symbol in (%s)"%(self.divdnt_data_tablename,stock_sym_str)
        if select_all: command_str = "SELECT * FROM %s "%self.divdnt_data_tablename
        
        self.cur.execute(command_str)
        headers =  [n[0] for n in self.cur.description]
        
        rows = self.cur.fetchall() # return list of tuples
        self.hist_div_df = pandas.DataFrame(rows, columns = headers) #need to get the header?? how to get full data from SQL

        self.close_db()

    def add_datekey_to_hist_price_df(self):
        """ Add datekey in the form of yyyymmdd for easy comparison.

        """
        self.hist_price_df['Datekey'] = self.hist_price_df['Date'].map(lambda x: int(x.replace('-','') ))

    def extr_hist_price_by_date(self, date_interval):
        """ Limit the hist_price_df by the date interval.
            Use the datekey as comparison.
            Set to the self.hist_price_df

        """
        self.add_datekey_to_hist_price_df()
        target_datekey = self.convert_date_to_datekey(date_interval)
        self.hist_price_df = self.hist_price_df[self.hist_price_df['Datekey']>=target_datekey]
        
    def convert_date_to_datekey(self, offset_to_current = 0):
        """ Function mainly for the hist data where it is required to specify a date range.
            Default return current date. (offset_to_current = 0)
            Kwargs:
                offset_to_current (int): in num of days. default to zero which mean get currnet date
            Returns:
                (int): yyymmdd format
        
        """
        last_eff_date_list = list((datetime.date.today() - datetime.timedelta(offset_to_current)).timetuple()[0:3])

        if len(str(last_eff_date_list[1])) == 1:
            last_eff_date_list[1] = '0' + str(last_eff_date_list[1])

        if len(str(last_eff_date_list[2])) == 1:
            last_eff_date_list[2] = '0' + str(last_eff_date_list[2])
    
        return int(str(last_eff_date_list[0]) + str(last_eff_date_list[1]) + str(last_eff_date_list[2]))


if __name__ == '__main__':

    print "start processing"

    db_full_path = r'C:\data\stock_sql_db\stock_hist.db'

    selection  = 6

    if selection == 1:
        f = FinanceDataStore(db_full_path)

        ## stock symbol path
        file = r'c:\data\full_Mar03.csv'
        full_stock_data_df = pandas.read_csv(file)
        stock_list = list(full_stock_data_df['SYMBOL'])

        f.setup_db_for_hist_prices_storage(stock_list)

    if selection ==7:
        """ Retrieve stocklist fr database"""
        f = FinanceDataStore(db_full_path)
        present_stocklist = f.retrieve_stocklist_fr_db()

        ## stock symbol path -- to get list of stocks  to input, 
        file = r'c:\data\full_Mar03.csv'
        full_stock_data_df = pandas.read_csv(file)
        target_stock_list = list(full_stock_data_df['SYMBOL'])

        required_stock_list = [n for n in target_stock_list if n not in present_stocklist]

        f.setup_db_for_hist_prices_storage(required_stock_list)

    if selection == 5:
        """"""
        f = FinanceDataStore(db_full_path)

        ## stock symbol path
        file = r'C:\data\compile_stockdata\full_20150405.csv'
        full_stock_data_df = pandas.read_csv(file)
        stock_list = list(full_stock_data_df['SYMBOL'])

        #header a bit different 
        f.scan_and_input_recent_prices(stock_list,10)

    if selection == 6:
        f = FinanceDataStore(db_full_path)

        ## stock symbol path
        file = r'C:\data\compile_stockdata\full_20150423.csv'
        full_stock_data_df = pandas.read_csv(file)
        stock_list = list(full_stock_data_df['SYMBOL'])

        f.retrieve_hist_data_fr_db(stock_list,0)
        f.extr_hist_price_by_date(200)
        print f.hist_price_df
        print
        #print f.hist_div_df.head()


    if selection  ==2:
        """for query all the data for processing

            Make it into a list of query by usng in 
        """
        con = lite.connect(db_full_path)
        cur = con.cursor()

        stock_sym_str = '"RE2.SI"'# for the command input
        #need to get the header
        command_str = "SELECT * FROM HISTprice where symbol in (%s)"%stock_sym_str
        cur.execute(command_str)

        rows = cur.fetchall() # return list of tuples
        #do not have the header --> add in header and convert to pandas data

        con.close()


    if selection  == 3:
        """ Divident data might not need to collect so often"""
        """ Deal with YQL and extract recent data """
        """ Hist price retrieve using YQL. """

        file = r'c:\data\full_Dec29.csv'
        full_stock_data_df = pandas.read_csv(file)
        
        w = YComDataExtr()
        w.set_full_stocklist_to_retrieve(list(full_stock_data_df['SYMBOL'])[:100])
        w.get_all_hist_data()
        print w.datatype_com_data_allstock_df.head() # str away use the pandas to sql version to send??

        ## Users parameters
        hist_data_tablename = 'histprice'
        divdnt_data_tablename = 'dividend'   

        ## initialized the database
        con = lite.connect(db_full_path)

        ## save to one particular funciton 
        #save to sql -- hist table
        w.datatype_com_data_allstock_df.to_sql(hist_data_tablename, con, flavor='sqlite',
                                schema=None, if_exists='replace', index=True,
                                index_label=None, chunksize=None, dtype=None)


    if selection  ==4:
        """ convert stock list to str"""
        s = ['a','b','c']
        print ''.join(['"' + n +'",' for n in s])
        print ''.join([n +',' for n in s])

        

