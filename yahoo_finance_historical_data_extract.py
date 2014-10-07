"""
    Module: Yahoo finance historical data extractor
    Name:   Tan Kok Hua

    Notes:
        Each url get historical data of one stock.

    YF API from:
    https://code.google.com/p/yahoo-finance-managed/wiki/CSVAPI

    Updates:
        Oct 08 2014: Able to save temp file and individual data (option)
                   : Able to save all data dataframe for further processing.
        Sep 16 2014: Enable multiple stocks data extract

    TODO:
        Get today dates
        Do dates manipulation\
        Have the get divident
        how to convert the date time in pandas
        may need to take care of sat and sun
        pull the last 5 days to see if continuous droppingh
        get trend information 


    Learning:
        pandas get moving average
        http://www.bearrelroll.com/2013/07/python-pandas-moving-average/
        

    
"""

import os, re, sys, time, datetime, copy, shutil
import pandas
from pattern.web import URL, extension

 
class YFHistDataExtr(object):
    """ Class to extract data from yahoo finance.
        Achieved by query the various url and downloading the respectively .csv files.
        Further analysis of data done by pandas.
    """
    def __init__(self):
        """ List of url parameters """
        # Param
        ## self.target_stocks use mainly for a few stocks.
        ## it also use when setting the 45 or 50 stocks at a time to url
        self.target_stocks = ['S58.SI','S68.SI'] ##special character need to be converted
        self.individual_stock_sym = '' #full range fo stocks
        self.date_interval = 30 # the number of dates to retrieve, temp  default to 1 day per interval 
                                                
        # URL forming 
        self.hist_quotes_start_url = "http://ichart.yahoo.com/table.csv?s="
        self.hist_quotes_stock_portion_url = ''
        self.hist_quotes_date_interval_portion_url = ''
        self.hist_quotes_end_url = "&ignore=.csv"
        self.hist_quotes_full_url = ''

        # Output storage
        self.hist_quotes_df = object()
        self.enable_save_raw_file = 1 # 1 - will save all the indivdual raw data
        self.hist_quotes_csvfile_path = r'c:\data' # for storing of all stock raw data
        self.tempfile_sav_location = r'c:\data\temp\temp_hist_data_save.csv'
        self.all_stock_df = []

        # Trend processing.


    def set_stock_to_retrieve(self, stock_sym):
        """ Set the stock symbol required for retrieval.
            Args:
                stock_sym (str): Input the stock symbol.
        """
        assert type(stock_sym) == str
        self.individual_stock_sym = stock_sym

    def set_interval_to_retrieve(self, days):
        """ Set the interval (num of days) to retrieve.
            Args:
                days (int): Number of days from current date to retrieve.
        """
        self.date_interval = days

    def set_multiple_stock_list(self, stocklist):
        """ Set the multiple stock list. Set to self.all_stock_sym_list
            Args:
                stocklist (list): list of stocks symbol.
        """
        self.all_stock_sym_list = stocklist
        
    def form_stock_part_url(self):
        """ Formed the stock portion of the url for query.
            Require the self.individual_stock_sym not to be empty
        """
        assert self.individual_stock_sym is not None
        fixed_portion = ''#temp not used
        self.hist_quotes_stock_portion_url  = fixed_portion + self.individual_stock_sym

    def calculate_start_and_end_date(self):
        """ Return the start and end (default today) based on the interval range in tuple.
            Returns:
                start_date_tuple : tuple in yyyy mm dd of the past date
                end_date_tuple : tupe in yyyy mm dd of current date today
        """
        ## today date or end date
        end_date_tuple = datetime.date.today().timetuple()[0:3] ## yyyy, mm, dd
        start_date_tuple = (datetime.date.today() - datetime.timedelta(self.date_interval)).timetuple()[0:3]
        return start_date_tuple, end_date_tuple

    def form_hist_quotes_date_interval_portion_url(self):
        """ Form the date interval portion of the url
            Set to self.hist_quotes_date_interval_portion_url
            Note: add the number of the month minus 1.
        """
        start_date_tuple, end_date_tuple = self.calculate_start_and_end_date()
        
        from_date_url_str = '&c=%s&a=%s&b=%s' %(start_date_tuple[0],start_date_tuple[1]-1, start_date_tuple[2]) 
        end_date_url_str = '&f=%s&d=%s&e=%s' %(end_date_tuple[0],end_date_tuple[1]-1, end_date_tuple[2]) 
        interval_str = '&g=d'

        self.hist_quotes_date_interval_portion_url = from_date_url_str + end_date_url_str + interval_str

    def form_url_str(self, type = 'hist_quotes'):
        """ Form the url str necessary to get the .csv file.close
            May need to segregate into the various types.
            Args:
                type (str): Retrieval type.
        """

        self.form_stock_part_url()
        self.form_hist_quotes_date_interval_portion_url()
        
            
        self.hist_quotes_full_url = self.hist_quotes_start_url + self.hist_quotes_stock_portion_url +\
                                    self.hist_quotes_date_interval_portion_url \
                                    + self.hist_quotes_end_url
             
    def downloading_csv(self):
        """ Download the csv information for particular stock.
        """
        url = URL(self.hist_quotes_full_url)
        f = open(self.tempfile_sav_location, 'wb') # save as test.gif
        f.write(url.download())
        f.close()

        if self.enable_save_raw_file:
            sav_filename = os.path.join(self.hist_quotes_csvfile_path,'hist_stock_price_'+ self.individual_stock_sym+ '.csv')
            shutil.copyfile(self.tempfile_sav_location,sav_filename )

    def save_stockdata_to_df(self):
        """ Create dataframe for the results.
            Achieved by reading the .csv file and retrieving the results using pandas.
        """
        self.hist_quotes_individual_df = pandas.read_csv(self.tempfile_sav_location)
        self.hist_quotes_individual_df['SYMBOL'] = self.individual_stock_sym

        if len(self.all_stock_df) == 0:
            self.all_stock_df = self.hist_quotes_individual_df
        else:
            self.all_stock_df = self.all_stock_df.append(self.hist_quotes_individual_df)
        
    def get_hist_data_of_all_target_stocks(self):
        """ Combine the cur quotes function.
            Formed the url, download the csv, put in the header. Have a dataframe object.
        """
        for stock in self.all_stock_sym_list:
            print 'Processing stock: ', stock
            self.set_stock_to_retrieve(stock)
            self.form_url_str()
            print self.hist_quotes_full_url
            self.downloading_csv()
            self.save_stockdata_to_df()


if __name__ == '__main__':
    
    print "start processing"
    
    choice = 1

    if choice == 1:
        data_ext = YFHistDataExtr()
        data_ext.set_interval_to_retrieve(10)
        data_ext.set_multiple_stock_list(['OV8.SI','G13.SI'])
        #data_ext.set_stock_to_retrieve('OV8.SI')
        data_ext.get_hist_data_of_all_target_stocks()
        
    #calculating support and resistance lines
        #need get the moving average --pandas use the rolling mean





