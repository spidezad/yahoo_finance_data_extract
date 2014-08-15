"""
    Module: Yahoo finance data extractor
    Name:   Tan Kok Hua
    
    YF API from:
    https://code.google.com/p/yahoo-finance-managed/wiki/CSVAPI

    TODO:
        get the list of data from table extract???
        use table extract fr excel
        get all stock symbol

    Learning:
        replace all names
        dataset.rename(columns={typo: 'Address' for typo in AddressCol}, inplace=True)
        
"""

import os, re, sys, time, datetime
import pandas
import pattern
from pattern.web import URL, extension
 
class YFinanceDataExtr(object):
    """ Class to extract data from yahoo finance.
        Achieved by query the various url and downloading the respectively .csv files.
        Further analysis of data done by pandas.
    """
    def __init__(self):
        """ List of url parameters """
        self.target_stocks = ['S58.SI','S68.SI'] ##special character need to be converted
        ## object for extractor --future construct.


        ## current data .csv file url formation
        #header to match the sequence of the formed url
        self.cur_quotes_parm_headers = ['NAME', 'SYMBOL', 'LATEST_PRICE', 'OPEN', 'CLOSE','VOL',
                                             'YEAR_HIGH','YEAR_LOW'] #label to be use when downloading.
                                            
        # URL forming 
        self.cur_quotes_start_url = "http://download.finance.yahoo.com/d/quotes.csv?s="
        self.cur_quotes_stock_portion_url = ''
        self.cur_quotes_property_portion_url = ''
        self.cur_quotes_end_url = "&e=.csv"
        self.cur_quotes_full_url = ''

        # Output storage
        self.cur_quotes_csvfile = r'c:\data\temp\stock_data.csv'
        self.cur_quotes_df = object()
        
    def form_cur_quotes_stock_url_str(self):
        """ Form the list of stock portion for the cur quotes url.
        """
        self.cur_quotes_stock_portion_url = ''
        for n in self.target_stocks:
            self.cur_quotes_stock_portion_url = self.cur_quotes_stock_portion_url + n + ','
            
        self.cur_quotes_stock_portion_url =self.cur_quotes_stock_portion_url[:-1]

    def form_cur_quotes_property_url_str(self):
        """ To form the properties/parameters of the data to be received for current quotes
            To eventually utilize the get_table_fr_xls.
            Current use default parameters.
            name(n0), symbol(s), the latest value(l1), open(o) and the close value of the last trading day(p)
            volumn (v), year high (k), year low(j)

            Further info can be found at : https://code.google.com/p/yahoo-finance-managed/wiki/enumQuoteProperty
        """
        start_str = '&f='
        target_properties = 'nsl1opvkj'
        self.cur_quotes_property_portion_url =  start_str + target_properties

    def form_url_str(self, type = 'cur_quotes'):
        """ Form the url str necessary to get the .csv file.close
            May need to segregate into the various types.

            Args:
                type (str): Retrieval type.

        """
        if type == 'cur_quotes':
            self.form_cur_quotes_stock_url_str()
            self.form_cur_quotes_property_url_str()
            self.cur_quotes_full_url = self.cur_quotes_start_url + self.cur_quotes_stock_portion_url +\
                                       self.cur_quotes_property_portion_url + self.cur_quotes_end_url
             
    def downloading_csv(self, url_address):
        """ Download the csv information from the url_address given.

        """
        url = URL(url_address)
        f = open(self.cur_quotes_csvfile, 'wb') # save as test.gif
        f.write(url.download())
        f.close()

    def cur_quotes_create_dataframe(self):
        self.cur_quotes_df = pandas.read_csv(self.cur_quotes_csvfile,header =None)
        self.cur_quotes_df.rename(columns={org: change for org, change\
                                           in zip(self.cur_quotes_df.columns,self.cur_quotes_parm_headers)},\
                                              inplace=True)

    def get_cur_quotes(self):
        """ Combine the cur quotes function.
            Formed the url, download the csv, put in the header. Have a dataframe object.
        """
        self.form_url_str()
        self.downloading_csv(self.cur_quotes_full_url)
        self.cur_quotes_create_dataframe()
        

if __name__ == '__main__':
    print "start processing"
    data_ext = YFinanceDataExtr()

    ## Specify the stocks to be retrieved. Each url constuct max up to 50 stocks.
    data_ext.target_stocks = ['S58.SI','S68.SI'] #special character need to be converted

    ## Get the url str
    data_ext.form_url_str()
    print data_ext.cur_quotes_full_url
    ## >>> http://download.finance.yahoo.com/d/quotes.csv?s=S58.SI,S68.SI&f=nsl1opvkj&e=.csv

    ## Go to url and download the csv.
    ## Stored the data as pandas.Dataframe.
    data_ext.get_cur_quotes()
    print data_ext.cur_quotes_df
    ## >>>   NAME  SYMBOL  LATEST_PRICE  OPEN  CLOSE      VOL  YEAR_HIGH  YEAR_LOW
    ## >>> 0  SATS  S58.SI          2.99  3.00   3.00  1815000       3.53      2.93
    ## >>> 1   SGX  S68.SI          7.18  7.19   7.18  1397000       7.63      6.66




