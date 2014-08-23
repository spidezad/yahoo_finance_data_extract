"""
    Module: Yahoo finance data extractor
    Name:   Tan Kok Hua
    
    YF API from:
    https://code.google.com/p/yahoo-finance-managed/wiki/CSVAPI

    Updates:
        Aug 22 2014: Add in excel to choose propertries from.
                   : Take care of situation where the particular extraction yield 0 results.
        Aug 19 2014: Add in capability to scape all the data set(>50)
        Aug 18 2014: Add in functions for multiple chunks procssing.

    TODO:
        get the list of data from table extract???
        use table extract fr excel
        filter those zero volumes out and erratic data out.
        Investigate why yield zero results.
        May need to store the url

    Learning:
        replace all names
        dataset.rename(columns={typo: 'Address' for typo in AddressCol}, inplace=True)

        list of stocks symbol
        http://investexcel.net/all-yahoo-finance-stock-tickers/

        Url from yahoo finance --> sort by alphabet and by page
        https://sg.finance.yahoo.com/lookup/stocks?s=a&t=S&m=SG&r=
        https://sg.finance.yahoo.com/lookup/stocks?s=a&t=S&m=SG&r=&b=20
        https://sg.finance.yahoo.com/lookup/stocks?s=b&t=S&m=SG&r=

        Splitting list to even chunks
        http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks-in-python

        
"""

import os, re, sys, time, datetime, copy
import pandas
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

        # Properties from excel
        self.enable_form_properties_fr_exceltable = 1
        self.properties_excel_table = r'C:\pythonuserfiles\yahoo_finance_data_extract\Individual_stock_query_property.xls'

        # Output storage
        self.cur_quotes_csvfile = r'c:\data\temp\stock_data.csv'
        self.cur_quotes_df = object()

        # for debug
        self.store_individual_set_df = []

    def set_target_stocks_list(self, list_of_stocks):
        """ Set the list of stocks to the self.target_stocks.
            Args:
                list_of_stocks (list): target list of stocks to set
        """
        self.target_stocks = list_of_stocks
        
    def form_cur_quotes_stock_url_str(self):
        """ Form the list of stock portion for the cur quotes url.
        """
        self.cur_quotes_stock_portion_url = ''
        for n in self.target_stocks:
            self.cur_quotes_stock_portion_url = self.cur_quotes_stock_portion_url + n + ','
            
        self.cur_quotes_stock_portion_url =self.cur_quotes_stock_portion_url[:-1]

    def form_cur_quotes_property_url_str_fr_excel(self):
        """ Required xls_table_extract_module.
            Get all the properties from excel table.
            Properties can be selected by comment out those properties not required.
            Also set the heeader: self.cur_quotes_parm_headers for the values.

        """
        from xls_table_extract_module import XlsExtractor
        self.xls_property_data = XlsExtractor(fname = self.properties_excel_table, sheetname= 'Sheet1',
                                             param_start_key = 'stock_property//', param_end_key = 'stock_property_end//',
                                             header_key = '', col_len = 2)

        self.xls_property_data.open_excel_and_process_block_data()

        ## form the header
        self.cur_quotes_parm_headers = [n.encode() for n in self.xls_property_data.data_label_list]

        ## form the url str
        start_str = '&f='
        target_properties = ''.join([n[0].encode().strip() for n in self.xls_property_data.data_value_list])
        self.cur_quotes_property_portion_url =  start_str + target_properties

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
            
            # form the property. 2 methods enabled.
            if self.enable_form_properties_fr_exceltable:
                self.form_cur_quotes_property_url_str_fr_excel()
            else:
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
        """ Create dataframe for the results.
            Achieved by reading the .csv file and retrieving the results using pandas.
        """
        self.cur_quotes_df = pandas.read_csv(self.cur_quotes_csvfile,header =None)
        self.cur_quotes_df.rename(columns={org: change for org, change\
                                           in zip(self.cur_quotes_df.columns,self.cur_quotes_parm_headers)},\
                                              inplace=True)

    def get_cur_quotes(self):
        """ Combine the cur quotes function.
            Formed the url, download the csv, put in the header. Have a dataframe object.
        """
        self.form_url_str()
        print self.cur_quotes_full_url
        self.downloading_csv(self.cur_quotes_full_url)
        self.cur_quotes_create_dataframe()

    def get_cur_quotes_fr_list(self, full_list):
        """ Cater for situation where there is large list.
            Limit for the url is 50. Take care where list exceed 50.
            For safeguard, clip limit to 49.
        """
        ## full list with special characters take care
        full_list = self.replace_special_characters_in_list(full_list)
        chunk_of_list = self.break_list_to_sub_list(full_list)
        self.temp_full_data_df = None
        for n in chunk_of_list:
            # set the small chunk of list
            self.set_target_stocks_list(n)
            self.get_cur_quotes()
    
            ## need take care of cases where the return is empty -- will return Missing symbols list
            if not len(self.cur_quotes_df.columns) < len(self.cur_quotes_parm_headers):
                self.store_individual_set_df.append(self.cur_quotes_df)
                if self.temp_full_data_df is None:
                    self.temp_full_data_df =  self.cur_quotes_df
                else:
                    self.temp_full_data_df = self.temp_full_data_df.append(self.cur_quotes_df)


    def break_list_to_sub_list(self, full_list, chunk_size = 45):
        """ Break list into smaller equal chunks specified by chunk_size.
            Args:
                full_list (list): full list of items.
            Kwargs:
                chunk_size (int): length of each chunk. Max up to 50.
            Return
                (list): list of list.

        """
        if chunk_size < 1:
            chunk_size = 1
        return [full_list[i:i + chunk_size] for i in range(0, len(full_list), chunk_size)]

    def replace_special_characters_in_list(self, full_list):
        """ Replace any special characters in symbol that might affect url pulling.
            At present only replace the ":".
            See the following website for all the special characters
            http://www.blooberry.com/indexdot/html/topics/urlencoding.htm
            Args:
                full_list (list): list of symbol
            Returns:
                (list): modified list with special characters replaced.

        """
        return [n.replace(':','%3A') for n in full_list]
        

if __name__ == '__main__':
    
    print "start processing"
    

    choice = 1

    if choice == 1:
        data_ext = YFinanceDataExtr()
        ## read  data from .csv file -- full list of stocks
        csv_fname = r'C:\pythonuserfiles\yahoo_finance_data_extract\stocklist.csv'
        stock_list = pandas.read_csv(csv_fname)
        # convert from pandas object to list
        stock_list = list(stock_list['SYMBOL'])
        #stock_list = ['S58.SI','S68.SI']
        data_ext.get_cur_quotes_fr_list(stock_list)
        data_ext.temp_full_data_df.to_csv(r'c:\data\full.csv', index = False)

    if choice == 2:
        data_ext.form_url_str()
        
    if choice == 3:
        counter = 0
        for n in data_ext.store_individual_set_df:
            print counter
            counter = counter +1
            print n[n.columns[:4]].head()
            print '---'

##    ## Specify the stocks to be retrieved. Each url constuct max up to 50 stocks.
##    data_ext.target_stocks = ['S58.SI','S68.SI'] #special character need to be converted
##
##    ## Get the url str
##    data_ext.form_url_str()
##    print data_ext.cur_quotes_full_url
##    ## >>> http://download.finance.yahoo.com/d/quotes.csv?s=S58.SI,S68.SI&f=nsl1opvkj&e=.csv
##
##    ## Go to url and download the csv.
##    ## Stored the data as pandas.Dataframe.
##    data_ext.get_cur_quotes()
##    print data_ext.cur_quotes_df
##    ## >>>   NAME  SYMBOL  LATEST_PRICE  OPEN  CLOSE      VOL  YEAR_HIGH  YEAR_LOW
##    ## >>> 0  SATS  S58.SI          2.99  3.00   3.00  1815000       3.53      2.93
##    ## >>> 1   SGX  S68.SI          7.18  7.19   7.18  1397000       7.63      6.66




