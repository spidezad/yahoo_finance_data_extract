"""
    YQL for company informaton. Similar to the website display but return as json for easiler scraping

    Requires:
        Pandas
        Pattern
        Simplejson


    learning:
        Json readthedocs
        http://simplejson.readthedocs.org/en/latest/

        easier to construct a dataframe with a list of dict.


    Company data for keppel (format in json)
    https://query.yahooapis.com/v1/public/yql?q=SELECT%20*%20FROM%20yahoo.finance.keystats%20WHERE%20symbol%3D%27BN4.SI%27&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=

    # can handle mulitiple data by using the SQL commmand in the YQL console
    SELECT * FROM yahoo.finance.keystats WHERE symbol in ("BN4.SI","BS6.SI")
    --> (%22BN4.SI%22%2C%22BS6.SI%22) --> 

    https://query.yahooapis.com/v1/public/yql?q=SELECT%20*%20FROM%20yahoo.finance.keystats%20WHERE%20symbol%20in%20(%22BN4.SI%22%2C%22BS6.SI%22)&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=

    TODO:
        Retrieved the mutliple stocks from the yahoo finance data extract
        Will still need to form mutiple times
        can add in sector informaton all that
        Remove % from the columns
        If use yahoo finance, a lot of data are missing.
        knowing the historic PE and historic price to book??

        Add in the company info tables (rank the company tables)
        Use YQL --> yahoo.finance.stocks

        Each finance script must be able to attach to main table bound by Symbol

        Let the formation of url str be universal and just change ouptut
        May be easily simpilied to get all rest of dta.


"""

import os, sys, re, datetime
import pandas
from pattern.web import URL, extension, cache
import simplejson as json
from yahoo_finance_data_extract import YFinanceDataExtr


class YComDataExtr(YFinanceDataExtr):
    """ Inherited class from YFinanceDataExtr.
        Using YQL to get company data.
        Get the corresponding url based on the YQL generated SQL.
        Retrieval in the form of json format. 
    """
    def __init__(self):
        super(YComDataExtr, self).__init__()
        
        """ List of url parameters """

        # URL forming for YQL details --> make this to common form for all typs
        self.com_data_start_url = 'https://query.yahooapis.com/v1/public/yql?q=SELECT%20*%20FROM%20yahoo.finance.keystats%20WHERE%20symbol%20in%20('
        self.com_data_stock_portion_url = '.SI' #stock must be in "stock1","stock2"
        self.com_data_stock_portion_additional_url = ''# for adding additonal str to the stock url.
        self.com_data_end_url = ')&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback='
        self.com_data_full_url = ''

        ## printing options
        self.__print_url = 0

        ## Save json file
        self.saved_json_file = r'c:\data\temptryyql.json'

        ## Results storage
        self.com_data_allstock_list = list() # list of dict where each dict is company info for each stock
        self.com_data_allstock_df = object()


    def set_stock_sym_append_str(self, append_str):
        """ Set additional append str to stock symbol when forming stock url.
            Set to sel.cur_quotes_stock_portion_additional_url.
            Mainly to set the '.SI' for singapore stocks.
            Args:
                append_str (str): additional str to append to stock symbol.
        
        """
        self.com_data_stock_portion_additional_url = append_str
        
    def form_url_str(self):
        """ Form the url str necessary to get the .csv file
            May need to segregate into the various types.
            Args:
                type (str): Retrieval type.
        """
        self.form_com_data_stock_url_str()
            
        self.com_data_full_url = self.com_data_start_url + self.com_data_stock_portion_url +\
                                   self.com_data_end_url

    def form_com_data_stock_url_str(self):
        """ Form the list of stock portion for the cur quotes url.
        """
        self.com_data_stock_portion_url = ''
        for n in self.target_stocks:
            self.com_data_stock_portion_url = self.com_data_stock_portion_url +'%22' + n +\
                                                self.com_data_stock_portion_additional_url  + '%22%2C'
            
        self.com_data_stock_portion_url = self.com_data_stock_portion_url[:-3]

    def get_com_data(self):
        """ Combine the cur quotes function.
            Formed the url, download the csv, put in the header. Have a dataframe object.
        """
        self.form_url_str()
        if self.__print_url: print self.com_data_full_url
        self.download_json()
        self.get_datalist_fr_json()

    def get_com_data_fr_all_stocks(self):
        """ Cater for situation where there is large list.
            For safeguard, clip limit to 49.
        """
        full_list = self.replace_special_characters_in_list(self.full_stocklist_to_retrieve)
        chunk_of_list = self.break_list_to_sub_list(self.full_stocklist_to_retrieve)
        
        self.temp_full_data_df = None
        for n in chunk_of_list:
            # print the progress
            sys.stdout.write('.')

            # set the small chunk of list
            self.set_target_stocks_list(n)
            self.get_com_data()

        # convert to dataframe
        self.com_data_allstock_df = pandas.DataFrame(self.com_data_allstock_list)
        self.com_data_allstock_df.rename(columns ={'symbol':'SYMBOL'}, inplace=True)
        
        print 'Done\n'

    def download_json(self):
        """ Download the json file from the self.com_data_full_url.
            The save file is defaulted to the self.saved_json_file.

        """
        cache.clear()
        url = URL(self.com_data_full_url)
        f = open(self.saved_json_file, 'wb') # save as test.gif
        f.write(url.download(timeout = 50)) #increse the time out time for this
        f.close()

    def get_datalist_fr_json(self):
        """
            Set to self.com_data_allstock_list.
            Will keep appending without any reset.
        """
        raw_data  = json.load(open(self.saved_json_file, 'r'))
        for indivdual_set in  raw_data['query']['results']['stats']:
            temp_dict_data = {}
            if type(indivdual_set) == str:
                #for single data
                continue # temp do not use
            for parameters in indivdual_set.keys():
                if type(indivdual_set[parameters]) == str:
                    temp_dict_data[parameters] = indivdual_set[parameters]#for symbol
                elif type(indivdual_set[parameters]) == dict:
                    if indivdual_set[parameters].has_key('content'):
                        temp_dict_data[parameters] = indivdual_set[parameters]['content']

            ## append to list
            self.com_data_allstock_list.append(temp_dict_data)

    def retrieve_company_data(self):
        """ Retrieve the list of company data. """
        self.set_stock_sym_append_str('')
        self.set_stock_retrieval_type('all') #'all', watcher
        self.load_stock_symbol_fr_file()


if __name__ == '__main__':
    
    print "start processing"
    
    choice = 2

    if choice == 1:
        """try the download format of  YQL"""
        url_address = 'https://query.yahooapis.com/v1/public/yql?q=SELECT%20*%20FROM%20yahoo.finance.keystats%20WHERE%20symbol%3D%27BN4.SI%27&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback='
        savefile = r'c:\data\temptryyql.json'

        cache.clear()
        url = URL(url_address)
        f = open(savefile, 'wb') # save as test.gif
        f.write(url.download())
        f.close()

    if choice == 2:
        """ Handling Json file
            how to include the multiple keys per --> use  w['query']['results']['stats'].keys()

        """
       
        savefile = r'c:\data\temptryyql.json'
        w  = json.load(open(r'c:\data\temptryyql.json', 'r'))
        com_data_stock_list = list()
        for indivdual_set in  w['query']['results']['stats']:
            temp_dict_data = {}
            if type(indivdual_set) == str:
                #for single data
                continue # temp do not use
            for parameters in indivdual_set.keys():
                if type(indivdual_set[parameters]) == str:
                    temp_dict_data[parameters] = indivdual_set[parameters]#for symbol
                elif type(indivdual_set[parameters]) == dict:
                    if indivdual_set[parameters].has_key('content'):
                        temp_dict_data[parameters] = indivdual_set[parameters]['content']

            ## append to list
            com_data_stock_list.append(temp_dict_data)

    if choice ==3:
        """ test the class """
        file = r'c:\data\temp\temp_stockdata.csv'
        full_stock_data_df = pandas.read_csv(file)

        w = YComDataExtr()
        w.set_full_stocklist_to_retrieve(list(full_stock_data_df['SYMBOL']))
##        w.retrieve_company_data()
##        chunk_of_list = w.break_list_to_sub_list(w.full_stocklist_to_retrieve)
##        w.target_stocks  = chunk_of_list[0]
        w.get_com_data()
        w.get_com_data_fr_all_stocks()


    if choice ==4:
        file = r'c:\data\temp\temp_stockdata.csv'
        full_stock_data_df = pandas.read_csv(file)
        
        
                