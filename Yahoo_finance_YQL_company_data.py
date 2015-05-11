"""
    YQL for company informaton. Similar to the website display but return as json for easier scraping

    Requires:
        Pandas
        Pattern
        Simplejson

    learning:
        Json readthedocs
        http://simplejson.readthedocs.org/en/latest/

        easier to construct a dataframe with a list of dict.

    List of data able to retrieve:
        Key financial stats
        Company information
        hist price
        div
        
    Ex of format:
        Company data for keppel (format in json)
        https://query.yahooapis.com/v1/public/yql?q=SELECT%20*%20FROM%20yahoo.finance.keystats%20WHERE%20symbol%3D%27BN4.SI%27&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=

        # can handle mulitiple data by using the SQL commmand in the YQL console
        SELECT * FROM yahoo.finance.keystats WHERE symbol in ("BN4.SI","BS6.SI")
        --> (%22BN4.SI%22%2C%22BS6.SI%22) --> 

        https://query.yahooapis.com/v1/public/yql?q=SELECT%20*%20FROM%20yahoo.finance.keystats%20WHERE%20symbol%20in%20(%22BN4.SI%22%2C%22BS6.SI%22)&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=

        Hist data YQL
        https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.historicaldata%20where%20symbol%20in%20(%22BN4.SI%22%2C%22BS6.SI%22)%20and%20startDate%20%3D%20%222009-09-11%22%20and%20endDate%20%3D%20%222010-03-10%22&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=

    Updates:
        Apr 16 2015: Add in debug for cases with unicode. Affect function get_datalist_fr_json. Remove the super class
        Mar 12 2015: Add in getting dividend from keystats (dividend info in list which initially not able to obtain.
        Feb 18 2015: Add in hist data retrieval with the columns similar to that in database
        Feb 12 2015: Have the strip % function.

    TODO:
        Each finance script must be able to attach to main table bound by Symbol

        Let the formation of url str be universal and just change ouptut
        May be easily simpilied to get all rest of dta.

        Bug cannot handle single stock





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

        #super(YComDataExtr, self).__init__()


        """ Dict for different different data url"""
        ## the dict will contain the (start url, end url str, json results tag)
        ## may be able to further simplify due to similarity in the url
        self.datatype_url_dict = {
                                    'keystats': ('https://query.yahooapis.com/v1/public/yql?q=SELECT%20*%20FROM%20yahoo.finance.keystats%20WHERE%20symbol%20in%20(',
                                                 ')&format=json&diagnostics=false&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=',
                                                 'stats'
                                                 ),
                                    'CompanyInfo':('https://query.yahooapis.com/v1/public/yql?q=SELECT%20*%20FROM%20yahoo.finance.stocks%20WHERE%20symbol%20in%20(',
                                                   ')&format=json&diagnostics=false&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=',
                                                   'stock'
                                                    ),
                                    }
            

        """ List of url parameters -- for url formation """
        # URL forming for YQL details --> make this to common form for all typs
        self.com_data_start_url = ''
        self.com_data_stock_portion_url = '' #stock must be in "stock1","stock2"
        self.com_data_stock_portion_additional_url = ''# for adding additonal str to the stock url.
        self.com_data_end_url = ''
        self.com_data_full_url = ''
        self.json_result_tag = ''

        ## Below function for the hist data pulling using the YQL.
        ## This is not combined with the datatype_url_dict as the retrieval mode do not required grouping.
        self.hist_data_start_url = 'https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.historicaldata%20where%20symbol%20in%20('
        self.hist_data_num_day_fr_current = 5 # number of days to get prior to current date.
        self.hist_data_end_url = '' # to obtain after setting the date interval

        ## temp function
        self.datatype_to_pull = 'keystats' # for use in conjunction with the dict for pulling

        ## printing options
        self.__print_url = 0

        ## Save json file
        self.saved_json_file = r'c:\data\temptryyql.json'

        ## Temp Results storage
        self.datatype_com_data_allstock_list = list() # list of dict where each dict is company info for each stock
        self.datatype_com_data_allstock_df = object()
        
        ## full result storage
        self.com_data_allstock_df = object()

    def set_datatype_to_pull(self, datatype):
        """ Method to set the data type to pull.
            Set to self.datatype_to_pull.
            Args:
                datatype (str): different data type according to the datatype dict keys.
        """
        self.datatype_to_pull = datatype

    def load_start_end_url(self):
        """ Load the start and end url based on the datatype dict.
            Load to self.com_data_start_url and self.com_data_end_url
            In addition, will load the json result tag.
        """
        self.com_data_start_url = self.datatype_url_dict[self.datatype_to_pull][0]
        self.com_data_end_url = self.datatype_url_dict[self.datatype_to_pull][1]
        self.json_result_tag = self.datatype_url_dict[self.datatype_to_pull][2]

    def set_and_load_datatype_url(self, datatype):
        """ Set the data type and also load to the start and end url.
            Args:
                datatype (str): different data type according to the datatype dict keys.
        """
        self.set_datatype_to_pull(datatype)
        self.load_start_end_url()

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
        self.datatype_com_data_allstock_df = pandas.DataFrame(self.datatype_com_data_allstock_list)
        self.datatype_com_data_allstock_df.rename(columns ={'symbol':'SYMBOL'}, inplace=True)
        
        print 'Done\n'

    def download_json(self):
        """ Download the json file from the self.com_data_full_url.
            The save file is defaulted to the self.saved_json_file.
            Need take care of Exceptions

        """
        cache.clear()
        url = URL(self.com_data_full_url)
        f = open(self.saved_json_file, 'wb') # save as test.gif
        try:
            str = url.download(timeout = 50)
        except:
            str = ''
        f.write(str) #increse the time out time for this
        f.close()

    def get_datalist_fr_json(self):
        """
            Set to self.datatype_com_data_allstock_list.
            Will keep appending without any reset.
        """
        try:
            raw_data  = json.load(open(self.saved_json_file, 'r'))
        except:
            return
        if raw_data == '': return # for situation where there is no data
        for indivdual_set in  raw_data['query']['results'][self.json_result_tag]:
            temp_dict_data = {}
            if type(indivdual_set) == str:
                #for single data
                continue # temp do not use
            for parameters in indivdual_set.keys():
                if type(indivdual_set[parameters]) == str or type(indivdual_set[parameters]) == unicode:
                    try:
                        temp_dict_data[parameters] = indivdual_set[parameters]#for symbol
                    except:
                        print 'not working', parameters
                elif type(indivdual_set[parameters]) == dict:
                    if indivdual_set[parameters].has_key('content'):
                        temp_dict_data[parameters] = indivdual_set[parameters]['content']
                # special handling for getting trailing dividend, which is a list
                elif parameters == 'TrailingAnnualDividendYield':
                    temp_dict_data['TRAILINGANNUALDIVIDENDYIELD'] = indivdual_set[parameters][0]
                    temp_dict_data['TRAILINGANNUALDIVIDENDYIELDINPERCENT'] = indivdual_set[parameters][1]

            ## append to list
            self.datatype_com_data_allstock_list.append(temp_dict_data)

    def retrieve_datatype_results(self, datatype):
        """ Retrieve the results (after json) in form of dataframe for the particular datatype.
            Args:
                datatype (str): different data type according to the datatype dict keys.
        """
        self.set_and_load_datatype_url(datatype)
        self.get_com_data_fr_all_stocks()

    def retrieve_all_results(self):
        """ Retrieve the results (after json) in form of dataframe for all datatype.
            All datatype processed from the self.datatype_url_dict
        """
        self.com_data_allstock_df = pandas.DataFrame()
        
        for datatype in self.datatype_url_dict.keys():
            print "Processing datatype: ", datatype
            ## reset all temp storage
            self.datatype_com_data_allstock_list =[]
            self.datatype_com_data_allstock_df = object()

            ## run the query for particular datatype
            self.retrieve_datatype_results(datatype)
    
            ## join the tables
            if len(self.com_data_allstock_df) == 0:
                self.com_data_allstock_df = self.datatype_com_data_allstock_df.copy(True)
            else:
                self.com_data_allstock_df = pandas.merge(self.com_data_allstock_df,self.datatype_com_data_allstock_df, on= 'SYMBOL' )

        ## Remove percentage from columns
        try:
            self.rm_percent_symbol_fr_cols()
        except:
            print 'some columns are missing for stripping percentage'
                
    def rm_percent_symbol_fr_cols(self):
        """ Remove the % symbol from those columns that have this symbol.
            Convert the columns to float for later easy filtering.
            Set to self.temp_full_data_df
        """
        col_with_percent = ['OperatingMargin','ProfitMargin', 'QtrlyEarningsGrowth', 'QtrlyRevenueGrowth',
                            'ReturnonAssets','ReturnonEquity','TRAILINGANNUALDIVIDENDYIELDINPERCENT']
        for col in col_with_percent:
            self.com_data_allstock_df[col] = self.com_data_allstock_df[col].map(lambda x: float(str(x).rstrip('%').replace(',','')))

    def get_batch_hist_data(self):
        """ Get hist data using the YQL.
            This is run separately from other parameters as not able to combine with other data type.mro
            Run a single batch of of hist data. The full run will be using the self.get_all_hist_data() function.
    
        """
        self.set_hist_data_end_url()
        self.com_data_start_url = self.hist_data_start_url
        self.com_data_end_url = self.hist_data_end_url
        self.json_result_tag = 'quote'
        self.get_com_data()

    def get_all_hist_data(self):
        """ Run with the full stock retrieval.
            TODO: very similar --> see can link back to original

        """

        full_list = self.replace_special_characters_in_list(self.full_stocklist_to_retrieve)
        chunk_of_list = self.break_list_to_sub_list(self.full_stocklist_to_retrieve)
        
        self.temp_full_data_df = None
        for n in chunk_of_list:
            # print the progress
            sys.stdout.write('.')

            # set the small chunk of list
            self.set_target_stocks_list(n)
            self.get_batch_hist_data()

        # convert to dataframe
        self.datatype_com_data_allstock_df = pandas.DataFrame(self.datatype_com_data_allstock_list)
        self.datatype_com_data_allstock_df.rename(columns ={'symbol':'SYMBOL','Adj_Close':'Adj Close'}, inplace=True)

        # Add in year
        self.datatype_com_data_allstock_df['Year'] = self.datatype_com_data_allstock_df['Date'].map(lambda x: x[:4])
        
        print 'Done\n'

    def set_hist_data_num_day_fr_current(self, num_days):
        """ Set the num of days from current date to get the historical price data.
            Set to self.hist_data_num_day_fr_current
            Args:
                num_days (int): number of days to
        """
        self.hist_data_num_day_fr_current = num_days

    def convert_date_to_str(self, offset_to_current = 0):
        """ Function mainly for the hist data where it is required to specify a date range.
            Default return current date. (offset_to_current = 0)
            Kwargs:
                offset_to_current (in): in num of days. default to zero which mean get currnet date
            Returns:
                (str): yyyy-mm-dd format
        
        """
        last_eff_date_list = list((datetime.date.today() - datetime.timedelta(offset_to_current)).timetuple()[0:3])

        if len(str(last_eff_date_list[1])) == 1:
            last_eff_date_list[1] = '0' + str(last_eff_date_list[1])
    
        return str(last_eff_date_list[0]) + '-' + last_eff_date_list[1] + '-' + str(last_eff_date_list[2])

    def set_hist_data_end_url(self):
        """ Set the hist data end url (based on YQL) by the date interval.
            Set to self.hist_data_end_url.
        """
        self.hist_data_end_url = ')%20and%20startDate%20%3D%20%22' +\
                                 self.convert_date_to_str(self.hist_data_num_day_fr_current) + \
                                '%22%20and%20endDate%20%3D%20%22' + self.convert_date_to_str(0) + \
                                '%22&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback='

    def retrieve_company_symbol(self):
        """ Retrieve the list of company symbol """
        self.set_stock_sym_append_str('')
        self.set_stock_retrieval_type('all') #'all', watcher
        self.load_stock_symbol_fr_file()


if __name__ == '__main__':
    
    print "start processing"
    
    choice = 3       

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
##        file = r'c:\data\full_Feb08.csv'
##        full_stock_data_df = pandas.read_csv(file)

        w = YComDataExtr()
        #w.set_full_stocklist_to_retrieve(list(full_stock_data_df['SYMBOL']))
        w.set_full_stocklist_to_retrieve(['J69U.SI','BN4.SI'])
##        w.retrieve_company_symbol()
##        chunk_of_list = w.break_list_to_sub_list(w.full_stocklist_to_retrieve)
##        w.full_stocklist_to_retrieve  = chunk_of_list[0][:3]
        w.retrieve_all_results()
         
        print w.com_data_allstock_df
        ##
        ##full_stock_data_df = pandas.merge(full_stock_data_df, w.com_data_allstock_df, on= 'SYMBOL')

        #full_stock_data_df.to_csv(file, index = False)

    if choice ==4:
        file = r'c:\data\temp\temp_stockdata.csv'
        full_stock_data_df = pandas.read_csv(file)
        
    if choice ==5:
        """ test the class
            run the data in batch.
            how to store the data -> store as dataframe but do not group.
            
        """
        file = r'c:\data\full_Apr15.csv'
        full_stock_data_df = pandas.read_csv(file)

        w = YComDataExtr()
        w.set_full_stocklist_to_retrieve(list(full_stock_data_df['SYMBOL'])[:10])
        w.get_all_hist_data()
        print w.datatype_com_data_allstock_df.head()

        # should add the year column




                