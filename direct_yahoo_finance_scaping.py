"""
    
Module: Direct Yahoo Finance Scraping Module
Name:   Tan Kok Hua
Programming Blog: http://simplypython.wordpress.com/
Usage: To retrieve statistics through direct scraping that cannot be retrieved from YF API.
    
Updates:
    Oct 30 2014: Add in different print function and add in single line progress printing.
    Oct 04 2014: Have additional str to append to stock symbol (.SI)
    Oct 02 2014: Convert the Symbol to capital letters
    Sep 01 2014: Modified the header value function. Update on more data on 50 and 200 days movin avg
    Aug 30 2014: Add in key statistics
    Aug 29 2014: Add in industry
    Aug 28 2014: Open up the analyst opinion to include the number of broker.

Learning:
    create  an empty dataframe
    http://stackoverflow.com/questions/13784192/creating-an-empty-pandas-dataframe-then-filling-it

TODO:
    More parameters to handle
        Debt to equity ratio
        cash flow
    can put the .SI to url as this is scrape one by one
    print those that are in errror.
    May have hit limit after a while. Need time to rest before retrieving

DEBUG:
    Analyst ranking is not present --> take note when doing combinng
    To check if better way to do combining
    check on OP0 error getting

"""
import os, re, sys, time, datetime, copy
import pandas
from pattern.web import URL, DOM, plaintext, extension

class YFinanceDirectScrape(object):
    """ Class to scrape data from yahoo finance.
        All data for this class get from YF through direct scraping.
        This will scrape stock by stock.
    """
    def __init__(self):

        ## general param
        self.all_stock_sym_list     = list()    # for all stock input.
        self.individual_stock_sym   = ''
        self.stock_sym_append_str   = '.SI'     # default ".SI", extra parameters to append to the stock symbol.
        
        ## printing option -- for printing additional information
        self.print_url_str          = 0         # 1- will print url str to be query
        
        ## url forming -- for individual stock
        self.start_url              = ''        # will be preloaded with different start url.
        self.individual_stock_url   = ''
        self.full_url_str           = ''

        ## param selector
        ## for use in getting the different cycle of dict.
        self.param_selector         = ''

        ## Dict for different type of parsing. Starl url will differ.
        self.start_url_dict = {
                                'Company_desc': 'http://finance.yahoo.com/q?',
                                'analyst_opinion':'http://finance.yahoo.com/q/ao?',
                                'industry':'https://sg.finance.yahoo.com/q/in?',
                                'key_stats': 'https://sg.finance.yahoo.com/q/ks?',
                              }

        ## selector for dom objects mainly for parsing the results.
        self.css_selector_dict = {
                                'Company_desc': 'div#yfi_business_summary div[class="bd"]',
                                'analyst_opinion':['td[class="yfnc_tablehead1"]','td[class="yfnc_tabledata1"]'], # analyst -- header, data str
                                'industry':['th[class="yfnc_tablehead1]','td[class="yfnc_tabledata1]'],
                                'key_stats':['td[class="yfnc_tablehead1]','td[class="yfnc_tabledata1]'],
                                 }

        ## Method select detection
        self.parse_method_dict = {
                                'Company_desc': self.parse_company_desc,
                                'analyst_opinion': self.parse_analyst_opinion,
                                'industry': self.parse_industry_info,
                                'key_stats': self.parse_key_stats,
                                 }

        ## output storage
        self.individual_stock_df = object() # individual df for storing the individual stocks.
                                            # store all the column informaiton
        self.header_list = list() # for holding the label of individual stock
        self.value_list = list() # for holding the data of individual stock
        self.all_stock_df =  None # dataframe object for holding all stocks
        self.all_individual_df_list = list()

        self.permanent_header_list = [] # give the header list that will be recorded in csv
                                        # can formed while setting the command??

        ## fault detection
        self.url_query_timeout = 0 # for time out issue when query.

        ## print option
        self.__print_individual_stock_data = 0  # for verbose printing of stock progress
        self.__print_parsing_problem = 0        # for those symbol that have problem
        self.__print_url_finding_error = 0      # print problem with url handling

    def set_stock_sym_append_str(self, append_str):
        """ Set additional append str to stock symbol.
            Set to sel.stock_sym_append_str.
            Args:
                append_str (str): additional str to append to stock symbol.
        """
        self.stock_sym_append_str = append_str

    def set_stock_to_retrieve(self, stock_sym):
        """ Set the stock symbol required for retrieval.
            Will append the self.stock_sym_append_str to the stock symbol.
            Args:
                stock_sym (str): Input the stock symbol.
        """
        assert type(stock_sym) == str
        self.individual_stock_sym = stock_sym + self.stock_sym_append_str

    def set_multiple_stock_list(self, stocklist):
        """ Set the multiple stock list. Set to self.all_stock_sym_list.
            Args:
                stocklist (list): list of stocks symbol.
        """
        self.all_stock_sym_list = stocklist
        
    def form_stock_part_url(self):
        """ Formed the stock portion of the url for query.
            Require the self.individual_stock_sym not to be empty
        """
        assert self.individual_stock_sym is not None
        fixed_portion = 's='
        self.individual_stock_url  = fixed_portion + self.individual_stock_sym 

    def get_list_of_param_selector_avaliable(self):
        """ Print out the list of param_selector avaliable that make it easier to set.
        """
        print self.start_url_dict.keys()

    def set_param_selector(self, param_type):
        """ Set the param selector necessary to form the full url.
            Args:
                param_type (str): set the param type that will be present in self.start_url_dict.
            Except:
                Will raise if the param type is not present.
        """
        if param_type not in self.start_url_dict.keys():
            print 'param type selected not valid.'
            raise
        self.param_selector =  param_type

    def quick_set_symbol_and_param_type(self, sym, param_type):
        """ A quick method to set the symbol and param type at one go.
            Will also set the 
            Args:
                sym (str): stock symbol.
                param_type (str): type of data to pull.
        """
        self.set_stock_to_retrieve(sym)
        self.set_param_selector(param_type)
        
    def form_full_url(self):
        """ Formed the full url based on the self.param_selectors.
            self.param_selector must lies in the start_url_dict keys.
            Will set the self.full_url_str.
        """
        assert self.param_selector in self.start_url_dict.keys()
        self.form_stock_part_url()
        self.start_url = self.start_url_dict[self.param_selector]
        self.full_url_str = self.start_url + self.individual_stock_url

    def create_dom_object(self):
        """ Create dom object based on element for scraping
            Take into consideration that there might be query problem.
            
        """
        try:
            url = URL(self.full_url_str)
            self.dom_object = DOM(url.download(cached=True))
        except:
            if self.__print_url_finding_error: print 'Problem retrieving data for this url: ', self.full_url_str
            self.url_query_timeout = 1

    def __dom_object_isempty(self, dom_object):
        """ Check if dom object is empty. Check if the URL return any useful information.
            Args:
                dom_object (dom object): DOM object from the URL
            Returns:
                (bool): True if empty.
        """
        if len(dom_object) == 0:
            if self.__print_parsing_problem: print 'Nothing being parsed'
            return True
        else:
            return False
        
    def tag_element_results(self, dom_obj, tag_expr):
        """ Take in expression for dom tag expression.
            Args:
                dom_obj (dom object): May be a subset of full object.
                tag_expr (str): expression that scrape the tag object. Similar to xpath.
                                Use pattern way of parsing.
            Returns:
                (list): list of tag_element_objects.

            TODO: May need to check for empty list.
        """
        return dom_obj(tag_expr)

    def parse_all_parameters(self):
        """ Parse all the parameters based on the self.start_url_dict.
            After parse, set to object

        """
        for n in self.start_url_dict.keys():
            if self.__print_individual_stock_data: print 'Parsing parameters: ', n
            self.set_param_selector(n)
            self.form_full_url()
            if self.print_url_str: print self.full_url_str
            self.parse_method_dict[n]()
            if self.url_query_timeout: return 

        ## set the symbol to the list and create to dataframe.
        self.header_list.insert(0, 'SYMBOL')
        self.value_list.insert(0, self.individual_stock_sym)# need to convert to columns

        ## create the stock df here.
        self.create_individual_stock_df()

    def create_individual_stock_df(self):
        """Create dataframe of individual stock based on the header and value list."""
        self.individual_stock_df = pandas.DataFrame(self.value_list).transpose()
        self.individual_stock_df.rename(columns={org: change for org, change\
                                           in zip(range(len(self.value_list)),self.header_list)},\
                                              inplace=True)
        
    def parse_company_desc(self):
        """ Method to parse the company info.
            Specific to self.param_selector =  'Company_desc'.
        """
        assert self.param_selector == 'Company_desc'
        self.create_dom_object()
        if self.url_query_timeout: return 
        dom_object = self.tag_element_results(self.dom_object, self.css_selector_dict[self.param_selector] )
        if self.__dom_object_isempty(dom_object):
            return
        try:
            self.value_list.append(str(dom_object[0][0]))
            self.header_list.append('company_desc')
        except:
            print "problem with company desc, ", self.individual_stock_sym

    def parse_analyst_opinion(self):
        """ Method to parse the analyst info.
            Specific to self.param_selector =  'analyst_opinion'.

            TODO: can make to general function.
        """
        assert self.param_selector == 'analyst_opinion'
        self.create_dom_object()
        if self.url_query_timeout: return

        self.parse_one_level_header_value_set(0,8)


    def parse_one_level_header_value_set(self, parse_start, parse_end):
        """ Use for parsing general format of header value data.
            Condition is that both header and value need to be scape.
            If header is missing, str away return without getting the values.
            The dom object that determine the parsing is of below form:
            dom_object[parse_start:parse_end)]
            
            self.param_selector have to be set and the parse_qty need to be inputted.
            Output will be saved to teh self.header_list and self.value_list directly
            
            Args:
                parse_start (int): start of the total list to be parsed.
                parse_end (int): end of the total list to be parsed.

            Why return object??

        """
        
        # process the header --> need 5 header
        dom_object = self.tag_element_results(self.dom_object, self.css_selector_dict[self.param_selector][0] )
        if self.__dom_object_isempty(dom_object):
            return
        for n in dom_object[parse_start:parse_end]:
            self.header_list.append(str(n.children[0]).strip(':'))
        #value
        dom_object = self.tag_element_results(self.dom_object, self.css_selector_dict[self.param_selector][1] )
        for n in dom_object[parse_start:parse_end]:
            try:
                temp_value_data = n.content
                if temp_value_data.isalnum:
                    self.value_list.append(temp_value_data) #if str append as str
                else:
                    self.value_list.append(temp_value_data)
            except:
                self.value_list.append(0)
            
        return dom_object

    def parse_industry_info(self):
        """ Get the industry categories of the particular stocks."""
        assert self.param_selector == 'industry'
        self.create_dom_object()
        if self.url_query_timeout: return

        self.parse_one_level_header_value_set(0,2)

    def parse_key_stats(self):
        """ Parse key statistics. Especially the financial data."""
        assert self.param_selector == 'key_stats'
        self.create_dom_object()
        if self.url_query_timeout: return

        self.parse_one_level_header_value_set(11,19)
        self.parse_one_level_header_value_set(21,31)
        self.parse_one_level_header_value_set(36,38)

    def clear_all_temp_store_data(self):
        """ Clear all the temporary store data for processing and clear fault.
        """
        self.value_list = []
        self.header_list = []
        self.individual_stock_df = object()
        self.url_query_timeout = 0

    def obtain_multiple_stock_data(self):
        """ Obtain multiple stocks data.
            Temporary do not get the 

        """
        for n in self.all_stock_sym_list:
            self.clear_all_temp_store_data()
            if self.__print_individual_stock_data: print 'Getting info for stock: ', n
            self.set_stock_to_retrieve(n)
            self.parse_all_parameters()

            ## joining
            if not self.url_query_timeout:  
                self.all_individual_df_list.append(self.individual_stock_df)
                if self.all_stock_df is None:
                    self.all_stock_df = self.individual_stock_df
                    self.permanent_header_list = copy.copy(self.header_list)
                else:
                    if len(self.individual_stock_df.columns) > len(self.all_stock_df.columns):
                        self.all_stock_df = self.individual_stock_df.append(self.all_stock_df)
                        ## may have problem with the header list
                        self.permanent_header_list = copy.copy(self.header_list)
                    else:
                        self.all_stock_df = self.all_stock_df.append(self.individual_stock_df)
                sys.stdout.write('.')
            else:
                sys.stdout.write('T:%s'%n) #if time out problem, print the error

            if self.__print_individual_stock_data: print '*'* 18, '\n'

        ## set the object to file
        self.all_stock_df = self.all_stock_df.reindex(columns = self.permanent_header_list)
        self.all_stock_df.to_csv(r'c:\data\extrainfo.csv', index=False)


if __name__ == '__main__':
    print
    choice = 4

    if choice == 1:
            ss = YFinanceDirectScrape()
            ss.quick_set_symbol_and_param_type('CC3', 'analyst_opinion')
##            ss.quick_set_symbol_and_param_type('S58.SI', 'analyst_opinion')
            ss.form_full_url()
##            print
##            print ss.get_list_of_param_selector_avaliable()
##            print ss.full_url_str
##            #ss.parse_company_desc()
##            d = ss.parse_analyst_opinion()
##            print ss.header_list, ss.value_list
            ss.parse_all_parameters()
            print ss.individual_stock_df
            ss.individual_stock_df.to_csv(r'c:\data\check.csv')

    if choice  == 3:
        
        url_str ='https://sg.finance.yahoo.com/q/ks?s=S24.SI'
        url =  URL(url_str)
        dom_object = DOM(url.download(cached=True))
        #get the yeear
        w= dom_object('td[class="yfnc_tabledata1"]')
        w= dom_object('td[class="yfnc_tablehead1]')
        for n in range(len(w)):
            print n
            print w[n].content

    if choice == 2:
        yf = YFinanceDirectScrape()
        yf.create_dom_object()
        # b tag inside the td
        # based on css selectors
        for n in yf.tag_element_results(yf.dom_object, 'td[class="yfnc_modtitle1"] b'):
            print n.content

        ## specified 2nd row of table
        for n in yf.tag_element_results(yf.dom_object, 'td[align="right"] strong')[:4]:
            print n.content
            # will be the first 4 years

    if choice ==4:

        #or str away append ot csv

        ## read  data from .csv file -- full list of stocks
        csv_fname = r'C:\pythonuserfiles\yahoo_finance_data_extract\stocklist.csv'
        csv_fname = r'C:\data\compile_stockdata\full_20150428.csv'
        stock_df = pandas.read_csv(csv_fname)
        # convert from pandas object to list
        stock_list = list(stock_df['SYMBOL'])
        #stock_list = list(stock_list['Symbol'])
        #stock_list = ['S58','C0R3', 'OO']
        #stock_list = ['S58.SI']
        #stock_list =  stock_list[:3]

        ss = YFinanceDirectScrape()
        ss.set_stock_sym_append_str('')
        ss.set_multiple_stock_list(stock_list)
        ss.obtain_multiple_stock_data()
        print
        print ss.all_stock_df
        ss.all_stock_df.to_csv(r'C:\data\stock_sql_db\company_data.csv',index=False)

    if choice == 5:
        ss.all_stock_df = None        
        for n in ss.all_individual_df_list:
            ss.individual_stock_df = n
            
            if ss.all_stock_df is None:
                ss.all_stock_df = ss.individual_stock_df
                ss.permanent_header_list = copy.copy(ss.header_list)
            else:
                if len(ss.individual_stock_df.columns) > len(ss.all_stock_df.columns):
                    ss.all_stock_df = ss.individual_stock_df.append(ss.all_stock_df)
                    ss.permanent_header_list = copy.copy(ss.header_list)
                else:
                    ss.all_stock_df = ss.all_stock_df.append(ss.individual_stock_df)

    if choice == 6:
        """individual parsing"""
        ss = YFinanceDirectScrape()
        ss.quick_set_symbol_and_param_type('CC3', 'analyst_opinion')
##            ss.quick_set_symbol_and_param_type('S58.SI', 'analyst_opinion')
        ss.form_full_url()
            


    
    