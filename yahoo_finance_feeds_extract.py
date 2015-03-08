"""
    Module: Yahoo finance feeds extractor
    Name:   Tan Kok Hua
    
    YF API from:
    https://code.google.com/p/yahoo-finance-managed/wiki/CSVAPI

    Updates:
        Oct 25 2014: Initial script up.


    Learning:


    TODO:
        Alerts
        remove the buyback scheme
        download as xml and use feed parser
        convert the date
        events for the past one week
        saving the data.
        ability to add feed easily

        Save in more feeds
        http://en.wikipedia.org/wiki/List_of_financial_data_feeds
        make the feed start url to loop in a lsit
        One more function that have creation of mulipile start and end 
        assume all RSS reader use the same tag

        display only the last 5 days or so

        how to save this
        may need to store the link as well
        may need to change some of the tag for the google

        Separate function for printing.

    News
    http://www.shareinvestor.com/news/index.html#/?type=regional_news_all&page=1

        
    Bugs:
    google rss got some problem --> still display certain html --. use plain text to strp??
    --> still messy with double resutls.

    use pattern feed parser http://www.clips.ua.ac.be/pages/pattern-web
    

                
"""

import os, re, sys, time, datetime, copy, calendar
import pandas
from pattern.web import URL, extension, cache, plaintext
import feedparser
 
class YFinanceFeedsExtr(object):
    """ Class to extract feeds from yahoo finance.
        Achieved by query the various url and downloading the respectively .csv files.
        Further analysis of data done by pandas.
    """
    def __init__(self):
        """ List of url parameters """
        # Param
        self.all_stock_sym_list = ['S58.SI','S68.SI'] ##special character need to be converted
        self.individual_stock_sym = '' #full range fo stocks

        # multiple url storage -- list of start and end list
        self.multiple_url_start_end_list = [
                                            ["http://feeds.finance.yahoo.com/rss/2.0/headline?s=", ".SI&region=SG&lang=en-SG"],#yahoo finance
                                            #["http://www.google.com/finance/company_news?q=SGX:", "&output=rss"], #google finance
                                            ]
                                            
        # URL forming 
        self.feeds_start_url = "http://feeds.finance.yahoo.com/rss/2.0/headline?s="
        self.feeds_stock_portion_url = ''
        self.feeds_stock_portion_additional_url = ''# for adding additonal str to the stock url.
        self.feeds_end_url = ".SI&region=SG&lang=en-SG"
        self.feeds_full_url = ''

        # Output storage
        self.feeds_xmlfile = r'c:\data\temp\feeds_data.xml'
        self.feeds_dict = dict() # storage as dict with sym as keys and the date as sub keys

        # for debug/printing
        self.store_individual_set_df = []
        self.__print_url = 1 # for printing the url string

        # Output data settng and output display
        self.date_interval = 5 # num of days to retrieve the dataset

    def set_stock_sym_append_str(self, append_str):
        """ Set additional append str to stock symbol when forming stock url.
            Set to sel.feeds_stock_portion_additional_url.
            Mainly to set the '.SI' for singapore stocks.
            Args:
                append_str (str): additional str to append to stock symbol.
        
        """
        self.feeds_stock_portion_additional_url = append_str

    def set_stock_to_retrieve(self, stock_sym):
        """ Set the stock symbol required for retrieval.
            Args:
                stock_sym (str): Input the stock symbol.
        """
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
        self.feeds_stock_portion_url  = fixed_portion + self.individual_stock_sym
        
    def form_url_str(self):
        """ Form the url str necessary to get the .xml file.close
        """
        self.form_stock_part_url()
        self.feeds_full_url = self.feeds_start_url + self.feeds_stock_portion_url +\
                                   self.feeds_stock_portion_additional_url + self.feeds_end_url

    def set_start_end_url(self, start_url, end_url):
        """ Set the start and end url for multiple feeds pulling.
            Set to self.feeds_start_url, self.feeds_end_url.
            Args:
                start_url (str): start url portion of str.
                end_url (str): end url portion of str.
        """ 
        self.feeds_start_url = start_url
        self.feeds_end_url = end_url


    # use html -- direct scaping method
    def downloading_xml(self, url_address):
        """ Download the xml information from the url_address given.
        """
        cache.clear()
        url = URL(url_address)
        f = open(self.feeds_xmlfile, 'wb') # save as test.gif
        f.write(url.download())
        f.close()

    def parse_date(self, date_str):
        """ For date parsing of info. Return as date key for easy compilation.
            May make it capital for more 
            Args:
                date_str (list): date in list [yyyy, mm ,dd]
            Returns:
                key (int): return as date key as yyyy mm dd
        """
        date_list = date_str.split()

        month_dict = {v: '0'+str(k) for k,v in enumerate(calendar.month_abbr) if k <10}
        month_dict.update({v:str(k) for k,v in enumerate(calendar.month_abbr) if k >=10})

        return int(date_list[3] + month_dict[date_list[2]] + date_list[1])

    def parse_xml_file(self):
        """ Parse the information in the xml file stored.
            Use the self.feeds_xmlfile
        """
        reader = feedparser.parse(self.feeds_xmlfile)
        for n in reader.entries:
            ## capture the stock date key as key
            date_key = self.parse_date(n.published)
            data_set = [n.title, plaintext(n.description)]
            
            if self.feeds_dict[self.individual_stock_sym].has_key(date_key):
                self.feeds_dict[self.individual_stock_sym][date_key].append(data_set)
            else:
                self.feeds_dict[self.individual_stock_sym][date_key] = data_set
##            print date_key
##            print n.title
##            print plaintext(n.description)
        
    def get_all_feeds(self):
        """ Get all stocks feeds. Create a dict for storing of indivdual stocks

        """
        for stock in self.all_stock_sym_list:
            self.feeds_dict[stock]= dict()
            print 'Assess current stock: ', stock
            self.set_stock_to_retrieve(stock)
            self.get_single_stock_all_feeds()
            print '--'*18

    def get_stock_individual_feed(self):
        """ Get the individual feeds
            Formed the url, download the csv
        """
        self.form_url_str()
        if self.__print_url: print self.feeds_full_url
        self.downloading_xml(self.feeds_full_url)
        self.parse_xml_file()

    def get_single_stock_all_feeds(self):
        for start_url, end_url in self.multiple_url_start_end_list:
            print start_url
            self.set_start_end_url(start_url, end_url)
            self.get_stock_individual_feed()

    ## get date delta -- so can print within the target frame
    def calculate_start_and_end_date(self):
        """ Return the start and end (default today) based on the interval range in int fomat yyyymmdd.
            Returns:
                start_date_num (int) : num in yyyy mm dd of the past date
                end_date_num (int): tupe in yyyy mm dd of current date today
        """
        end_date_tuple = datetime.date.today().timetuple()[0:3] ## yyyy, mm, dd
        start_date_tuple = (datetime.date.today() - datetime.timedelta(self.date_interval)).timetuple()[0:3]
        start_date_num = int(str(start_date_tuple[0]) + str(start_date_tuple[1]) + str(start_date_tuple[2]))
        end_date_num = int(str(end_date_tuple[0]) + str(end_date_tuple[1]) + str(end_date_tuple[2]))
        return start_date_num, end_date_num

    def print_feeds_for_all_stocks(self):
        """ Print 

        """
        for sym in self.all_stock_sym_list:
            print 'Currently printing feeds for stock: ', sym
            self.print_feeds_for_one_stock(sym)

    def print_feeds_for_one_stock(self, sym):
        """ Print the feeds for a stock limit by the date interval.
            The earliest date determine by the 

        """
        all_feeds_for_indivdual_stock = self.feeds_dict[sym]
        ## get the date interval.
        start_date_num, end_date_num = self.calculate_start_and_end_date()
        for datekey in all_feeds_for_indivdual_stock.keys():
            if datekey >= start_date_num:
                current_feeds = all_feeds_for_indivdual_stock[datekey]
                print datekey
                print current_feeds[0]
                print current_feeds[1]
                


if __name__ == '__main__':
    
    print "start processing"
    
    choice = 1

    if choice == 1:
        feed_ext = YFinanceFeedsExtr()

        ## running all stock information
        feed_ext.set_multiple_stock_list(['OV8','J69U','S05','N4E','BN4'])
        #feed_ext.set_multiple_stock_list(['OV8', 'J69U'])
        feed_ext.set_interval_to_retrieve(10)
        feed_ext.get_all_feeds()
        feed_ext.print_feeds_for_all_stocks()

    if choice ==2:
        reader = feedparser.parse(feed_ext.feeds_xmlfile)
        for n in reader.entries[:1]:
            print n.published
            print n.title
            print n.description

    if choice ==3:
        feed_ext = YFinanceFeedsExtr()
        print feed_ext.calculate_start_and_end_date()







