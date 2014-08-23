"""
https://developer.yahoo.com/finance/company.html
--> RSS feed
https://code.google.com/p/yahoo-finance-managed/wiki/miscapiRssFeed
language
https://developer.yahoo.com/boss/search/boss_api_guide/supp_regions_lang.html
--> not very useful....

"""
import os, re, sys, time, datetime, copy
import pandas
from pattern.web import URL, extension
from pattern.web import URL, DOM, plaintext

## have a class??
## main to get the cash flow earning  data


## quick way to pass in the dom object --> like having two iterations --> make it a habit
## or make it go number of depth defined (recusion)???
## may not need as can be nested within the str.

#use yahoo query???

class YFinanceDirectScrape(object):
    """ Class to scrape data from yahoo finance.
        Method through direct scraping.
        
    """

    def __init__(self):
        ## url forming
        self.full_url_str = 'https://sg.finance.yahoo.com/q/cf?s=S58.SI&annual'

        ## dom objects
        # the different tag element for parsing.
        

    def create_dom_object(self):
        """ Create dom object based on element for scraping"""
        url = URL(self.full_url_str)
        self.dom_object = DOM(url.download(cached=True))
        
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

if __name__ == '__main__':
    print
    choice = 2

    if choice  == 1:
        
        url_str ='http://feeds.finance.yahoo.com/rss/2.0/headline?s=S58.SI&region=SG&lang=en-SG'
        url =  URL(url_str)
        dom_object = DOM(url.download(cached=True))
        #get the yeear
        dom_object('td[class="yfnc_modtitle1"]')

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

    
    