"""
Module: extract_all_stock_symbols
Name  : Tan Kok Hua

Usage:
    To retrieve all the stocks symbol from particular market by scaping from Yahoo Finance.

Requires:
    Pattern.
    pandas.

Updates:
    Aug 16 2014: First draft.

Question:
    web output might not be in order???

"""
import re, sys, os, time, string
import pandas
from pattern.web import URL, DOM, plaintext

class AllSymExtr(object):
    """ Class to extract all symbol. Current default Singapore market.
    """
    def __init__(self):
        
        # URL forming -- Current Singapore
        # default m (market) - SG, t (type) - S (stock)
        self.sym_start_url = "https://sg.finance.yahoo.com/lookup/stocks?t=S&m=SG&r="
        self.sym_page_url = '&b=0'#page
        self.sym_alphanum_search_url = '&s=a' #search alphabet a
        self.sym_full_url = ''
        self.alphanum_str_to_search = string.ascii_lowercase # full alphabet

        # Parameters/ output
        self.dom_object = object()  # For storing the dom object of the url for further parsing
        self.sym_list   = list()    # For all symbol list storage. 
        self.sym_df     = object()  # dataframe object for the symbol list (extra output)

    def set_alphanum_portion_url(self, alphanum):
        """ Set the alphabet portion of the url by passing the alphabet.
            Args:
                alphanum (str): can be alphabet or digits.
        """
        self.sym_alphanum_search_url = '&s=' + str(alphanum)

    def set_page_portion_url(self, pageno):
        """ Set the page portion of the url by passing the pageno.
            Args:
                pageno (str): page number.
        """
        self.sym_page_url = '&b=' + str(pageno)

    def form_full_sym_url(self):
        """ Give the full url necessary for sym scan by joining the search parameter and page no.
        """
        self.sym_full_url =  self.sym_start_url + self.sym_alphanum_search_url + self.sym_page_url
        
    def set_dom_object_fr_url(self):
        """ Set the DOM object from url self.sym_full_url.

        """
        url =  URL(self.sym_full_url)
        self.dom_object = DOM(url.download(cached=True))

    def get_sym_for_each_page(self):
        """ Scan all the symbol for one page. The parsing are split into odd and even rows.
        """
        self.set_dom_object_fr_url()

        for n in self.dom_object('tr[class="yui-dt-odd"]'):
            for e in n('a'):
                self.sym_list.append(str(e[0]))
                
        for n in self.dom_object('tr[class="yui-dt-even"]'):
            for e in n('a'):
               self.sym_list.append(str(e[0]))

    def get_total_page_to_scan(self):
        """ Get the total search results based on each search to determine the number of page to scan.
            Args:
                (int): The total number of page to scan
            Current handle up to 999,999 results
        """
        #Get the number of page
        total_search_str = self.dom_object('div#pagination')[0].content
        total_search_qty = re.search('of ([1-9]*\,*[0-9]*).*',total_search_str).group(1)
        total_search_qty = int(total_search_qty.replace(',','', total_search_qty.count(',')))
        final_search_page_count = total_search_qty/20 #20 seach per page.

        return final_search_page_count

    def get_total_sym_for_each_search(self):
        """ Scan all the page indicate by the search item.
            The first time search or the first page will get the total number of search.
            Dividing it by 20 results per page will give the number of page to search.
        """
        # Get the first page info first
        self.set_page_portion_url(0)
        self.form_full_sym_url()
        self.get_sym_for_each_page()# get the symbol for the first page first.
        total_page_to_scan =  self.get_total_page_to_scan()
        print 'total number of pages to scan: ', total_page_to_scan

        # Scan the rest of the page.
        # may need to get time to rest
        for page_no in range(1,total_page_to_scan+1,1):
            self.set_page_portion_url(page_no*20)
            self.form_full_sym_url()
            print 'Scanning page number: ', page_no, ' url: ',   self.sym_full_url          
            self.get_sym_for_each_page()

    def sweep_of_seach_item(self):
        """ Sweep through all the alphanum to get the full list of shares.
        """
        for alphanum in self.alphanum_str_to_search:
            print
            print 'searching: ', alphanum
            self.set_alphanum_portion_url(alphanum)
            self.get_total_sym_for_each_search()

    def convert_data_to_df_and_rm_duplicates(self):
        """ Convert the data set to dataframe object and remove all duplicates

        """
        self.sym_df = pandas.DataFrame(self.sym_list).drop_duplicates()
        self.sym_df.rename(columns = {0:'SYMBOL'}, inplace =True)

if __name__ == '__main__':
    print "start processing"
    
    ## initialize the class
    sym_extract = AllSymExtr()
    
    ## list the alphabets and number to search. To search all will label a to z
    ## for demo, only search 'a' and 'b'.
    sym_extract.alphanum_str_to_search = 'ab'

    ## perform sweep of each search alphabet and each page
    sym_extract.sweep_of_seach_item()

    ## convert to dataframe and remove duplicates.
    sym_extract.convert_data_to_df_and_rm_duplicates()
    print sym_extract.sym_df




    ## for breaking down
##    sym_extract.set_alphanum_portion_url('a')
##    sym_extract.set_page_portion_url(0)
##    sym_extract.form_full_sym_url()
##    print sym_extract.sym_full_url
##    sym_extract.get_sym_for_each_page()
##    print sym_extract.sym_list
##    print 
##    print sym_extract.get_total_page_to_scan()
##    print
##    sym_extract.get_total_sym_for_each_search()
##    print sym_extract.sym_list
##    print 














