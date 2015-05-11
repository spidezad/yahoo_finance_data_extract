"""
    Module for gettting the stock announcement from SGX.
    Also include the json web extract class.

    Updates:
        Apr 30 2015: Resolve bug where there is not data.
                     Add in buy to sell volume ratio.
        Apr 22 2015: Combined shortsell with curr price. notification add in shortsell vol perc
        Apr 20 2015: Add in shortsell info function and notification
        Apr 18 2015: Replace the str variable with non keyword url_data
        Apr 14 2015: Change the price alert for lower, only alert when price >0 
        Apr 09 2015: Add in get stock announcment from excel.
        Mar 26 2015: Have the price limit alert.
        Mar 23 2015: Set alert using pushbullet.
        Mar 21 2015: Join the stock index to the various ex date and announcement

    learning:
        https://docs.python.org/2/library/difflib.html

        Adding quotes to words
        http://stackoverflow.com/questions/15961990/adding-quotes-to-words-using-regex-in-python

    Bugs:
        take note of cases where the announcment is zero hence givning another form

    ToDo:
        rewrite the push button as dupication
        some of the self.announcment might not be working.
        filter the company announcement by category

        custom alerts
        need to creat a dataframe that can have date montior

        need to combine with the current volume for short volumne percentagate

        should put the df formation in process_all_data fucntion

        Need to store the data in case some data are missing.

    Bug:need to settle case where some of table are missing.

"""

import os, re, sys, time, datetime, copy, calendar
import difflib
import simplejson as json
import pandas
from pattern.web import URL, extension, cache, plaintext, Newsfeed
from xls_table_extract_module import XlsExtractor

from pyPushBullet.pushbullet import PushBullet

class WebJsonRetrieval(object):
    """
        General object to retrieve json file from the web.
        Would require only the first tag so after that can str away form the dict
    """
    def __init__(self):
        """ 

        """
        ## parameters
        self.saved_json_file = r'c:\data\temptryyql.json'
        self.target_tag = '' #use to identify the json data needed

        ## Result dataframe
        self.result_json_df = pandas.DataFrame()

    def set_url(self, url_str):
        """ Set the url for the json retrieval.
            url_str (str): json url str
        """
        self.com_data_full_url = url_str

    def set_target_tag(self, target_tag):
        """ Set the target_tag for the json retrieval.
            target_tag (str): target_tag for json file
        """
        self.target_tag = target_tag
        
    def download_json(self):
        """ Download the json file from the self.com_data_full_url.
            The save file is default to the self.saved_json_file.

        """
        cache.clear()
        url = URL(self.com_data_full_url)
        f = open(self.saved_json_file, 'wb') # save as test.gif
        try:
            url_data = url.download(timeout = 50)
        except:
            url_data = ''

        f.write(url_data) 
        f.close()

    def process_json_data(self):
        """ Processed the json file for handling the announcement.

        """
        try:
            self.json_raw_data  = json.load(open(self.saved_json_file, 'r'))
        except:
            print "Problem loading the json file."
            self.json_raw_data = [{}] #return list of empty dict


    def convert_json_to_df(self):
        """ Convert json data (list of dict) to dataframe.
            Required the correct input of self.target_tag.

        """
        self.result_json_df = pandas.DataFrame(self.json_raw_data[self.target_tag])   
    

class SGXDataExtract(WebJsonRetrieval):
    """ Class for extracting all sgx website information.        
    """
    def __init__(self):
        super(SGXDataExtract, self).__init__()
        # dict will contain a tuple of website and the target tag
        self.retrieval_dict = {
                                'company_info':('http://54.254.221.141/sgx/search?callback=jQuery111008655668143182993_1425728207793&json=%7B%22criteria%22%3A%5B%5D%7D&_=1425728207802',
                                                'companies'),
                                
                                'announcement': ('http://www.sgx.com/proxy/SgxDominoHttpProxy?timeout=100&dominoHost=http%3A%2F%2Finfofeed.sgx.com%2FApps%3FA%3DCOW_CorpAnnouncement_Content%26B%3DAnnouncementToday%26R_C%3D%26C_T%3D200',
                                                 'items'),

                                'ex_div_data': ('http://www.sgx.com/proxy/SgxDominoHttpProxy?timeout=100&dominoHost=http%3A%2F%2Finfofeed.sgx.com%2FApps%3FA%3DCow_CorporateInformation_Content%26B%3DCorpDistributionByExDate%26S_T%3D1%26C_T%3D400',
                                                 'items'),

                                'curr_price': ('http://sgx.com/JsonRead/JsonData?qryId=RStock&timeout=30',
                                                 'items'),
                                }

        ## for short sell information.
        self.shortsell_info_start_url = 'http://sgx.com/wps/wcm/connect/sgx_en/home/market_info/short_sale/short_sale_daily/DailyShortSell'
        self.shortsell_date_url = '20150417'
        self.shortsell_end_url= '.txt'
        self.shortsell_full_url = r''

        ## parameters
        self.saved_json_file = r'c:\data\temptryyql.json'
        self.saved_parm_df_dict = {} #storing the final df in dict with type as keyword

        ## for setting up custom alerts
        self.custom_alert_dict_list = [] #list of dict??
        self.custom_alert_df = pandas.DataFrame() #convert the custom alert dict to 

        ## final output paramters
        self.sgx_announ_df = pandas.DataFrame()
        self.sgx_div_ex_date_df = pandas.DataFrame()
        self.sgx_curr_price_df = pandas.DataFrame()
        self.price_limit_alerts_df = pandas.DataFrame()
        self.shortsell_info_df = pandas.DataFrame()
        self.sgx_curr_plus_company_df = pandas.DataFrame()

        ## shortend output version for alert creation
        self.div_ex_date_shtver = ''
        self.filtered_announ_shtver = ''

        ## watchlist to set
        self.price_limit_reach_watchlist = []
        self.announce_watchlist = []

        ## target stocks for announcements -- using excel query
        xls_set_class = XlsExtractor(fname = r'C:\data\stockselection_for_sgx.xls', sheetname= 'stockselection',
                             param_start_key = 'stock//', param_end_key = 'stock_end//',
                             header_key = 'header#2//', col_len = 2)
        xls_set_class.open_excel_and_process_block_data()
        self.announce_watchlist = xls_set_class.data_label_list #also get the company name
        self.companyname_watchlist = [n[0].encode() for n in xls_set_class.data_value_list] #also get the company name
               
    #May add this method to the base class
    def modify_json_file(self):
        """ For modification for the original file.
            Require to remove some character for json to work correctly.
            remove the first 4 character. 
        """
        with open(self.saved_json_file,'r') as f:
            data = f.read()
        #write back the data
        with open(self.saved_json_file,'w') as f:
            data_str = data[4:]
            f.write(data_str)

    def modify_jquery_json_file(self):
        """ For modification for the original file.
            Require to remove some character for json to work correctly.
            Remove the jquery calling function .
        """
        with open(self.saved_json_file,'r') as f:
            data = f.read()
        #write back the data
        with open(self.saved_json_file,'w') as f:
            start_index = data.index('{')
            end_index = -1* (data[::-1].index('}'))
            data_str = data[start_index:end_index]
            f.write(data_str)

    def modify_jquery_json_file_for_curr_price(self):
        """ For modification for the original download json file. 
            Require to remove some character for json to work correctly.
        """
        with open(self.saved_json_file,'r') as f:
            raw_data = f.read()
            
        #remove the front few characters
        raw_data = raw_data[4:]

        # Have double quotes on the key of dict
        replacer = re.compile("(\w+):")
        modified_data = replacer.sub(r'"\1":', raw_data)

        # Replace values of single quotes with double quotes.
        replacer2 = re.compile("\'")
        modified_data = replacer2.sub(r'"', modified_data)

        # Change the label of the time item
        modified_data = re.sub('"label":.*:[0-9][0-9]\s[A|P]M",','"label":1,',modified_data)

        with open(self.saved_json_file, 'w') as f:
            f.write(modified_data)

    def process_all_data(self):
        """ Process all data from the self.retrieval_dict
        """
        for sgx_item in self.retrieval_dict.keys():
            sgx_item_url, sgx_item_tag = self.retrieval_dict[sgx_item]
            self.set_url(sgx_item_url)
            self.set_target_tag(sgx_item_tag)
            self.download_json()
            try:
                if sgx_item == 'company_info':
                    self.modify_jquery_json_file()
                elif sgx_item == 'curr_price':
                    self.modify_jquery_json_file_for_curr_price()
                else:
                    self.modify_json_file()
            except:
                print "problem modifying the json file",sgx_item
                continue
                
            self.process_json_data()
            try:
                self.convert_json_to_df()
            except:
                print "problem converting json to df, ", sgx_item
                continue
            self.saved_parm_df_dict[sgx_item] = self.result_json_df

        ## retrieve the shortsell info which is separate from the retrieval dict
        self.retrieve_shortsell_info()   

        ## form the various dataframe.
        self.retrieve_curr_price_df()
        self.joined_curr_to_company_info_data()
            

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
    
        return int(str(last_eff_date_list[0]) + last_eff_date_list[1] + str(last_eff_date_list[2]))

    def get_company_going_ex_date(self, days_fr_cur):
        """ Get the company that is going ex date , with num of days fr cur as specified by the days_fr_cur.
            Required joined_relevent_sgx_data function to run first,hence the self.sgx_div_ex_date_df.
            
            Args:
                days_fr_cur (int): num of days from current date.
            Returns:
                (Dataframe object): filtered data of the self.sgx_div_ex_date_df

        """
        ex_date_df = self.sgx_div_ex_date_df.copy()

        # change the NA to 0 and convert to int.
        ex_date_df.fillna(0, inplace = True)
        ex_date_df['EXDate'] = ex_date_df['EXDate'].astype(int)
        
        target_df =  ex_date_df[ex_date_df['EXDate']- self.convert_date_to_datekey()>0]
        target_df =  target_df[target_df['EXDate']<= self.convert_date_to_datekey(-1*days_fr_cur)]

        ##shorten version for alert sending
        self.div_ex_date_shtver = target_df[['CompanyName','EXDate']].to_string()

        return target_df

    def print_company_announcement(self):
        """ For getting company announcemnt
        """
        print self.saved_parm_df_dict['announcement']
        k = self.saved_parm_df_dict['announcement'][['IssuerName','CategoryName','AnnTitle']]
        k.to_csv(r'c:\data\temp\company_announcement.csv')

    def process_sgx_stockinfo(self):
        """ Handle the symbol to name join: tickerCode
        add inthe .SI to join to main moudle

        """
        sgx_stockinfo_df = self.saved_parm_df_dict['company_info']
        return sgx_stockinfo_df

    def joined_curr_to_company_info_data(self):
        """ Joined the current price df to the company information.
            Will be used to replace the yahoo finance curr data.

        """
        stock_info_df = self.saved_parm_df_dict['company_info']
        self.sgx_curr_plus_company_df = pandas.merge(self.sgx_curr_price_df, stock_info_df, left_on = 'SYMBOL', right_on ='tickerCode')

        #change some of the symbol to match the stock combine data set.
        self.sgx_curr_plus_company_df = self.sgx_curr_plus_company_df.rename(columns={'peRatio':'PERATIO',
                                                                                 'priceToBookRatio':'PRICEBOOK',
                                                                                 'dividendYield':'TRAILINGANNUALDIVIDENDYIELDINPERCENT',
                                                                                 'volume':'AVERAGEDAILYVOLUME',
                                                                                 'OpenPrice':'OPEN',
                                                                                 'totalDebtEquity':'TotalDebtEquity',
                                                                                })
        
    def joined_relevent_sgx_data(self):
        """ Join the company info to the company announcement and sgx ex div data.
            In some way, this limit the information to the stock list presented in sgx screener.
            
            Joining based on the company name. As the company name might not be totally similar,
            utilize the difflib to try to match the company name.
            The matching only take place if the matching have high confidence.
        
            Would need the company info  and the announcement.(with this as key table)
            Also try to include the ex div
        """
        ## company info, all stock info from sgx screener
        stock_info_df = self.saved_parm_df_dict['company_info']

        ## only get the ticker/symbol info and the company name for joining.
        stockcom_info_df = stock_info_df[['tickerCode','companyName']]
        stockcom_info_df['companyName'] = stockcom_info_df['companyName'].apply(lambda x: x.encode().upper())
        stockcom_list = [n[0].encode().upper() for n in stock_info_df[['companyName']].values]
        
        ## sgx announcement news
        announc_df = self.saved_parm_df_dict['announcement']
        ex_date_df = self.saved_parm_df_dict['ex_div_data']

        def match_name(x):
            try:
                closest_match = difflib.get_close_matches(x, stockcom_list, n=1, cutoff = 0.85 )
            except:
                return None #return none if input is nan
            if len(closest_match) ==1:
                return closest_match[0]
            else:
                return None
            
        announc_df['companyName'] = announc_df['SecurityName'].apply(match_name)
        ex_date_df['companyName'] = ex_date_df['CompanyName'].apply(match_name)

        ## join back to get the symbol
        self.sgx_announ_df = pandas.merge(announc_df,stockcom_info_df, on = 'companyName')
        self.sgx_div_ex_date_df = pandas.merge(ex_date_df,stockcom_info_df, on = 'companyName')

    def filter_key_announcement(self, list_of_stock):
        """ key announcement and disclosure
            Pass in a series of symbol for checking.
            In the shortened version, the anntitle is shortened to 20 char
        """
        filtered_announc_df = self.sgx_announ_df[self.sgx_announ_df['tickerCode'].isin(list_of_stock)].dropna()

        ##shortened version
        sh_filtered_announc_df = filtered_announc_df[['SecurityName','CategoryName','AnnTitle']]
        sh_filtered_announc_df['AnnTitle'] = sh_filtered_announc_df['AnnTitle'].apply(lambda x: x[:20] )
        if len(sh_filtered_announc_df) <1:
            self.filtered_announ_shtver = ''
        else:
            self.filtered_announ_shtver = sh_filtered_announc_df.to_string()
        
        return filtered_announc_df

    def retrieve_curr_price_df(self):
        """ Separate the current price df and also rename the columns.

        """
        self.sgx_curr_price_df = self.saved_parm_df_dict['curr_price']
        self.sgx_curr_price_df.rename(columns={'B':'BuyPrice','BV':'BuyVolume',
                                               'C':'PriceChange','H':'DailyHigh',
                                               'L':'DailyLow','LT':'LastPrice',
                                               'NC':'SYMBOL','N':'CompanyName',
                                               'O':'OpenPrice','P':'PricePercentChange',
                                               'R':'SGXRemark','S':'SellPrice',
                                               'V':'Value','VL':'DailyVolume',
                                               'SV':'SellVolume',
                                                },inplace =True)
 
        for parameter in ['DailyVolume','BuyVolume','SellVolume']:
            self.sgx_curr_price_df[parameter] = self.sgx_curr_price_df[parameter] * 1000 #convert to actual shares

        ## new parameters
        self.sgx_curr_price_df['buy_to_sell_ratio'] = self.sgx_curr_price_df['BuyVolume'] / (self.sgx_curr_price_df['SellVolume']+1) #plus one to make it non zero
        

    def retrieve_shortsell_info(self):
        """ Retrieve the shortsell information.
            will form the url and retrieved the information using pandas to make into table.
            The function will set to self_shortsell_info_df.
            make it iterat over the days to get the latest data
        """
        for last_effective_date in range(7): 
            self.form_shortsell_url(last_effective_date)
            url = URL(self.shortsell_full_url)
            try:
                #see data is available for that current date
                url_data = url.download(timeout = 50)
                shortsell_list = pandas.io.html.read_html(url_data)
                self.shortsell_info_df =shortsell_list[1]
            except:
                continue

            #continue if there is no data
            if len(self.shortsell_info_df) == 0: continue

            self.shortsell_info_df.rename(columns={0:'Security',1:'Short Sale Volume',
                                                  2:'Currency',3:'Short Sale Value',
                                                    },inplace =True)
            self.shortsell_info_df = self.shortsell_info_df[1:-3]
            #change type of the columns
            self.shortsell_info_df[['Short Sale Volume', 'Short Sale Value']] = self.shortsell_info_df[['Short Sale Volume', 'Short Sale Value']].astype(float)
            #need a rank on the short sell
            self.shortsell_info_df['ranked_shortsell'] = self.shortsell_info_df['Short Sale Volume'].rank(method='min',ascending=False)
            self.shortsell_info_df['shortsell_lastdate'] = self.set_last_desired_date(last_effective_date)
            #need percentage as well

            # have a sorting of data?
            return
        
        print 'No suitable data found within time frame.'
        return
        
    def form_shortsell_url(self, last_effective_date):
        """ Based on the current date to set the shorsell url.
            Set to self.shortsell_full_url
            Args:
                last_effective_date (int): last desired date in yyyymmdd.
        """
        #retrieve the current date in yyyymmdd format
        self.shortsell_date_url = self.set_last_desired_date(num_days = last_effective_date)
        self.shortsell_full_url = self.shortsell_info_start_url + self.shortsell_date_url + self.shortsell_end_url
        
    def set_last_desired_date(self, num_days = 0):
        """ Return the last date in which the results will be displayed.
            It is set to be the current date - num of days as set by users.
            Affect only self.print_feeds function.
            Kwargs:
                num_days (int): num of days prior to the current date.
                Setting to 0 will only retrieve the current date
            Returns:
                (int): datekey as yyyyymmdd.
        """
        last_eff_date_list = list((datetime.date.today() - datetime.timedelta(num_days)).timetuple()[0:3])

        if len(str(last_eff_date_list[1])) == 1:
            last_eff_date_list[1] = '0' + str(last_eff_date_list[1])

        if len(str(last_eff_date_list[2])) == 1:
            last_eff_date_list[2] = '0' + str(last_eff_date_list[2])

        return str(last_eff_date_list[0]) + str(last_eff_date_list[1]) + str(last_eff_date_list[2])

    def shortsell_notification(self):
        """ Use for alerts on shortsell information.
            Identify top ten short sell plus target stock short sell information.

        """
        ## get the current price df so can combined with the shortsell info
        self.process_all_data()
        merged_shortsell_df = pandas.merge(self.shortsell_info_df,self.sgx_curr_price_df,left_on = 'Security', right_on = 'CompanyName' )

        ## add in additional columns
        merged_shortsell_df['shortsell_vol_per'] = merged_shortsell_df['Short Sale Volume']/merged_shortsell_df['DailyVolume']
        merged_shortsell_df['ranked_percent_vol_shortsell'] = merged_shortsell_df['shortsell_vol_per'].rank(method='min',ascending=False)
        
        top_shortsell_df = merged_shortsell_df[merged_shortsell_df['ranked_shortsell'].isin(range(1,16))]
        top_shortsell_df  = top_shortsell_df.sort(columns = 'ranked_shortsell', ascending =True)
        top_shortsell_df = top_shortsell_df[['Security','Short Sale Volume','shortsell_lastdate']]
        shortsell_top15_shtver = top_shortsell_df.to_string()

        api_key_path = r'C:\Users\356039\Desktop\running bat\pushbullet_api\key.txt'
        with open(api_key_path,'r') as f:
            apiKey = f.read()

        p = PushBullet(apiKey)

        if shortsell_top15_shtver:
            p.pushNote('all', 'Shortsell top10', shortsell_top15_shtver,recipient_type="random1")

        ## display for target watchlist
        tar_watchlist_shortsell_df = merged_shortsell_df[merged_shortsell_df['Security'].isin(self.companyname_watchlist)]
        tar_watchlist_shortsell_df = tar_watchlist_shortsell_df[['Security','Short Sale Volume','ranked_shortsell','shortsell_vol_per','ranked_percent_vol_shortsell']]
        tar_watchlist_shortsell_df =tar_watchlist_shortsell_df[tar_watchlist_shortsell_df['ranked_shortsell'].isin(range(1,100))]
        tar_watchlist_shortsell_df  = tar_watchlist_shortsell_df.sort(columns = 'ranked_shortsell', ascending =True)
        tar_watchlist_shortsell_shtver = tar_watchlist_shortsell_df.to_string()

        if tar_watchlist_shortsell_shtver:
            p.pushNote('all', 'Shortsell targetwatchlist', tar_watchlist_shortsell_shtver,recipient_type="random1")
        

    def scan_price_limit_alert(self):
        """ Monitor the most current price and match it to the limit to watch.
            Watch will based on higher than and lower than price.
            
        """
        self.price_limit_alerts_df = pandas.DataFrame()
        
        #first scan
        for stockname, price_target, criteria in self.price_limit_reach_watchlist:
            try:
                target_df = self.sgx_curr_price_df[self.sgx_curr_price_df['SYMBOL'] == stockname]
            except:
                print 'problem with target stock: ', stockname
                continue
            target_df = target_df[['SYMBOL','CompanyName','LastPrice']]

            #reset whether to store or not store the result
            store_the_result = 0
            
            #cater for upper and lower criteria
            if criteria == 'greater':
                if target_df['LastPrice'].values[0] >=  price_target: #assume only one data returned
                    store_the_result = 1
            elif criteria == 'lower':
                if (target_df['LastPrice'].values[0] > 0) and (target_df['LastPrice'].values[0] <=  price_target): #assume only one data returned
                    store_the_result = 1                    

            if store_the_result == 1:
                if len(self.price_limit_alerts_df ) <1:
                    self.price_limit_alerts_df  = target_df
                else:
                    self.price_limit_alerts_df  = pandas.concat([self.price_limit_alerts_df ,target_df])

    def set_stock_to_watchlist(self,stocklist, watchlist_type = ''):
        """ Set the different stock symbol to watchlist. Note that the SYMBOL do not need to have .SI.
            Args:
                stocklist (list): list of stock symbol
            Kwargs:
                watchlist_type (str): either curr_price, announcement

        """
        if watchlist_type == 'curr_price':
            self.price_limit_reach_watchlist = stocklist
        elif watchlist_type == 'announcement':
            self.announce_watchlist = stocklist
        else:
            print 'incorrect item selected'
            print 'pls choose from: curr_price, announcement'
            raise

    def retrieve_and_notify(self):
        """ Send notification of the required data. Required pushbullet account.

        """
        self.process_all_data()
        self.joined_relevent_sgx_data()
        print 'list of company going ex date in 7 days'
        self.get_company_going_ex_date(7)

        #make this to user defined target.
        #cannot remove as depend on scheduling
        # hence self.announce_watchlist will not take effect.
        #get from excel
##        target_stock_list = ['E28','564','C2PU','S6NU','P07', 'SV3U',
##             '573','544','P40U',
##             'P13','S19','P07','E02', 'BN4','BS6','U96','J69U','S05', 'AGS',
##             'N4E','AJBU','T8JU','O23','T12',
##             'OV8','500', 'SV3U']

        print 'target announcmenet'
        self.filter_key_announcement(self.announce_watchlist)

        from pyPushBullet.pushbullet import PushBullet

        api_key_path = r'C:\Users\356039\Desktop\running bat\pushbullet_api\key.txt'
        with open(api_key_path,'r') as f:
            apiKey = f.read()

        p = PushBullet(apiKey)
        # to send all- change the recipent_type to arbitiary text
        p.pushNote('all', 'Ex Date Info', self.div_ex_date_shtver,recipient_type="random1")

        if self.filtered_announ_shtver:
            p.pushNote('all', 'Announcement Info', self.filtered_announ_shtver,recipient_type="random1")

        ## for price set alert
        self.retrieve_curr_price_df()
        self.scan_price_limit_alert()
        if len(self.price_limit_alerts_df)>0:
            p.pushNote('all', 'Price alert', self.price_limit_alerts_df.to_string(),recipient_type="random1")

def price_stream_alert():
    """ Function for streaming the price 

    """
    w = SGXDataExtract()
    w.process_all_data()

    #make this to excel table for easier
    #group together 
    price_limit_reach_watchlist = [['OV8',0.83, 'greater'],['OV8',0.78, 'lower'],
                                   ['P13',0.19, 'lower'], ['O23',1.65, 'lower'],
                                    ['C2PU',2.20, 'lower'], ['U96',4.55, 'lower'],
                                   ['BS6',1.46, 'greater'],  ['U96',4.42, 'lower'],
                                    ['BN4',9.60, 'greater'], ['S05',0.9, 'greater'],
                                   ['N4E',0.355, 'greater'], 
                                   ['BS6',1.35, 'lower'], ['U96',4.35, 'lower'],
                                   ['T12',0.341, 'lower'],
                                   ['BS6',1.39, 'lower'],['AGS',0.85, 'greater'],
                                   ['N4E',0.33, 'lower'], 
                                   ['B2F',3.65, 'greater'],['S63',3.48, 'lower'],
                                   ['AGS',0.819, 'greater'],['B2F',3.4, 'lower'],
                                   ['J69U',2.02, 'lower'],['S63',3.75, 'lower'],
                                   ['C22',1.1, 'lower'],
                                    ]
    w.set_stock_to_watchlist(price_limit_reach_watchlist, watchlist_type = 'curr_price')

    api_key_path = r'C:\Users\356039\Desktop\running bat\pushbullet_api\key.txt'
    with open(api_key_path,'r') as f:
        apiKey = f.read()

    p = PushBullet(apiKey)

    ## for price set alert
    w.scan_price_limit_alert()

    if len(w.price_limit_alerts_df)>0:
        p.pushNote('all', 'Price alert', w.price_limit_alerts_df.to_string(),recipient_type="random1")
        
    print 'completed'


if __name__ == '__main__':

    choice  = 5

    if choice ==1:

        price_stream_alert()
        sys.exit()

        w = SGXDataExtract()
        w.process_all_data()
        w.retrieve_curr_price_df()
        w.scan_price_limit_alert()

        w.joined_relevent_sgx_data()
        print 'list of company going ex date in 7 days'
        print w.get_company_going_ex_date(7)
        print

        print 'target announcmenet'

        announ_target_stock_list = ['E28','564','C2PU','S6NU','P07', 'SV3U',
                                     '573','544','P40U',
                                     'P13','S19','P07','E02', 'BN4','BS6','U96','J69U','S05', 'AGS',
                                     'N4E','AJBU','T8JU',
                                     'OV8','500', 'SV3U', 'O23','T12'
                                    ]
        
        w.set_stock_to_watchlist(announ_target_stock_list, watchlist_type = 'announcement')
        print w.filter_key_announcement(announ_target_stock_list)
        
    if choice ==2:
        with open(r'c:\data\temptryyql.json','r') as f:
            data = f.read()
        #write back teh data
        with open(r'c:\data\temptryyql.json','w') as f:
            data_str = data[4:]
            f.write(data_str)

    if choice == 3:
        #w  = json.load(open(r'c:\data\temptryyql.json', 'r'))
        with open(r'c:\data\temptryyql.json', 'r') as f:
            raw_data = f.read()
##        replacer = re.compile("(\w+):")
##        modified_data = replacer.sub(r'"\1":', raw_data)
##
##        replacer2 = re.compile("\'")
##        modified_data = replacer2.sub(r'"', modified_data)

        ## get rid of the label
        #modified_data = re.sub('"label":(.*M)",','"label":1',modified_data)#not working. has to specify number of character
        #modified_data = re.sub('"label":.*:[0-9][0-9]\s(A\P)M",','"label":1',raw_data)
##        with open(r'c:\data\temptryyql.json', 'w') as f:
##            f.write(modified_data)

    #need also replace all the ' with "
            #remove the time instance which will cause the additional ""

    if choice  ==4:
        """ Create str for announcment.


        """
        w = SGXDataExtract()

        ## set the various stock list
        #set it to excel table extract??
        announ_target_stock_list = ['E28','564','C2PU','S6NU','P07', 'SV3U',
                                     '573','544','P40U',
                                     'P13','S19','P07','E02', 'BN4','BS6','U96','J69U','S05', 'AGS',
                                     'N4E','AJBU','T8JU',
                                     'OV8','500', 'SV3U']
        
        w.set_stock_to_watchlist(announ_target_stock_list, watchlist_type = 'announcement')


        price_limit_reach_watchlist = [['OV8',0.83, 'greater'],  ['P13',0.19, 'lower'],
                                       ['573',0.50, 'lower'],  ['C2PU',2.20, 'lower'],
                                       ['BS6',1.34, 'greater'],  ['U96',4.15, 'lower'],
                                       ['AGS',0.799, 'greater'],
                                        ]
        w.set_stock_to_watchlist(price_limit_reach_watchlist, watchlist_type = 'curr_price')
        

        w.retrieve_and_notify()

    if choice == 5:
        """ Handling current price

        """
        w = SGXDataExtract()
        #w.shortsell_notification()
        #print w.shortsell_info_df
        w.process_all_data()
        w.sgx_curr_price_df.to_csv(r'c:\data\temp.csv',index =False)

    if choice == 6:
        """ Use excel table to get the required stocks."""
        xls_set_class = XlsExtractor(fname = r'C:\data\stockselection_for_sgx.xls', sheetname= 'stockselection',
                                     param_start_key = 'stock//', param_end_key = 'stock_end//',
                                     header_key = 'header#2//', col_len = 2) 

        xls_set_class.open_excel_and_process_block_data()
        xls_set_class.data_label_list

    if choice ==7:
        """ use the price extract here to pull """


    if choice ==8:
        """ Get the short sell volume. Need to get the shortsell vs the volumne traded that day
            Url need change with the current date

        """
        target_url = 'http://sgx.com/wps/wcm/connect/sgx_en/home/market_info/short_sale/short_sale_daily/DailyShortSell20150417.txt'
        url = URL(target_url)
        url_data = url.download(timeout = 50)
        shortsell_list = pandas.io.html.read_html(url_data)
        shortsell_df =shortsell_list[1]
        #ned to remove the first ropws
        shortsell_df.rename(columns={0:'Security',1:'Short Sale Volume',
                                              2:'Currency',3:'Short Sale Value',
                                                },inplace =True)
        shortsell_df = shortsell_df[1:-3]

    if choice == 9:
        """ combine the shortsell with the current price."""
        w = SGXDataExtract()
        w.process_all_data()
        w.joined_relevent_sgx_data()
        w.retrieve_curr_price_df() #formed the df
        w.retrieve_shortsell_info()

    if choice ==10:
        """ determine the sell volumne to buy volumn ratio"""
        

    
