"""
    Module: Yahoo finance historical data extractor
    Name:   Tan Kok Hua

    Notes:
        Each url get historical data of one stock.

    YF API from:
    https://code.google.com/p/yahoo-finance-managed/wiki/CSVAPI

    Updates:
        Dec 20 2014: Add in combined run.
                   : Add in pre 3rd year avg data
        Oct 30 2014: Enable shorten printing of error
        Oct 22 2014: Add in identify dividend quarter. 
        Oct 18 2014: Add in dividend retrieval
        Oct 11 2014: Resolve bug where there is less than 3 entites for day trends.
                   : Able to switch on and off printint.
                   : Resolve cases where there is problem with url download.
        Oct 09 2014: Add in monitor 3 days trends.
        Oct 08 2014: Able to save temp file and individual data (option)
                   : Able to save all data dataframe for further processing.
        Sep 16 2014: Enable multiple stocks data extract


    Learning:
        pandas get moving average
        http://www.bearrelroll.com/2013/07/python-pandas-moving-average/

        take top n data for each group --pandas
        http://stackoverflow.com/questions/20069009/pandas-good-approach-to-get-top-n-records-within-each-group

        pandas and financial data analysis
        http://nbviewer.ipython.org/github/twiecki/financial-analysis-python-tutorial/blob/master/2.%20Pandas%20replication%20of%20Google%20Trends%20paper.ipynb

        pandas tutorial
        http://nbviewer.ipython.org/gist/fonnesbeck/5850413

        scipy and pandas regression
        http://stackoverflow.com/questions/14775068/how-to-apply-linregress-in-pandas-bygroup
        http://stackoverflow.com/questions/19991445/run-an-ols-regression-with-pandas-data-frame

        print without getting a new line
        http://stackoverflow.com/questions/4499073/printing-without-newline-print-a-prints-a-space-how-to-remove

        get dividend url
        real-chart.finance.yahoo.com/table.csv?s=558.SI&a=04&b=25&c=2001&d=09&e=18&f=2014&g=v&ignore=.csv
        differences in the back where the g = v instead of d
        interval change to v instead of d,m,y

    TODO:
        if need the linear regression, need to convert date
        detect large jump in volume --get average volumne??
        year on year gain
        store the data and get from local storage??
        Add in the symbol for each stock --> have a function called generate all data .csv

        will put all the data to sql?? design the arhitecture for thsi

        need to be able to extract from database and select date only within selected range.

"""

import os, re, sys, time, datetime, copy, shutil
import pandas
from scipy.stats import linregress
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
        self.all_stock_sym_list = ['S58.SI','S68.SI'] ##special character need to be converted
        self.individual_stock_sym = '' #full range fo stocks
        self.date_interval = 10 # the number of dates to retrieve, temp  default to 1 day per interval
        self.bypass_data_downloading = 0 # bypass the downloading of data --> enable calling from database
##        self.get_data_fr_database = 0 # if 1, will get data from database.
##
##        # Database parametes
##        self.hist_database_path = r'C:\data\stock_sql_db\stock_hist.db'
                                                
        # URL forming 
        self.hist_quotes_start_url = "http://ichart.yahoo.com/table.csv?s="
        self.hist_quotes_stock_portion_url = ''
        self.hist_quotes_date_interval_portion_url = ''
        self.hist_quotes_date_dividend_portion_url = '' # dividend part (combined with the interval portion)
        self.hist_quotes_end_url = "&ignore=.csv"
        self.hist_quotes_full_url = ''
        self.div_history_full_url = ''

        # Output storage
        self.hist_quotes_df = object()
        self.enable_save_raw_file = 1 # 1 - will save all the indivdual raw data
        self.hist_quotes_csvfile_path = r'c:\data\raw_stock_data' # for storing of all stock raw data
        self.tempfile_sav_location = r'c:\data\temp\temp_hist_div_data_save.csv'
        self.all_stock_df = []# to trick it as len 0 item
        self.all_stock_div_hist_df = []
        self.all_stock_consolidated_div_df = []
        self.all_stock_combined_post_data_df = []

        # Trend processing.
        self.processed_data_df = object()
        self.processed_interval = 3 #set the period in which to process the post processing. For the filter most recert stock data.
        self.price_trend_data_by_stock = object() # provide the information. one line per stock
        ## take the adjusted close of the data

        # Print options
        self.print_current_processed_stock = 0 # if 1 -enable printing.

        # Fault check
        self.download_fault = 0

        ## print
        self.__print_download_fault = 0 # if 1, print statement on download problem.

    def set_bypass_data_download(self):
        """ Set the bypass of data download"""
        self.bypass_data_downloading = 1

    def set_raw_dataset(self, hist_price_df, hist_div_df):
        """ Set the raw data set. Use in cases where the data loading is bypass and just need data processing.
            
        """
        self.all_stock_df = hist_price_df
        self.all_stock_div_hist_df = hist_div_df 

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
        dividend_str = '&g=v'

        self.hist_quotes_date_interval_portion_url = from_date_url_str + end_date_url_str + interval_str
        self.hist_quotes_date_dividend_portion_url = from_date_url_str + end_date_url_str + dividend_str

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

        self.div_history_full_url = self.hist_quotes_start_url + self.hist_quotes_stock_portion_url +\
                                        self.hist_quotes_date_dividend_portion_url \
                                        + self.hist_quotes_end_url
             
    def downloading_csv(self, download_type = 'hist'):
        """ Download the csv information for particular stock.
            download_type can be hist or div. If hist, will download the hist price.
            If div, will download dividend history.
            Kwargs:
                download_type (str): hist or div (default hist).
        """
        self.download_fault = 0

        if download_type == 'hist':
            target_url = self.hist_quotes_full_url
            sav_filename = os.path.join(self.hist_quotes_csvfile_path,'hist_stock_price_'+ self.individual_stock_sym+ '.csv')
        elif download_type == 'div':
            target_url = self.div_history_full_url
            sav_filename = os.path.join(self.hist_quotes_csvfile_path,'div_hist_'+ self.individual_stock_sym+ '.csv')
        else:
            print 'wrong download type'
            raise

        url = URL(target_url)
        f = open(self.tempfile_sav_location, 'wb') # save as test.gif
        try:
            f.write(url.download())#if have problem skip
        except:
            if self.__print_download_fault: print 'Problem with processing this data: ', target_url
            self.download_fault =1
        f.close()

        if not self.download_fault:
            if self.enable_save_raw_file:
                shutil.copyfile(self.tempfile_sav_location,sav_filename )

    def save_stockdata_to_df(self, download_type = 'hist'):
        """ Create dataframe for the results.
            Achieved by reading the .csv file and retrieving the results using pandas.
            Will save separately to self.all_stock_df or self.all_stock_div_hist_df depending on the dowload type
            Kwargs:
                download_type (str): hist or div (default hist).
        """
        self.hist_quotes_individual_df = pandas.read_csv(self.tempfile_sav_location)
        self.hist_quotes_individual_df['SYMBOL'] = self.individual_stock_sym

        if download_type == 'hist':
            if len(self.all_stock_df) == 0:
                self.all_stock_df = self.hist_quotes_individual_df
            else:
                self.all_stock_df = self.all_stock_df.append(self.hist_quotes_individual_df)

            ## Include additional parameters in self.all_stock_df
            self.breakdown_date_in_stock_df() 
                
        elif download_type == 'div':
            if len(self.all_stock_div_hist_df) == 0:
                self.all_stock_div_hist_df = self.hist_quotes_individual_df
            else:
                self.all_stock_div_hist_df = self.all_stock_div_hist_df.append(self.hist_quotes_individual_df)

    def breakdown_date_in_stock_df(self):
        """ Add in additional paramters such as Date breakdown to append to self.all_stock_df.
        """
        self.all_stock_df['Year'] = self.all_stock_df['Date'].map(lambda x: int(x[:4]))
        
    def get_hist_data_of_all_target_stocks(self):
        """ Combine the cur quotes function.
            Formed the url, download the csv, put in the header. Have a dataframe object.
            Will get both the hist price and the div data
        """
        print "Getting historical data plus dividend data for each stock."
        print "Will run twice: one for historical data, the other for dividend"
        for stock in self.all_stock_sym_list:
            if self.print_current_processed_stock: print 'Processing stock: ', stock
            self.set_stock_to_retrieve(stock)
            self.form_url_str()
            if self.print_current_processed_stock: print self.hist_quotes_full_url
            
            ## get the hist data
            self.downloading_csv(download_type = 'hist')
            if not self.download_fault:
                self.save_stockdata_to_df(download_type = 'hist')
            ## get the div data
            self.downloading_csv(download_type = 'div')
            if not self.download_fault:
                self.save_stockdata_to_df(download_type = 'div')
                sys.stdout.write('.')
            else:
                sys.stdout.write('E:%s'%stock)
        print 'Done\n'

    ## methods for postprocessing data set -- hist data
    def removed_zero_vol_fr_dataset(self):
        """ Remove any stocks data that have volume = 0. Meaning no transaction during that day
            Set to self.processed_data_df. 
        """
        self.processed_data_df = self.all_stock_df[~(self.all_stock_df['Volume'] == 0)]

    def filter_most_recent_stock_data(self):
        """ Filter the most recent stock info. Target based on 3 days. (self.processed_interval)
            Number of days must be less than date_interval (also take note no trades on holiday and weekend.
            Set to self.processed_data_df. Also modified from self.processed_data_df

        """
        assert self.processed_interval <= self.date_interval
        self.processed_data_df  = self.processed_data_df.groupby("SYMBOL").head(self.processed_interval)

    def stock_with_at_least_3_entries(self, raw_data_df):
        """ Return list of Symbol that has at least 3 entries (for 3 days trends).
            Args:
                raw_data_df (Dataframe object): containing all the stocks raw data.
        """
        grouped_data = raw_data_df.groupby('SYMBOL').count()
        return list(grouped_data[grouped_data['Adj Close'] ==3].reset_index()['SYMBOL'])

    def get_trend_of_last_3_days(self):
        """ Get the trends based on each symbol whether it is constantly falling or rising.

        """
        self.processed_data_df = self.processed_data_df[self.processed_data_df['SYMBOL'].isin(self.stock_with_at_least_3_entries(self.processed_data_df))]
        grouped_symbol = self.processed_data_df.groupby("SYMBOL")
        falling_data=  (grouped_symbol.nth(2)['Adj Close']>= grouped_symbol.nth(1)['Adj Close']) &\
                        (grouped_symbol.nth(1)['Adj Close'] >= grouped_symbol.nth(0)['Adj Close']) &\
                         (~(grouped_symbol.nth(2)['Adj Close'] == grouped_symbol.nth(0)['Adj Close'])) #check flat

        falling_df = falling_data.to_frame().rename(columns = {'Adj Close':'Trend_3_days_drop'}).reset_index()

        rising_data =  (grouped_symbol.nth(0)['Adj Close']>= grouped_symbol.nth(1)['Adj Close']) &\
                        (grouped_symbol.nth(1)['Adj Close'] >= grouped_symbol.nth(2)['Adj Close']) &\
                        (~(grouped_symbol.nth(2)['Adj Close'] == grouped_symbol.nth(0)['Adj Close'])) #check flat
        rising_df = rising_data.to_frame().rename(columns = {'Adj Close':'Trend_3_days_rise'}).reset_index()
        
        self.price_trend_data_by_stock = pandas.merge(falling_df, rising_df, on = 'SYMBOL' )

    def get_trend_data(self):
        """ Consolidated methods to get the trend performance (now 3 working days data)

        """
        self.get_hist_data_of_all_target_stocks()
        self.removed_zero_vol_fr_dataset()
        self.filter_most_recent_stock_data()
        self.get_trend_of_last_3_days()

    ## YEar on Year gain.

    ## for processing dividend data
    def process_dividend_hist_data(self):
        """ Function for processing the dividend hist data

        """
        self.insert_yr_mth_col_to_div_df()
        self.insert_dividend_quarter()
        self.get_num_div_payout_per_year()
        self.get_dividend_payout_quarter_df()
        
    def insert_yr_mth_col_to_div_df(self):
        """ Insert the year and month of dividend to div df.
            Based on the self.all_stock_div_hist_df["Date"] to get the year and mth str.
            Set back to self.all_stock_div_hist_df
        """
        self.all_stock_div_hist_df['Div_year'] = self.all_stock_div_hist_df['Date'].map(lambda x: int(x[:4]))
        self.all_stock_div_hist_df['Div_mth'] = self.all_stock_div_hist_df['Date'].map(lambda x: int(x[6:7]))

    def insert_dividend_quarter(self):
        """ Insert the dividend quarter. Based on Calender year.
        """

        #combined all the div mth??
        self.all_stock_div_hist_df['Div_1stQuarter'] = self.all_stock_div_hist_df['Div_mth'].isin([1,2,3,])
        self.all_stock_div_hist_df['Div_2ntQuarter'] = self.all_stock_div_hist_df['Div_mth'].isin([4,5,6])
        self.all_stock_div_hist_df['Div_3rdQuarter'] = self.all_stock_div_hist_df['Div_mth'].isin([7,8,9])
        self.all_stock_div_hist_df['Div_4thQuarter'] = self.all_stock_div_hist_df['Div_mth'].isin([10,11,12])

    def get_dividend_payout_quarter_df(self):
        """ Get the dividend payout quarter for each stock.
            Based on curr year -1 as guage.
            Append to the self.all_stock_consolidated_div_df
        """
        curr_yr, curr_mth = self.get_cur_year_mth()
        target_div_hist_df = self.all_stock_div_hist_df[(self.all_stock_div_hist_df['Div_year']== curr_yr-1)]
        def check_availiable1(s):
            for n in s.values:
                if n == True:
                    return True
            return False
        target_div_hist_df = target_div_hist_df.groupby('SYMBOL').agg(check_availiable1).reset_index()[['SYMBOL','Div_1stQuarter','Div_2ntQuarter','Div_3rdQuarter','Div_4thQuarter' ]]
        self.all_stock_consolidated_div_df = pandas.merge(self.all_stock_consolidated_div_df,target_div_hist_df, on = 'SYMBOL', how = 'left')
        

    def get_cur_year_mth(self):
        """ Get the current year and mth.
            Returns:
                (int): Year in yyyy
                (int): mth in mm
        """
        now = datetime.datetime.now()
        return int(now.year), int(now.month)

    def get_num_div_payout_per_year(self):
        """ Get the number of div payout per year, group by symbol and year.
            Exclude the curr year information.
        """
        curr_yr, curr_mth = self.get_cur_year_mth()

        ## exclude the current year as dividend might not have pay out yet and keep within 4 years period
        target_div_hist_df = self.all_stock_div_hist_df[~(self.all_stock_div_hist_df['Div_year']== curr_yr)]
        target_div_hist_df = target_div_hist_df[target_div_hist_df['Div_year']>= curr_yr-4]

        ## get the div payout each year in terms of count
        div_cnt_df =  target_div_hist_df.groupby(['SYMBOL', 'Div_year']).agg("count").reset_index()
        div_payout_df = div_cnt_df.groupby('SYMBOL').agg('mean').reset_index()[['SYMBOL','Dividends']].rename(columns = {'Dividends':'NumDividendperYear'})

        ## get the number of years div pay for 4 year period --4 means every year.
        div_cnt_yr_basis_df = div_cnt_df.groupby('SYMBOL').agg('count').reset_index()[['SYMBOL','Div_year']].rename(columns = {'Div_year':'NumYearPayin4Yr'})

        ## join the data frame
        self.all_stock_consolidated_div_df = pandas.merge(div_payout_df,div_cnt_yr_basis_df, on = 'SYMBOL')

    ## year trends
    def get_prev_3rd_yr_df(self):
        """ Get the prev 3rd year avg data.
            Return:
                (Dateframe): results of prior 3rd year avg data
        """
        curr_yr, curr_mth = self.get_cur_year_mth()
        target_div_hist_df = self.all_stock_df[(self.all_stock_df['Year']== curr_yr-2)]
        prev_3rd_yr_df = target_div_hist_df.groupby('SYMBOL').agg('mean').reset_index()[['SYMBOL','Adj Close']].rename(columns = {'Adj Close':'Pre3rdYear_avg'})
        return prev_3rd_yr_df

    ## combined run
    def run_all_hist_data(self):
        """ Run all post processed function."""

        if not self.bypass_data_downloading:
            self.get_hist_data_of_all_target_stocks()
        self.get_trend_data()
        self.process_dividend_hist_data()
        prev_3rd_yr_df = self.get_prev_3rd_yr_df()
        self.all_stock_combined_post_data_df = pandas.merge(self.price_trend_data_by_stock,\
                                                            self.all_stock_consolidated_div_df,\
                                                            on = 'SYMBOL', how ='left')
        self.all_stock_combined_post_data_df = pandas.merge(self.all_stock_combined_post_data_df,\
                                                    prev_3rd_yr_df,\
                                                    on = 'SYMBOL', how ='left')

if __name__ == '__main__':
    
    print "start processing"
    
    choice = 6

    if choice == 1:
        data_ext = YFHistDataExtr()
        data_ext.set_interval_to_retrieve(365*5)
        data_ext.set_multiple_stock_list(['OV8.SI','S58.SI'])
        data_ext.set_stock_to_retrieve('OV8.SI')
        data_ext.get_trend_data()
        
        print data_ext.processed_data_df        
        print data_ext.price_trend_data_by_stock

    if choice == 2:
        w = data_ext.processed_data_df
        grouped_symbol = w.groupby("SYMBOL")
        falling_data=  (grouped_symbol.nth(0)['Adj Close']>= grouped_symbol.nth(1)['Adj Close'])  >= grouped_symbol.nth(2)['Adj Close']

    if choice == 3:
        """Check for dividend --no dividend from histor day"""
        data_ext = YFHistDataExtr()
        data_ext.set_interval_to_retrieve(700)
        data_ext.set_multiple_stock_list(['S58.SI'])
        data_ext.get_hist_data_of_all_target_stocks()
        #data_ext.set_stock_to_retrieve('OV8.SI')

    if choice ==4:
        """Just for processing the dividend data"""
        curr_yr =2014
        curr_mth = 10
        div_df = data_ext.all_stock_div_hist_df
        div_df['Div_year'] = div_df['Date'].map(lambda x: int(x[:4]))
        div_df['Div_mth'] = div_df['Date'].map(lambda x: int(x[6:7]))
        target_div_hist_df = div_df[~(div_df['Div_year']== curr_yr)]
        
        div_cnt_df =  target_div_hist_df.groupby(['SYMBOL', 'Div_year']).agg("count").reset_index()
        print div_cnt_df.groupby('SYMBOL').agg('mean')

    if choice ==5:
        data_ext = YFHistDataExtr()
        data_ext.set_interval_to_retrieve(365*5)
        data_ext.set_multiple_stock_list(['OV8.SI','BN4.SI'])
        data_ext.run_all_hist_data()
        #print data_ext.all_stock_consolidated_div_df
        print data_ext.all_stock_combined_post_data_df

    if choice == 6:
        """ Try disable downloading data."""
        """ Use in conjunction with the get datafrom database"""

        data_ext = YFHistDataExtr()
        data_ext.set_bypass_data_download()
        data_ext.set_raw_dataset(f.hist_price_df, f.hist_div_df)
        data_ext.run_all_hist_data()
        #print data_ext.all_stock_consolidated_div_df
        print data_ext.all_stock_combined_post_data_df








