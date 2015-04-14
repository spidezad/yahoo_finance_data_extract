"""
    Get the tech analysis of the particular stocks. Data retrieve from yahoo finance.
    
Required Modules:
    Pandas
    yahoo_finance_historical_data_extract
    matplotlib (plotting)

Updates:
    Mar 06 2015: Add in average volume and check volume spike
    Feb 23 2015: enable database data getting and from web pulling.
    Feb 22 2015: Shift the analysis execution to function
    Feb 07 2015: Update GUI for stock plot selection.

Learning:
    candlestick plots
    http://stackoverflow.com/questions/19580116/plotting-candlestick-data-from-a-dataframe-in-python

    bollingerbands
    https://github.com/arvindevo/MachineLearningForTrading/blob/master/bollingerbands.py

    bollinger bandwidth
    http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:bollinger_band_width

    Tech analysis library
    http://ta-lib.org/
    
    adjclose = adjclose.fillna(method='backfill') --> backfill??
    
    MACD derivation
    First derivative: 12-day EMA and 26-day EMA
    Second derivative: MACD (12-day EMA less the 26-day EMA)
    Third derivative: MACD signal line (9-day EMA of MACD)
    Fourth derivative: MACD-Histogram (MACD less MACD signal line)

    yahoo charts download
    https://code.google.com/p/yahoo-finance-managed/wiki/miscapiImageDownload

To do:
    may need to clean some of the data first. --> remove all those volume is zero
    make it such that every script can run on its own and combined with main data.
    May add montage with mutltiple plots.

    Get the data from database.

    Allow passing of the data. check the number of days in which to get.

    check sudden spike in volumen --> get volumne average

    Add mopre indicator

    Shift the data retrieval to method. Allow two types of data retreival, from database or from direct date retrival

    May need to change the raw data output.

    bugs: got problem extracting from databaser --> cannot fix the date

    need to remove some of the columns such as date price etc

    the average percentage drop


"""
import os, re, sys, time, datetime, copy, shutil
import pandas
from yahoo_finance_historical_data_extract import YFHistDataExtr
from hist_data_storage import FinanceDataStore
import matplotlib.pyplot as plt

try:
    from pyET_tools import easygui
except:
    print 'Unable to use the GUI function.'


class TechAnalysisAdd(object):
    def __init__(self,stocklist):
        """ Intialize hist data extr.
            Set the stocklist and get all hist data.
            Args:
                stocklist (list): list of stock symbol. For Singapore stocks, best to include the .SI
        """
        ## stocklist
        self.stocklist = stocklist

        ## user parameters
        self.get_fr_database = 0 # 1 will pull data from database, 0 will get directly get from web.
        self.database_path = r'C:\data\stock_sql_db\stock_hist.db'

        ## Save parameters.
        self.raw_all_stock_df = pandas.DataFrame()
        self.histdata_indiv_stock = object() # for single stock handling.
        self.histdata_combined = pandas.DataFrame()
        self.processed_histdata_combined = object()
        
    def set_stocklist(self, stocklist):
        """ Method to set the stocklist for raw data pulling.
            Set to self.histdata_ext.
            Args:
                stocklist (list): list of stock symbol. For Singapore stocks, best to include the .SI
        """
        self.histdata_ext.set_multiple_stock_list(stocklist)

    def enable_pull_fr_database(self):
        """ Set the self.get_fr_database parameter so it will pull from database.
            set to self.get_fr_database.
        """
        self.get_fr_database = 1
    
    def retrieve_hist_data(self):
        """ Retrieve all the hist data.
        """
        if not self.get_fr_database:
            self.retrieve_hist_data_fr_web()
        else:
            self.retrieve_hist_data_fr_database()
        self.raw_all_stock_df['Date'] =  pandas.to_datetime( self.raw_all_stock_df['Date'])

    def retrieve_hist_data_fr_database(self):
        """ Retrieve the hist data from web using yahoo_finance_historical_data_extract module.
            Set to self.raw_all_stock_df

        """

        c = FinanceDataStore(self.database_path)
        c.retrieve_hist_data_fr_db(self.stocklist,0)
        c.extr_hist_price_by_date(200)
        self.raw_all_stock_df = c.hist_price_df

    def retrieve_hist_data_fr_web(self):
        """ Retrieve the hist data from web using yahoo_finance_historical_data_extract module.
            Set to self.raw_all_stock_df

        """
        self.histdata_ext = YFHistDataExtr()
        self.histdata_ext.set_interval_to_retrieve(200)
        self.set_stocklist(self.stocklist)
        self.histdata_ext.get_hist_data_of_all_target_stocks()
        self.raw_all_stock_df = self.histdata_ext.all_stock_df
        
    def add_Bollinger_parm(self):
        """ Add the list of Bolinger Parm.
            Set to self.histdata_indiv_stock.
        """

        temp_data_set = self.histdata_indiv_stock.sort('Date',ascending = True ) #sort to calculate the rolling mean

        temp_data_set['20d_ma'] = pandas.rolling_mean(temp_data_set['Adj Close'], window=20)
        temp_data_set['50d_ma'] = pandas.rolling_mean(temp_data_set['Adj Close'], window=50)
        temp_data_set['Bol_upper'] = pandas.rolling_mean(temp_data_set['Adj Close'], window=20) + 2* pandas.rolling_std(temp_data_set['Adj Close'], 20, min_periods=20)
        temp_data_set['Bol_lower'] = pandas.rolling_mean(temp_data_set['Adj Close'], window=20) - 2* pandas.rolling_std(temp_data_set['Adj Close'], 20, min_periods=20)
        temp_data_set['Bol_BW'] = ((temp_data_set['Bol_upper'] - temp_data_set['Bol_lower'])/temp_data_set['20d_ma'])*100
        temp_data_set['Bol_BW_200MA'] = pandas.rolling_mean(temp_data_set['Bol_BW'], window=50)#cant get the 200 daa
        temp_data_set['Bol_BW_200MA'] = temp_data_set['Bol_BW_200MA'].fillna(method='backfill')##?? ,may not be good
        temp_data_set['20d_exma'] = pandas.ewma(temp_data_set['Adj Close'], span=20)
        temp_data_set['50d_exma'] = pandas.ewma(temp_data_set['Adj Close'], span=50)
        
        self.histdata_indiv_stock = temp_data_set.sort('Date',ascending = False ) #revese back to original

    def add_MACD_parm(self):
        """ Include the MACD parm.
        """
        temp_data_set = self.histdata_indiv_stock.sort('Date',ascending = True )
        
        temp_data_set['12d_exma'] = pandas.ewma(temp_data_set['Adj Close'], span=12)
        temp_data_set['26d_exma'] = pandas.ewma(temp_data_set['Adj Close'], span=26)
        temp_data_set['MACD'] = temp_data_set['12d_exma'] - temp_data_set['26d_exma'] #12-26
        temp_data_set['MACD_signalline'] = pandas.rolling_mean(temp_data_set['MACD'], window=9)
        temp_data_set['MACD_hist'] = temp_data_set['MACD'] - temp_data_set['MACD_signalline']
        
        self.histdata_indiv_stock = temp_data_set.sort('Date',ascending = False ) #revese back to original

    def add_pivot_point(self):
        """ Getting the pivot pt based on the last entry. Same operation as other add function.
            Cater for one stock at a time. Assume the first entry is the latest date.
            The returning will be the self.histdata_indiv_stock with the addional parameter
        """
        temp = self.histdata_indiv_stock.head(2)[1:2]
        try:
            pivot_value = list((temp['High'] + temp['Low'] + temp['Adj Close'])/3)[0]
            R1_value = (pivot_value*2) - list(temp['Low'])[0]
            S1_value = (pivot_value*2) - list(temp['High'])[0]
        except:
            pivot_value = 0
            R1_value = 0
            S1_value = 0
        self.histdata_indiv_stock['Pivot'] = pivot_value
        self.histdata_indiv_stock['S1'] = S1_value
        self.histdata_indiv_stock['R1'] = R1_value

    def add_average_vol(self):
        """ Getting the average volume over 200 days.
        """
        temp_group = self.histdata_indiv_stock.groupby('SYMBOL')
        try:
            avg_volume = temp_group['Volume'].agg('mean').values[0]
        except:
            avg_volume = 0

        self.histdata_indiv_stock['Avg_volume_200d'] = avg_volume
        self.histdata_indiv_stock['Avg_volume_above30per'] = (self.histdata_indiv_stock['Volume']> 1.3*avg_volume)
        self.histdata_indiv_stock['Avg_volume_above70per'] = (self.histdata_indiv_stock['Volume']> 1.7*avg_volume)
        

    def add_analysis_parm(self):
        """ Add all the different analysis data to the data set.
        """
        self.histdata_combined = pandas.DataFrame()
        for symbol in self.stocklist:
            self.histdata_indiv_stock = self.raw_all_stock_df[self.raw_all_stock_df['SYMBOL']== symbol] 
            self.add_Bollinger_parm()
            self.add_MACD_parm()
            self.add_pivot_point()
            self.add_average_vol()
            if len(self.histdata_combined) == 0:
                self.histdata_combined = self.histdata_indiv_stock
            else:
                self.histdata_combined = self.histdata_combined.append(self.histdata_indiv_stock)

    def bollinger_plots(self):
        """ Plot the bollinger plots for each stocks.
            Used self.histdata_indiv_stock for data passing.
            Pivot is added to here

        """
        self.histdata_indiv_stock.plot(x='Date', y=['Adj Close','20d_ma','50d_ma','Bol_upper','Bol_lower','Pivot','S1','R1' ])
        self.histdata_indiv_stock.plot(x='Date', y=['Bol_BW','Bol_BW_200MA' ])
        plt.show()

    def MACD_plots(self):
        """ Plot the MACD plots for each stocks.
            Used self.histdata_indiv_stock for data passing.

        """
        self.histdata_indiv_stock.plot(x='Date', y=['MACD','MACD_signalline' ],ylim=[-0.3,0.3])
        #self.histdata_indiv_stock.plot(x='Date', y=['MACD_hist' ],kind='bar')#problem with this plot
        plt.show()

    def analysis_plot_for_tgt_sym(self, symbol):
        """ Get all analysis plot for target symbol.
            For each symbol, set to self.histdata_indiv_stock
            Args:
                symbol (str): stock symbol.
            Type of plots to include

        """
        self.histdata_indiv_stock = self.histdata_combined[self.histdata_combined['SYMBOL'] == symbol]
        self.bollinger_plots()
        self.MACD_plots()

    def get_most_current_dataset(self):
        """ Get the first (or most current) data for every stock.
            Additional parm --> cross the bollinger band
            cross the 50 days ex moving avg
            Bol width - Bol 200 (percentage)
            MACD hist (percentage)
            
        """
        self.processed_histdata_combined = self.histdata_combined.groupby("SYMBOL").first().reset_index()

    def add_response_trigger(self):
        """ Add series of basic trigger point for basic technical analysis.
        """
        tar_data = self.processed_histdata_combined
        tar_data['Above_Boll_upper'] = tar_data['Adj Close'] > tar_data['Bol_upper']
        tar_data['Below_Boll_lower'] = tar_data['Bol_lower'] > tar_data['Adj Close']
        tar_data['price_above_50dexm'] = tar_data['Adj Close'] > tar_data['50d_exma']
        tar_data['20dexm_above_50dexm'] = tar_data['20d_exma'] > tar_data['50d_exma']
        self.processed_histdata_combined = tar_data

    def display_avaliable_info_GUI(self):
        """ For plot display in GUI.

        """
        stock_choice = self.histdata_combined['SYMBOL'].drop_duplicates().tolist()
        while True:
            choice = easygui.choicebox(choices = stock_choice)
            if choice == None:
                return
            else:
                self.analysis_plot_for_tgt_sym(choice)
            

if __name__ == '__main__':
     
    print "start processing"
    
    choice = [4]

    stocklist = ['BN4.SI','BS6.SI','U96.SI','J69U.SI','S05.SI', 'AGS.SI',
             'N4E.SI','AJBU.SI','T8JU.SI',
             'OV8.SI','500.SI', 'SV3U.SI']
    
    if 4 in choice:
        """ Try the analysis class        """
        stocklist = ['BN4.SI','BS6.SI','U96.SI','J69U.SI','S05.SI', 'AGS.SI',
                     'N4E.SI','AJBU.SI','T8JU.SI',
                     'OV8.SI','500.SI', 'SV3U.SI', 'P13.SI', 'C2PU.SI','STX']
        w = TechAnalysisAdd(stocklist)
        w.retrieve_hist_data()
        w.add_analysis_parm()
        w.display_avaliable_info_GUI()

    if 5 in choice:
        """ download all related information for quick view particular when decide when to sell"""
        from yahoo_finance_data_extract import YFinanceDataExtr

        data_ext = YFinanceDataExtr()
        data_ext.set_stock_sym_append_str('')
        
        data_ext.set_full_stocklist_to_retrieve(stocklist)
    
        data_ext.get_cur_quotes_fr_list()
        t = data_ext.temp_full_data_df[['SYMBOL','NAME','OPEN','PERATIO','PRICEBOOK','DILUTEDEPS']]
        print t

    if 6 in choice:
        """ Determine the pivot point.
            Get last entry of every stock. Determine pivot by using adj close high low/3

        """
        data_ext = TechAnalysisAdd(['AGS.SI'])
        
        w = data_ext.histdata_combined.groupby("SYMBOL").last().reset_index()

    if 1 in choice:
        stocklist = ['BN4.SI','BS6.SI','U96.SI','J69U.SI','S05.SI', 'AGS.SI',
                     'N4E.SI','AJBU.SI','T8JU.SI',
                     'OV8.SI','500.SI', 'SV3U.SI']
        w = TechAnalysisAdd(stocklist)
        w.enable_pull_fr_database()
        w.retrieve_hist_data()
        w.add_analysis_parm()
        w.get_most_current_dataset()
        w.add_response_trigger()
        w.processed_histdata_combined.to_csv(r'c:\data\temp.csv', index =False)
            







