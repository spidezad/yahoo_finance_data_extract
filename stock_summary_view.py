"""
    Create a view of stock by getting the important columns.

    Need historical high price, highest earning per share. warning symbol

    Summary view also contain feeds that have the company names

    or use google with date for more data

    target stock check

    Map out the alert system.
    price target
    limit stock check --> excel file, based on symbol?? stock name

    set the date as another module.

    
"""

import os, sys, re, datetime
import pandas


## Using gui to select the targeted stocks.
def select_stock_fr_gui(self):
    """ Using gui to quickly selct the target stocks.
        include find? and sort 

    """
    import pyET_tools.easygui as gui

## Handling date
def set_last_desired_date( num_days = 0):
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

def get_filename(dir_path, filename_prefix, offset_to_cur_date = 0, file_ext = '.csv'):
    """ Generate the filename based on current date.
        Args:
            dir_path (str): full dir path
            filename_prefix (str): filename prefix before the date
            offset_to_cur_date (int): num of days offset to current
        Kwargs:
            file_ext (str): extension of the file.
        Return:
            (str): file path
    """
    return os.path.join(dir_path, filename_prefix + set_last_desired_date(offset_to_cur_date)+ file_ext )

def is_current_date_file_exists(dir_path, filename_prefix, file_ext = '.csv'):
    """ Scan for current date file.        
        Args:
            dir_path (str): full dir path
            filename_prefix (str): filename prefix before the date
        Kwargs:
            file_ext (str): extension of the file.
        Return:
            (bool): whether file exists
    """
    return os.path.isfile(get_filename(dir_path, filename_prefix, file_ext)) 


if __name__ == "__main__":

    choice = [1]

    ## able to set the previous day if requird.

    ## need to include EPS
    target_file = get_filename(r'c:\data\compile_stockdata','full_') #need to scan teh file with latest date
    target_df = pandas.read_csv(target_file)

    target_columns = ['SYMBOL', 'NAME','OPEN','DILUTEDEPS','PERATIO','PRICEBOOK',
                      'TRAILINGANNUALDIVIDENDYIELDINPERCENT','NumDividendperYear',
                      'NumYearPayin4Yr','Pre3rdYear_avg','ReturnonEquity','TotalDebtEquity',
                      'Above_Boll_upper','Below_Boll_lower','price_above_50dexm','20dexm_above_50dexm',
                      'Industry','Sector']

    target_columns = ['SYMBOL', 'CompanyName','OPEN','eps','PERATIO','PRICEBOOK',
                      'TRAILINGANNUALDIVIDENDYIELDINPERCENT','NumDividendperYear',
                      'NumYearPayin4Yr','Pre3rdYear_avg','returnOnEquity','TotalDebtEquity','Operating Cash Flow (ttm)','Levered Free Cash Flow (ttm)',
                      'Above_Boll_upper','Below_Boll_lower','price_above_50dexm','20dexm_above_50dexm',
                      'industry','industryGroup']

    if 1 in choice:
        print 'start'
        
        stocklist = ['E28.SI','564.SI','C2PU.SI','S6NU.SI','P07.SI', 'SV3U.SI',
                     '573.SI','544.SI','P40U.SI',
                     'P13.SI','S19.SI','P07.SI','E02.SI']

        stocklist2 = ['BN4.SI','BS6.SI','U96.SI','J69U.SI','S05.SI', 'AGS.SI',
                     'N4E.SI','AJBU.SI','T8JU.SI',
                     'OV8.SI','500.SI', 'SV3U.SI']
        stocklist3 = ['IX2.SI','S49.SI','U13.SI','5OI.SI','P19.SI','N01.SI','V03.SI','5ER.SI','D6U.SI','NO4.SI']

        stocklist4 = ['BN2.SI','D38.SI','544.SI','M01.SI','W05.SI','N08.SI','5CH.SI','M26.SI','KF4.SI','5ER.SI','NO4.SI','D6U.SI','5OI.SI','N01.SI','P19.SI','IX2.SI','S49.SI','U13.SI','V03.SI']
        
        w=  target_df[target_df['SYMBOL'].isin(stocklist4)][target_columns]

        print w

        ##need to save to some file
        w.to_csv(r'c:\data\temp\view_stock.csv')


    if 2 in choice:
        """ stock alert
            Cases where there is sudden spike in volumne or the Bollinger band crosses.
            store the previous data so if any new data will update
            ## it would depend on the target file... might need to run it interactive.y.

            Scan for today dte file so that it will not keep using back the previous data.
        """
        stored_dir = r'C:\data\stock_alert'

        new_target_columns = target_columns + ['Avg_volume_above70per','VOLUME','Avg_volume_200d']

        # volumne spike
        target_file = 'vol_spike.csv'
        print 'for volumne spike'
        w=  target_df[(target_df['Avg_volume_above70per']== True)][new_target_columns]
        sliced_df = w[['SYMBOL','NAME','OPEN']]
        print sliced_df

        # take out the old choice and write in the new file??
        try:
            old_data_df = pandas.read_csv(os.path.join(stored_dir,target_file))
            old_symbol_list = list(old_data_df['SYMBOL'])
            print 'new stock entry'
            print sliced_df[~sliced_df['SYMBOL'].isin(old_symbol_list)]
        except:
            print 'file not exist'

        #overwrite the old file
        sliced_df.to_csv(os.path.join(stored_dir,target_file), index = False)
        
        sys.exit()
        
        # price drop 3 years low --> or heck the bollinger bands 
        print 'for price dropping below 3 yrs low'
        new_target_columns = target_columns 
        w=  target_df[(target_df['OPEN']< target_df['Pre3rdYear_avg'])][new_target_columns]
        w['below_3yr_avg'] = (target_df['OPEN']< target_df['Pre3rdYear_avg'])
        print w[['SYMBOL','NAME','OPEN']]

        ##need to save to some file
        w.to_csv(r'c:\data\temp\view_stock.csv')

        #need to get those that ex date tommorrow or day after tomorrow. or group by week.like ex date next week.








