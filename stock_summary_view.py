"""
    Create a view of stock by getting the important columns.

    Need historical high price, highest earning per share. warning symbol

    Summary view also contain feeds that have the company names

    or use google with date for more data 

    
"""

import os, sys, time
import pandas
from General_feed_extract import FeedsReader


if __name__ == "__main__":

    choice = [2]

    target_file = r'C:\data\full_Mar06.csv'
    target_df = pandas.read_csv(target_file)

    target_columns = ['SYMBOL', 'NAME','OPEN','DILUTEDEPS','PERATIO','PRICEBOOK',
                      'TRAILINGANNUALDIVIDENDYIELDINPERCENT','NumDividendperYear',
                      'NumYearPayin4Yr','Pre3rdYear_avg','ReturnonEquity','TotalDebtEquity',
                      'Above_Boll_upper','Below_Boll_lower','price_above_50dexm','20dexm_above_50dexm',
                      'Industry','Sector']

    
    if 1 in choice:
        print 'start'
        
        stocklist = ['E28.SI','564.SI','C2PU.SI','S6NU.SI','P07.SI', 'SV3U.SI',
                     '573.SI','544.SI','P40U.SI',
                     'P13.SI','S19.SI','P07.SI','E02.SI']

        stocklist2 = ['BN4.SI','BS6.SI','U96.SI','J69U.SI','S05.SI', 'AGS.SI',
                     'N4E.SI','AJBU.SI','T8JU.SI',
                     'OV8.SI','500.SI', 'SV3U.SI']

        w=  target_df[target_df['SYMBOL'].isin(stocklist)][target_columns]

        print w

        ##need to save to some file
        w.to_csv(r'c:\data\temp\view_stock.csv')


    if 2 in choice:
        """ stock alert
            Cases where there is sudden spike in volumne or the Bollinger band crosses
            
        """
        new_target_columns = target_columns + ['Avg_volume_above70per','VOLUME','Avg_volume_200d']

        # volumne spike
        w=  target_df[(target_df['Avg_volume_above30per']== True)][new_target_columns]

        # price drop 3 years low.
        new_target_columns = target_columns 
        w=  target_df[(target_df['OPEN']< target_df['Pre3rdYear_avg'])][new_target_columns]
        w['below_3yr_avg'] = (target_df['OPEN']< target_df['Pre3rdYear_avg'])
        print w

        ##need to save to some file
        w.to_csv(r'c:\data\temp\view_stock.csv')

        #need to get those that ex date tommorrow or day after tomorrow. or group by week.like ex date next week.








