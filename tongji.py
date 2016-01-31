
from pyalgotrade.barfeed import yahoofeed
from pyalgotrade import dispatcher
from pyalgotrade import bar
from pyalgotrade.talibext import indicator
import os
import pandas as pd
import numpy as np
from datetime import datetime



#nasdaq
nasdaq_df = pd.read_csv("data/nasdaq.csv")
nasdaq_df = nasdaq_df[nasdaq_df.Industry != 'n/a']

#amex
amex_df = pd.read_csv("data/amex.csv")
amex_df = amex_df[amex_df.Industry != 'n/a']

#ntse

nyse_df = pd.read_csv("data/nyse.csv")
nyse_df = nyse_df[nyse_df.Industry != 'n/a']

stock_list_df = pd.concat([nasdaq_df,amex_df,nyse_df])
stock_list_df = stock_list_df.drop_duplicates('Symbol')
stock_list_df = stock_list_df.set_index("Symbol")

stock_list_df = stock_list_df.reindex(columns=list(stock_list_df.columns)[:-1]+['incr2015','incr2014','incr2013','incr3year','incr6year'])



filenames  =  os.listdir("result")

pd_dict = dict()

for filename in filenames:
    symbol = os.path.splitext(filename)[0]
    df = pd.read_csv("result/%s"%(filename))
    df = df.sort_values(by="Date")
    pd_dict[symbol] = df

for symbol,df in pd_dict.items():
    try:
        qoute_2015_df = df[(df['Date']>'2014-12-31') & (df['Date']<'2016-01-01')].reset_index()
        qoute_2014_df = df[(df['Date']>'2013-12-31') & (df['Date']<'2015-01-01')].reset_index()
        qoute_2013_df = df[(df['Date']>'2012-12-31') & (df['Date']<'2014-01-01')].reset_index()
        qoute_3year_df = df[(df['Date']>'2012-12-31') & (df['Date']<'2016-01-01')].reset_index()
        qoute_6year_df = df[(df['Date']>'2009-12-31') & (df['Date']<'2016-01-01')].reset_index()
        if not qoute_2015_df.empty:
            stock_list_df.loc[symbol,'incr2015'] = round((qoute_2015_df['Adj Close'][len(qoute_2015_df['Adj Close'])-1] - qoute_2015_df['Adj Close'][0])/qoute_2015_df['Adj Close'][0]*100,2)
        if not qoute_2014_df.empty:
            stock_list_df.loc[symbol,'incr2014'] = round((qoute_2014_df['Adj Close'][len(qoute_2014_df['Adj Close'])-1] - qoute_2014_df['Adj Close'][0])/qoute_2014_df['Adj Close'][0]*100,2)
        if not qoute_2013_df.empty:
            stock_list_df.loc[symbol,'incr2013'] = round((qoute_2013_df['Adj Close'][len(qoute_2013_df['Adj Close'])-1] - qoute_2013_df['Adj Close'][0])/qoute_2013_df['Adj Close'][0]*100,2)
        if not qoute_3year_df.empty:
            stock_list_df.loc[symbol,'incr3year'] = round((qoute_3year_df['Adj Close'][len(qoute_3year_df['Adj Close'])-1] - qoute_3year_df['Adj Close'][0])/qoute_3year_df['Adj Close'][0]*100,2)
        if not qoute_6year_df.empty:
            stock_list_df.loc[symbol,'incr6year'] = round((qoute_6year_df['Adj Close'][len(qoute_6year_df['Adj Close'])-1] - qoute_6year_df['Adj Close'][0])/qoute_6year_df['Adj Close'][0]*100,2)
    except:
        continue
stock_list_df = stock_list_df[stock_list_df['incr2015'].notnull()]
stock_list_df.to_csv("data/result.csv")
#barFeed.setUseAdjustedValues(True)
'''
dispatcher = dispatcher.Dispatcher()
dispatcher.addSubject(barFeed)
dispatcher.run()

for filename in filenames[0:100]:
    symbol = os.path.splitext(filename)[0]
    closeDS = list(barFeed.getDataSeries(symbol).getAdjCloseDataSeries())
    max_v = max(closeDS)
    max_v_index = closeDS.index(max_v)
    if max_v_index == 0:
        continue
    min_v = min(closeDS[:max_v_index])
    print symbol,min_v,max_v
'''

            



