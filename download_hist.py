'''
download last three year history data
'''

from pyalgotrade.tools import yahoofinance
from pyalgotrade.utils import dt
import threadpool
import pandas as pd
import datetime


#nasdaq
nasdaq_df = pd.read_csv("data/nasdaq.csv")
nasdaq_df = nasdaq_df[nasdaq_df.Industry != 'n/a']

#amex
amex_df = pd.read_csv("data/amex.csv")
amex_df = amex_df[amex_df.Industry != 'n/a']

#ntse

nyse_df = pd.read_csv("data/nyse.csv")
nyse_df = nyse_df[nyse_df.Industry != 'n/a']

now = datetime.datetime.now() 
year = now.year

begin = dt.get_first_monday(year-3)
end = dt.get_last_monday(year) + datetime.timedelta(days=6)


data = nasdaq_df['Symbol'].tolist() + amex_df['Symbol'].tolist()+nyse_df['Symbol'].tolist()


def callback(request, result):
    symbol = request.args[0]
    csvFile = "result/%s.csv"%(symbol)
    if result is not None:
        f = open(csvFile, "w")
        f.write(result)
        f.close()
    
def run(symbol):
    try:
        return yahoofinance.download_csv(symbol,begin, end, "w")
    except Exception:
        pass
thread_num = 100    
pool = threadpool.ThreadPool(thread_num) 
requests = threadpool.makeRequests(run, data, callback) 
[pool.putRequest(req) for req in requests] 
pool.wait()
pool.dismissWorkers(thread_num, do_join=True)




#yahoofinance.download_weekly_bars(instrument, year, csvFile)