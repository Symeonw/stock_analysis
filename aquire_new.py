import pandas as pd
import pandas_datareader as web

netflix = web.get_data_yahoo("NFLX", start = "2009-01-01",end = "2020-02-21")
netflix

def aquire_yahoo_stock_data(stock_folder, stock,finished_list=[]):
    stock_list = stock.tickers
    stock_list = list(set(stock_list) - set(finished_list))
    error_list = []
    fl = []
    stock_list_len = len(stock_list)
    fin = 0
    for stock in stock_list:
        try:
            df = web.get_data_yahoo(stock, start = "2009-01-01", end = "2020-02-21")
            df.drop(columns=["Adj Close"], inplace=True)
            df.to_parquet(f"stock_data/resampled/{stock_folder}/{stock}", engine="pyarrow", compression="snappy")
            fl.append(stock)
            fin += 1
            print(f"{(fin/stock_list_len)*100}% Done")
        except:
            print(f"Error for stock: {stock}")
            error_list.append(stock)
            fin += 1
    return error_list, fl




bank_ticks = pd.read_csv("tickers_data/tickers_banks.csv")
tech_ticks = pd.read_csv("tickers_data/tickers_tech.csv")
cycle_ticks = pd.read_csv("tickers_data/tickers_cycle.csv")

error_list, finished_list = aquire_yahoo_stock_data("bank_data", bank_ticks, finished_list)

error_list, finished_list = aquire_yahoo_stock_data("tech_data", tech_ticks)


error_list, finished_list= aquire_yahoo_stock_data("cycle_data", cycle_ticks)    

new = pd.DataFrame(list(set(cycle_ticks.tickers)-set(error_list)))
new.rename(columns={0:"tickers"}, inplace=True)
new.to_csv("tickers_cycle_re.csv")

