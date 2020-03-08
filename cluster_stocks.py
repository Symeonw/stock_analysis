import pandas as pd 
import numpy as np
import seaborn as sns
from scipy.stats import spearmanr

df = pd.read_parquet("stock_summary/classify_gains")

def stocks_corr_pct_2018(df):
    dfi = df.drop(columns="ind")
    dfi = df[dfi.index > "2017-12-31"]
    final = pd.DataFrame()
    tickers = list(set(dfi.ticker))
    counter = 0
    to_parq = 1
    to_break = False
    remove_tick = []
    while True:
        remove_tick = list(set(remove_tick))
        tickers = [x for x in tickers if x not in remove_tick]
        for tick in tickers:
            if to_break == True:
                to_break = False
                break
            for tick2 in tickers:
                if tick == tick2:
                    continue
                print(counter)
                try:
                    df2 = dfi[dfi.ticker == tick]
                    df3 = dfi[dfi.ticker == tick2]
                    higher_close_corr = spearmanr(list(df2.higher_close), list(df3.higher_close))
                    higher_pct_corr = spearmanr(list(df2.higher_close_pct),list(df3.higher_close_pct))
                    out = pd.DataFrame([[higher_close_corr[0],higher_pct_corr[0], tick,tick2]], columns=["higher_close_corr", "higher_close_pct_corr", "ticker_1", "ticker_2"])
                    counter += 1
                    final = pd.concat([final, out])
                    remove_tick.append(tick)
                    to_break = True
                except:
                    print(f"Not enough data for {tick}, {tick2}")
                    counter += 1
                    remove_tick.append(tick)
                    remove_tick.append(tick2)
                    to_break = True
                if counter % 10000 == 0:
                    final.to_parquet(f"stock_summary/cluster_stocks{to_parq}", engine = "pyarrow", compression="snappy")
                    to_parq += 1
                    final = pd.DataFrame()
                
        if len(tickers) <= 1:
            break
    return final


final = stocks_corr_pct_2018(df)






df2 = pd.DataFrame()

for n in range(1,30):
    x = pd.read_parquet(f"stock_summary/cluster_stocks{n}")
    df2 = pd.concat([x, df2])

x = list(set(df2.ticker_1))



def stocks_corr_pct_2018_batch_2(df, x):
    dfi = df.drop(columns="ind")
    dfi = df[dfi.index > "2017-12-31"]
    final = pd.DataFrame()
    tickers = x
    counter = 0
    to_parq = 1
    to_break = False
    remove_tick = []
    while True:
        remove_tick = list(set(remove_tick))
        tickers = [x for x in tickers if x not in remove_tick]
        for tick in tickers:
            if to_break == True:
                to_break = False
                break
            for tick2 in tickers:
                if tick == tick2:
                    continue
                print(counter)
                try:
                    df2 = dfi[dfi.ticker == tick]
                    df3 = dfi[dfi.ticker == tick2]
                    higher_close_corr = spearmanr(list(df2.higher_close), list(df3.higher_close))
                    higher_pct_corr = spearmanr(list(df2.higher_close_pct),list(df3.higher_close_pct))
                    out = pd.DataFrame([[higher_close_corr[0],higher_pct_corr[0], tick,tick2]], columns=["higher_close_corr", "higher_close_pct_corr", "ticker_1", "ticker_2"])
                    counter += 1
                    final = pd.concat([final, out])
                    remove_tick.append(tick)
                    to_break = True
                except:
                    print(f"Not enough data for {tick}, {tick2}")
                    counter += 1
                    remove_tick.append(tick)
                    remove_tick.append(tick2)
                    to_break = True
                if counter % 10000 == 0:
                    final.to_parquet(f"stock_summary/cluster_stocks{to_parq}", engine = "pyarrow", compression="snappy")
                    to_parq += 1
                    final = pd.DataFrame()
                
        if len(tickers) <= 1:
            break
    return final

final = stocks_corr_pct_2018_batch_2(df, x)

final

df.reset_index(inplace=True)
df.drop(columns="index", inplace=True)
df.to_parquet("stock_summary/cluster_stock_group_one", engine="pyarrow", compression="snappy")

t = pd.read_parquet("stock_summary/cluster_stocks1")
df2 = pd.concat([t,df2])
df2 = pd.concat([final, df2])
df2.reset_index(inplace=True)
df2.drop(columns='index', inplace=True)
pd.read_parquet("stock_summary/cluster_stocks_final")
pd.read()




df2 = pd.DataFrame()

for n in range(1,100):
    try:
        x = pd.read_parquet(f"stock_summary/cluster_stocks{n}")
        df2 = pd.concat([x, df2])
    except:
        continue

df2


x = list(set(df.ticker)-set(df2.ticker_1))
len(x)

df2 = pd.read_parquet("stock_summary/cluster_stock_group_one")

df2[df2.higher_close_corr == 0.661962213099544]

df2[df2.higher_close_corr == 0.7233779408290603]

df2.higher_close_pct_corr.describe()
df2.higher_close_corr.describe()

list(set(df2.ticker_1))
x = list(set(df.ticker)-set(df2.ticker_1))

len(x)
x = x[1:]