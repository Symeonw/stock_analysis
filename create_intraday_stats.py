import pandas as pd
import statistics
import datetime as dt
from math import sqrt

bank_ticks = pd.read_csv("tickers_data/tickers_banks_re.csv")
tech_ticks = pd.read_csv("tickers_data/tickers_tech_re.csv")
cycle_ticks = pd.read_csv("tickers_data/tickers_cycle_re.csv")


def intraday_stats_10Y(stock_folder, stock_list):
    intraday_stats_10Y = pd.DataFrame()
    for stock_folder, stock_list in zip(stock_folder, stock_list):
        stock_list = stock_list.tickers
        for stock in stock_list:
            df = pd.read_parquet(f"stock_data/resampled/{stock_folder}/{stock}")
            df = df[["open", "high", "low", "close"]]
            df["intraday_pct_diff_high_low"] = ((df.high - df.low)/df.high*100)
            df["intraday_pct_diff_open_close"] = ((df.open - df.close)/df.high*100)
            df["date"] = df.index
            df["dow"] = df.date.dt.day_name()
            df.drop(columns = ["date","close", "low", "high"], inplace=True)
            df = df.groupby("dow").mean()
            df.reset_index(inplace=True)
            df["ticker"] = stock
            df["ind"] = stock_folder.split("_")[0]
            intraday_stats_10Y = pd.concat([intraday_stats_10Y, df])
    return intraday_stats_10Y

intraday_stats_10Y = intraday_stats_10Y(["cycle_data", "tech_data", "bank_data"]\
    ,[cycle_ticks, tech_ticks, bank_ticks])

intraday_stats_10Y.to_parquet("stock_summary/intraday_pct_10YA", engine="pyarrow", compression="snappy")







def intraday_stats_Q(stock_folder, stock_list):
    intraday_stats_Q = pd.DataFrame()
    for stock_folder, stock_list in zip(stock_folder, stock_list):
        stock_list = stock_list.tickers
        for stock in stock_list:
            df = pd.read_parquet(f"stock_data/resampled/{stock_folder}/{stock}")
            df = df[["open", "high", "low", "close"]]
            df["pct_diff_high_low"] = ((df.high - df.low)/df.high*100)
            df["pct_diff_open_close"] = ((df.open - df.close)/df.high*100)
            df["date"] = df.index
            df["dow"] = df.date.dt.day_name()
            df.drop(columns = ["date","close", "low", "high"], inplace=True)
            df1 = df.groupby("dow").resample("Q-JAN", convention="end").agg("mean")
            df2 = df.groupby("dow").resample("Q-JAN", convention="end").agg("std")
            df2.drop(columns=["pct_diff_high_low", "pct_diff_open_close"], inplace=True)
            df2.rename(columns={"open":"open_std"}, inplace=True)
            df3 = df.groupby("dow").resample("Q-JAN", convention="end").agg("var")
            df3.drop(columns=["pct_diff_high_low", "pct_diff_open_close"], inplace=True)
            df3.rename(columns={"open":"open_var"}, inplace=True)
            df1.reset_index(inplace=True)
            df2.reset_index(inplace=True)
            df3.reset_index(inplace=True)
            df2 = df2[["open_std"]]
            df3["vol"] = df3.open_var.apply(sqrt)
            df3 = df3[["vol"]]
            df = df1.join(df2)
            df = df.join(df3)
            df["ticker"] = stock
            df["ind"] = stock_folder.split("_")[0]
            intraday_stats_Q = pd.concat([intraday_stats_Q, df])
    return intraday_stats_Q

intraday_stats_Q = intraday_stats_Q(["cycle_data", "tech_data", "bank_data"]\
    ,[cycle_ticks, tech_ticks, bank_ticks])

intraday_stats_Q.to_parquet("stock_summary/intraday_stats_QA", engine="pyarrow", compression="snappy")




def intraday_stats_M(stock_folder, stock_list):
    intraday_stats_M = pd.DataFrame()
    for stock_folder, stock_list in zip(stock_folder, stock_list):
        stock_list = stock_list.tickers
        for stock in stock_list:
            df = pd.read_parquet(f"stock_data/resampled/{stock_folder}/{stock}")
            df = df[["open", "high", "low", "close"]]
            df["pct_diff_high_low"] = ((df.high - df.low)/df.high*100)
            df["pct_diff_open_close"] = ((df.open - df.close)/df.high*100)
            df["date"] = df.index
            df["dow"] = df.date.dt.day_name()
            df.drop(columns = ["date","close", "low", "high"], inplace=True)
            df1 = df.groupby("dow").resample("M", convention="end").agg("mean")
            df2 = df.groupby("dow").resample("M", convention="end").agg("std")
            df2.drop(columns=["pct_diff_high_low", "pct_diff_open_close"], inplace=True)
            df2.rename(columns={"open":"open_std"}, inplace=True)
            df3 = df.groupby("dow").resample("M", convention="end").agg("var")
            df3.drop(columns=["pct_diff_high_low", "pct_diff_open_close"], inplace=True)
            df3.rename(columns={"open":"open_var"}, inplace=True)
            df1.reset_index(inplace=True)
            df2.reset_index(inplace=True)
            df3.reset_index(inplace=True)
            df2 = df2[["open_std"]]
            df3["vol"] = df3.open_var.apply(sqrt)
            df3 = df3[["vol"]]
            df = df1.join(df2)
            df = df.join(df3)
            df["ticker"] = stock
            df["ind"] = stock_folder.split("_")[0]
            intraday_stats_M = pd.concat([intraday_stats_M, df])
    return intraday_stats_M


intraday_stats_M = intraday_stats_M(["cycle_data", "tech_data", "bank_data"]\
    ,[cycle_ticks, tech_ticks, bank_ticks])

intraday_stats_M
intraday_stats_M.to_parquet("stock_summary/intraday_stats_MA", engine="pyarrow", compression="snappy")



def intraday_stats_W(stock_folder, stock_list):
    intraday_stats_W = pd.DataFrame()
    for stock_folder, stock_list in zip(stock_folder, stock_list):
        stock_list = stock_list.tickers
        for stock in stock_list:
            df = pd.read_parquet(f"stock_data/resampled/{stock_folder}/{stock}")
            df = df[["open", "high", "low", "close"]]
            df["pct_diff_high_low"] = ((df.high - df.low)/df.high*100)
            df["pct_diff_open_close"] = ((df.open - df.close)/df.high*100)
            df["date"] = df.index
            df["dow"] = df.date.dt.day_name()
            df.drop(columns = ["date","close", "low", "high"], inplace=True)
            df1 = df.groupby("dow").resample("W", convention="end").agg("mean")
            df2 = df.groupby("dow").resample("W", convention="end").agg("std")
            df2.drop(columns=["pct_diff_high_low", "pct_diff_open_close"], inplace=True)
            df2.rename(columns={"open":"open_std"}, inplace=True)
            df3 = df.groupby("dow").resample("W", convention="end").agg("var")
            df3.drop(columns=["pct_diff_high_low", "pct_diff_open_close"], inplace=True)
            df3.rename(columns={"open":"open_var"}, inplace=True)
            df1.reset_index(inplace=True)
            df2.reset_index(inplace=True)
            df3.reset_index(inplace=True)
            df2 = df2[["open_std"]]
            df3["vol"] = df3.open_var.apply(sqrt)
            df3 = df3[["vol"]]
            df = df1.join(df2)
            df = df.join(df3)
            df["ticker"] = stock
            df["ind"] = stock_folder.split("_")[0]
            intraday_stats_W = pd.concat([intraday_stats_W, df])
    return intraday_stats_W


intraday_stats_W = intraday_stats_W(["cycle_data", "tech_data", "bank_data"]\
    ,[cycle_ticks, tech_ticks, bank_ticks])


intraday_stats_W.to_parquet("stock_summary/intraday_stats_WA", engine="pyarrow", compression="snappy")
