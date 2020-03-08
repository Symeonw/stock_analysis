import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from math import sqrt


corr = pd.read_parquet("stock_summary/cluster_stocks_final")

df = corr[corr.ticker_1 == "AAPL"]

corr.groupby(["ticker_1"], sort=False)["higher_close_pct_corr"].max()

idx = corr.groupby(["ticker_1"])["higher_close_pct_corr"].transform(max) == corr["higher_close_pct_corr"]
corr[idx]

df = corr[corr.higher_close_pct_corr > .6]

df







intraday = pd.read_parquet("stock_summary/intraday_stats_MA")
intraday

plt.plot(intraday.groupby("Date").mean().pct_high_low)

plt.plot(intraday.groupby("Date").mean().pct_open_close)

plt.plot(intraday.groupRby("Date").mean().open_std)

plt.plot(intraday.groupby("Date").mean().open_var)

df = intraday[intraday.ticker == "AAPL"]
sns.lineplot(x=df.Date, y=df.vol)



#Tests for volatile ranks (1-4(least to most volatility):
    #Month to Month rolling period compared to All stocks 
    #Bi-Monthly rolling period 
    #Quarterly rolling period
    #Bi-quarterly rolling period

#When does it switch volatility ranking in case up massive increase in volatility?
    # A week by week rolling period? Day by Day/intraday rolling period? 


#Need to calculate momentum (whether stock keeps moving in the same direction)

