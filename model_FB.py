import pandas as pd
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation

df = pd.read_parquet("stock_data/resampled/tech_data/FB")
df = df[["open"]]
df["ds"] = pd.to_datetime(df.index)
df.rename(columns={"open":"y"}, inplace=True)
df.reset_index(inplace=True)
df.drop(columns=("Date"), inplace=True)
m = Prophet()
m.fit(df)
future = m.make_future_dataframe(periods=20)
future
forecast = m.predict(future)

fig1 = m.plot(forecast)
df_cv = cross_validation(m, initial='30 days', period='5 days', horizon = '10 days')

from fbprophet.diagnostics import performance_metrics
df_p_1 = performance_metrics(df_cv)
df_p_2 = performance_metrics(df_cv)
from fbprophet.plot import plot_cross_validation_metric
fig = plot_cross_validation_metric(df_cv, metric='mape')
fig2 = plot_cross_validation_metric(df_cv, metric='mape')
