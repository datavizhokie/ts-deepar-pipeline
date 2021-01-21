# ts-deepar-pipeline
Flexible pipeline for modeling time series data with DeepAR, calling predictions, and producing results.

1. Initial grouping of data to desired level for forecasting
2. Processing for DeepAR json format
3. Tuning job
4. Batch Transform and post-processing of results

## Data quality and Seasonality

Crime data is quite stochastic, however seasonal patterns prevail. As such, predicting at the daily level would not be very effective, so predicting at the weekly level is a more tractable problem. The data starts in January 2016 and goes through early January 2021. In order to assess performance on a sizeable portion of the data, I chose to train through December 2019, and predict 52 weeks in the year 2020.

## Below is an interesting correlation between two Categories:
![viz](https://github.com/datavizhokie/ts-deepar-pipeline/blob/master/Den%20Traffic%20Accidents%20%26%20Auto%20Thefts.png)
