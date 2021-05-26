# ts-deepar-pipeline
Flexible pipeline for modeling time series data with DeepAR, calling predictions, and producing results.

1. Initial grouping of data to desired level for forecasting
2. Processing for DeepAR json format
3. Tuning job
4. Batch Transform and post-processing of results

## Data quality and Seasonality

Crime data is quite stochastic, however seasonal patterns prevail. As such, predicting at the daily level would not be very effective, so predicting at the weekly level is a more tractable problem. 

### An interesting correlation between two Categories, given the global pandemic of 2020:
<img src="https://github.com/datavizhokie/ts-deepar-pipeline/blob/master/Den%20Traffic%20Accidents%20%26%20Auto%20Thefts.png" width=50% height=50%>


## Training and Test Data

The data starts in January 2016 and goes through early January 2021. In order to assess performance on a sizeable portion of the data, I chose to train through December 2019, and predict 52 weeks in the year 2020.

The maximum Context Length equals:

(Training Set length) - (Prediction Period) =  209-52 = 157

## Modeling results

After a few iterations of hyperparameter tuning jobs, the results are not as good as expected. Things to consider that affect model performance:


### Data Considerations
* Immediate drop in crime as a result of Denver Covid shutdown in March
* Civil unrest and rioting in late May/early June
* Lower crime overall due to Covid lockdowns across 2020

### Further Modeling Considerations
* Add categorical features for each series (i.e. creating broader group for grouping similar categories such as "auto-theft" and "theft-from-motor-vehicle"
* Add dynamic features (i.e. binary indicators for scheduled political events and holidays)

### Visualization of modeling results for each Crime Category forecasted

<img src="https://github.com/datavizhokie/ts-deepar-pipeline/blob/master/deepar_results_viz" width=75% height=75%>
