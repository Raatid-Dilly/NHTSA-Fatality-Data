--Create the model
CREATE OR REPLACE MODEL `dataset.accidents_arima_model`
OPTIONS
  (
    model_type = 'ARIMA_PLUS',
    time_series_timestamp_col = 'Year',
    time_series_data_col = 'Fatals',
    time_series_id_col = 'State',
    auto_arima_max_order = 5,
    data_frequency = 'yearly'
  ) AS
SELECT
   Year, State, SUM(Fatals) AS Fatals
FROM
    `dataset.accidents_arima_model`
WHERE
    Year IS NOT NULL
GROUP BY 1, 2


--Evaluating the model
SELECT * FROM
  ML.ARIMA_EVALUATE(MODEL `dataset.accidents_arima_model`)

--Evaluating model coefficients
SELECT * FROM
  ML.ARIMA_COEFFICIENTS(MODEL `dataset.accidents_arima_model`)


--Forecasting the model
SELECT
  EXTRACT(Year FROM forecast_timestamp) AS Year,
  State,
  SUM(ROUND(forecast_value)) AS Forcasted_Fatals,
FROM
  ML.FORECAST(MODEL `dataset.accidents_arima_model`,
              STRUCT(3 AS horizon, 0.9 AS confidence_level))
GROUP BY 1, 2
ORDER BY 1, 2