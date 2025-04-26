WITH RankedActuals AS (
  SELECT
    a.CarParkID,
    a.timestamp AS actual_time,
    a.AvailableLots AS actual_lots,
    ROW_NUMBER() OVER (
      PARTITION BY a.CarParkID, TIMESTAMP_TRUNC(a.timestamp, MINUTE)
      ORDER BY a.timestamp ASC
    ) AS rn
  FROM
    `smart-car-park-availability-1.lta_data.carpark_availability` a
),
prediction_vs_actual AS (
  SELECT
    p.CarParkID,
    p.utctimestamp AS prediction_time,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(CAST(p.utctimestamp AS TIMESTAMP), MINUTE),
      INTERVAL CAST(
        30 + (FLOOR(EXTRACT(MINUTE FROM CAST(p.utctimestamp AS TIMESTAMP)) / 5) * 5)
        - EXTRACT(MINUTE FROM CAST(p.utctimestamp AS TIMESTAMP))
        AS INT64
      ) MINUTE
    ) AS expected_time,
    p.PredictedLots,
    ra.actual_time,
    ra.actual_lots
  FROM
    `smart-car-park-availability-1.lta_data.view_carpark_predictions_30min_1` p
  LEFT JOIN
    RankedActuals ra
  ON
    p.CarParkID = ra.CarParkID
    AND TIMESTAMP_TRUNC(ra.actual_time, MINUTE) = TIMESTAMP_TRUNC(
      TIMESTAMP_ADD(
        TIMESTAMP_TRUNC(CAST(p.utctimestamp AS TIMESTAMP), MINUTE),
        INTERVAL CAST(
          30 + (FLOOR(EXTRACT(MINUTE FROM CAST(p.utctimestamp AS TIMESTAMP)) / 5) * 5)
          - EXTRACT(MINUTE FROM CAST(p.utctimestamp AS TIMESTAMP))
          AS INT64
        ) MINUTE
      ),
      MINUTE
    )
    AND ra.rn = 1
)

SELECT
  TIMESTAMP("2025-04-22 19:00:00+08:00") AS monitoring_timestamp,  -- Required for table schema
  COUNT(*) AS total_records,
  ROUND(AVG(ABS(PredictedLots - actual_lots)), 2) AS MAE,
  ROUND(SQRT(AVG(POW(PredictedLots - actual_lots, 2))), 2) AS RMSE,
  ROUND(
    AVG(
      CASE
        WHEN actual_lots IS NOT NULL AND actual_lots != 0 THEN ABS((actual_lots - PredictedLots) / actual_lots) * 100
        ELSE NULL
      END
    ),
    2
  ) AS MAPE
FROM prediction_vs_actual;
