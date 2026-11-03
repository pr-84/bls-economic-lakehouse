-- Question 1 Alternative: Spark SQL
SELECT
  AVG(population) AS mean_population,
  STDDEV(population) AS stddev_population,
  COUNT(population) AS year_count
FROM macroeconomic_data.raw_data.silver_population
WHERE year BETWEEN 2013 AND 2018;

-- Question 2 Alternative: Spark SQL
WITH quarterly_sums AS (
  SELECT
    series_id,
    year,
    SUM(value) AS annual_sum
  FROM macroeconomic_data.raw_data.silver_bls_data
  WHERE period LIKE 'Q%' AND period != 'Q55'
  GROUP BY series_id, year
),
ranked_series AS (
  SELECT
    series_id,
    year AS best_year,
    annual_sum,
    ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY annual_sum DESC) AS rnk
  FROM quarterly_sums
)
SELECT
  r.series_id,
  s.series_title,
  r.best_year,
  r.annual_sum
FROM ranked_series r
LEFT JOIN macroeconomic_data.raw_data.silver_bls_series s
  ON r.series_id = s.series_id
WHERE r.rnk = 1;

-- Question 3 Alternative: Spark SQL
SELECT
  b.year,
  b.series_id,
  b.period,
  b.value,
  p.population
FROM macroeconomic_data.raw_data.silver_bls_data b
LEFT JOIN macroeconomic_data.raw_data.silver_population p
  ON b.year = p.year
WHERE b.series_id = 'PRS30006032' AND b.period = 'Q01'
ORDER BY b.year;
