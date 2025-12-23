CREATE TABLE crypto_volatility (
    timestamp DATETIME,
    open DECIMAL(15,6),
    high DECIMAL(15,6),
    low DECIMAL(15,6),
    close DECIMAL(15,6),
    volume DECIMAL(20,2),
    asset VARCHAR(50),
    symbol VARCHAR(20),
    returns DECIMAL(10,6),
    `7_day_MA` DECIMAL(15,6),
    `30days_MA` DECIMAL(15,6),
    volatility1 DECIMAL(15,6)
);
select * from crypto_volatility

-- Daily Average Close Price
select date(timestamp) as date , AVG(close) as AVG_close
from crypto_volatility
group by timestamp 
order by date;

-- Top 10 Highest Volume Days\

select asset, volume
from crypto_volatility
order by volume desc
limit 10;

-- Highest Volatility Days
select asset,  (high-low) as volatility 
from crypto_volatility
order by volatility desc
limit 10;
 
 -- Monthly Average Close Price
SELECT 
    YEAR(timestamp) AS year,
    MONTH(timestamp) AS month,
    AVG(close) AS avg_close
FROM crypto_volatility
GROUP BY YEAR(timestamp), MONTH(timestamp)
ORDER BY year, month;

-- Count how many days had positive returns.

SELECT 
    asset,
    COUNT(*) AS positive_return_days
FROM crypto_volatility
WHERE close > open
GROUP BY asset;

-- Find total trading volume per asset (Bitcoin, Memecoin).
select asset, SUM(volume ) as total_volume
from crypto_volatility
group by asset;

-- Rank days by volume within each asset and find top 3 days.


SELECT 
    asset,
    trade_date,
    total_volume
FROM (
    SELECT 
        asset,
        DATE(timestamp) AS trade_date,
        SUM(volume) AS total_volume,
        DENSE_RANK() OVER (
            PARTITION BY asset 
            ORDER BY SUM(volume) DESC
        ) AS rnk
    FROM crypto_volatility
    GROUP BY asset, DATE(timestamp)
) t
WHERE rnk <= 3
ORDER BY asset, rnk;

-- Compute 7-day moving average of closing price for each asset.

select DAY(timestamp) as trade_date,
asset,
close,
 AVG(close) over (partition by asset order by DATE(timestamp) ROWS bETWEEN 6 preceding AND Current ROW) as MA_7_day
 from crypto_volatility
 order by asset, trade_date
 
 -- Rank daily returns from highest to lowest across all days.
SELECT
    trade_date,
    asset,
    daily_return,
    DENSE_RANK() OVER (ORDER BY daily_return DESC) AS return_rank
FROM (
    SELECT
        DATE(timestamp) AS trade_date,
        asset,
        (close - LAG(close) OVER (
            PARTITION BY asset 
            ORDER BY DATE(timestamp)
        )) / LAG(close) OVER (
            PARTITION BY asset 
            ORDER BY DATE(timestamp)
        ) AS daily_return
    FROM crypto_volatility
) t
WHERE daily_return IS NOT NULL
ORDER BY return_rank;

 -- Find the day with highest volatility in each month.
 SELECT
    trade_year,
    trade_month,
    trade_date,
    asset,
    volatility
FROM (
    SELECT
        YEAR(timestamp) AS trade_year,
        MONTH(timestamp) AS trade_month,
        DATE(timestamp) AS trade_date,
        asset,
        (high - low) AS volatility,
        ROW_NUMBER() OVER (
            PARTITION BY YEAR(timestamp), MONTH(timestamp), asset
            ORDER BY (high - low) DESC
        ) AS rn
    FROM crypto_volatility
) t
WHERE rn = 1
ORDER BY trade_year, trade_month, asset;

 
 