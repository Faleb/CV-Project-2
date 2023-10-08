USE [CV Project]

-- Number of days for which data is available for each cryptocurrency --

SELECT [Name], COUNT(*) AS NumberOfDaysWithData
FROM CryptoData
GROUP BY [Name]
ORDER BY NumberOfDaysWithData DESC;

-- Average closing price for each cryptocurrency --

SELECT [Name], AVG([Close]) AS AverageClosePrice
FROM CryptoData
GROUP BY [Name]
ORDER BY AverageClosePrice DESC;

-- Moving Average closing price for each cryptocurrency --

SELECT 
    [Name],
    [Date],
    [Close],
    AVG([Close]) OVER (PARTITION BY [Name] ORDER BY [Date] ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS Two_day_moving_average,
    AVG([Close]) OVER (PARTITION BY [Name] ORDER BY [Date] ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS Thirty_days_moving_average
FROM 
    CryptoData;

-- Maximum and minimum closing prices for each cryptocurrency --

SELECT [Name], MAX([Close]) AS MaxClosePrice, MIN([Close]) AS MinClosePrice
FROM CryptoData
GROUP BY [Name]
ORDER BY MinClosePrice DESC;

-- Total trading volume for each cryptocurrency --

SELECT [Name], SUM([Volume]) AS TotalVolume
FROM CryptoData
GROUP BY [Name]
ORDER BY TotalVolume DESC;

-- Standard deviation of closing prices for each cryptocurrency --

SELECT [Name], STDEV([Close]) AS StandardDeviationClosePrice
FROM CryptoData
WHERE [Close] IS NOT NULL
GROUP BY [Name]
HAVING COUNT([Close]) > 5 AND STDEV([Close]) > 0 -- Exclude groups with less than 6 closing price values and a standard deviation of zero
ORDER BY StandardDeviationClosePrice ASC;

-- Closing price percentiles for each cryptocurrency --

WITH Quartiles AS (
    SELECT 
        [Name], 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY [Close]) OVER (PARTITION BY [Name]) AS Q1,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [Close]) OVER (PARTITION BY [Name]) AS Median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY [Close]) OVER (PARTITION BY [Name]) AS Q3
    FROM 
        CryptoData
)
SELECT DISTINCT [Name], Q1, Median, Q3
FROM Quartiles
ORDER BY Median DESC;

-- Calculating correlations for cryptocurrencies --

WITH CorrelationData AS (
    SELECT 
        [Name],
        COUNT(*) AS SampleSize,
        SUM([Open] * [High]) AS SumOpenHigh,
        SUM([Open] * [Low]) AS SumOpenLow,
        SUM([Open] * [Close]) AS SumOpenClose,
        SUM([High] * [Low]) AS SumHighLow,
        SUM([High] * [Close]) AS SumHighClose,
        SUM([Low] * [Close]) AS SumLowClose,
        SUM([Open]) AS SumOpen,
        SUM([High]) AS SumHigh,
        SUM([Low]) AS SumLow,
        SUM([Close]) AS SumClose,
        SUM([Open] * [Open]) AS SumOpenSquared,
        SUM([High] * [High]) AS SumHighSquared,
        SUM([Low] * [Low]) AS SumLowSquared,
        SUM([Close] * [Close]) AS SumCloseSquared
    FROM 
        CryptoData
    GROUP BY 
        [Name]
)
SELECT 
    [Name],
    CASE 
        WHEN (SQRT(SampleSize * SumOpenSquared - SumOpen * SumOpen) * 
              SQRT(SampleSize * SumHighSquared - SumHigh * SumHigh)) = 0 THEN NULL
        ELSE (SampleSize * SumOpenHigh - SumOpen * SumHigh) / 
             (SQRT(SampleSize * SumOpenSquared - SumOpen * SumOpen) *
              SQRT(SampleSize * SumHighSquared - SumHigh * SumHigh))
    END AS correlation_open_high,
    CASE 
        WHEN (SQRT(SampleSize * SumOpenSquared - SumOpen * SumOpen) * 
              SQRT(SampleSize * SumLowSquared - SumLow * SumLow)) = 0 THEN NULL
        ELSE (SampleSize * SumOpenLow - SumOpen * SumLow) / 
             (SQRT(SampleSize * SumOpenSquared - SumOpen * SumOpen) *
              SQRT(SampleSize * SumLowSquared - SumLow * SumLow))
    END AS correlation_open_low,
    CASE 
        WHEN (SQRT(SampleSize * SumOpenSquared - SumOpen * SumOpen) * 
              SQRT(SampleSize * SumCloseSquared - SumClose * SumClose)) = 0 THEN NULL
        ELSE (SampleSize * SumOpenClose - SumOpen * SumClose) / 
             (SQRT(SampleSize * SumOpenSquared - SumOpen * SumOpen) *
              SQRT(SampleSize * SumCloseSquared - SumClose * SumClose))
    END AS correlation_open_close,
    CASE 
        WHEN (SQRT(SampleSize * SumHighSquared - SumHigh * SumHigh) * 
              SQRT(SampleSize * SumLowSquared - SumLow * SumLow)) = 0 THEN NULL
        ELSE (SampleSize * SumHighLow - SumHigh * SumLow) / 
             (SQRT(SampleSize * SumHighSquared - SumHigh * SumHigh) *
              SQRT(SampleSize * SumLowSquared - SumLow * SumLow))
    END AS correlation_high_low,
    CASE 
        WHEN (SQRT(SampleSize * SumHighSquared - SumHigh * SumHigh) * 
              SQRT(SampleSize * SumCloseSquared - SumClose * SumClose)) = 0 THEN NULL
        ELSE (SampleSize * SumHighClose - SumHigh * SumClose) / 
             (SQRT(SampleSize * SumHighSquared - SumHigh * SumHigh) *
              SQRT(SampleSize * SumCloseSquared - SumClose * SumClose))
    END AS correlation_high_close,
    CASE 
        WHEN (SQRT(SampleSize * SumLowSquared - SumLow * SumLow) * 
              SQRT(SampleSize * SumCloseSquared - SumClose * SumClose)) = 0 THEN NULL
        ELSE (SampleSize * SumLowClose - SumLow * SumClose) / 
             (SQRT(SampleSize * SumLowSquared - SumLow * SumLow) *
              SQRT(SampleSize * SumCloseSquared - SumClose * SumClose))
    END AS correlation_low_close
FROM 
    CorrelationData
ORDER BY 
    correlation_open_high DESC,
    correlation_open_low DESC,
    correlation_open_close DESC,
    correlation_high_low DESC,
    correlation_high_close DESC,
    correlation_low_close DESC;

-- Autocorrelation --

WITH LaggedData AS (
    SELECT 
        [Name],
        [Date],
        [Close],
        LAG([Close], 1, NULL) OVER (PARTITION BY [Name] ORDER BY [Date]) AS LaggedClose
    FROM 
        CryptoData
)
SELECT 
    [Name],
    [Date],
    [Close],
    LaggedClose,
    (CASE 
        WHEN LaggedClose IS NOT NULL AND LaggedClose <> 0 THEN 
            ([Close] - LaggedClose) / LaggedClose
        ELSE 
            NULL 
    END) AS Autocorrelation
FROM 
    LaggedData;

-- Average daily trading volume for each currency --

SELECT 
    [Name],
    AVG([Volume]) AS AvgDailyVolume
FROM 
    Cryptodata
GROUP BY 
    [Name]
ORDER BY 
    AvgDailyVolume DESC;

--Total trading volume by day of the week for all currencies --

	SELECT 
    DATEPART(weekday, [Date]) AS DayOfWeek,
    SUM([Volume]) AS TotalVolume
FROM 
    CryptoData
GROUP BY 
    DATEPART(weekday, [Date])
ORDER BY 
    DayOfWeek;