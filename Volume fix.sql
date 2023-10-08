USE [CV Project]

-- Update [Volume] values by replacing NULL with 0 and removing spaces --
UPDATE CryptoData
SET [Volume] = TRY_CAST(REPLACE(ISNULL([Volume], '0'), ' ', '') AS BIGINT);

-- Change [Volume] type --
ALTER TABLE CryptoData
ALTER COLUMN [Volume] BIGINT;

-- Update [Volume] values by replacing NULL with 0 and removing spaces --
UPDATE [CryptoData]
SET [Close]=0
WHERE [Close] IS NULL;
