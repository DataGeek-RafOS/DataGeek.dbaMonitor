SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE VIEW [dbo].[BlitzFirst_PerfmonStats_Deltas] AS 
WITH RowDates as
(
        SELECT 
                ROW_NUMBER() OVER (ORDER BY [ServerName], [CheckDate]) ID,
                [CheckDate]
        FROM [dbo].[BlitzFirst_PerfmonStats]
        GROUP BY [ServerName], [CheckDate]
),
CheckDates as
(
        SELECT ThisDate.CheckDate,
               LastDate.CheckDate as PreviousCheckDate
        FROM RowDates ThisDate
        JOIN RowDates LastDate
        ON ThisDate.ID = LastDate.ID + 1
)
SELECT
       pMon.[ServerName]
      ,pMon.[CheckDate]
      ,pMon.[object_name]
      ,pMon.[counter_name]
      ,pMon.[instance_name]
      ,DATEDIFF(SECOND,pMonPrior.[CheckDate],pMon.[CheckDate]) AS ElapsedSeconds
      ,pMon.[cntr_value]
      ,pMon.[cntr_type]
      ,(pMon.[cntr_value] - pMonPrior.[cntr_value]) AS cntr_delta
      ,(pMon.cntr_value - pMonPrior.cntr_value) * 1.0 / DATEDIFF(ss, pMonPrior.CheckDate, pMon.CheckDate) AS cntr_delta_per_second
      ,pMon.ServerName + CAST(pMon.CheckDate AS NVARCHAR(50)) AS JoinKey
  FROM [dbo].[BlitzFirst_PerfmonStats] pMon
  INNER HASH JOIN CheckDates Dates
  ON Dates.CheckDate = pMon.CheckDate
  JOIN [dbo].[BlitzFirst_PerfmonStats] pMonPrior
  ON  Dates.PreviousCheckDate = pMonPrior.CheckDate
      AND pMon.[ServerName]    = pMonPrior.[ServerName]   
      AND pMon.[object_name]   = pMonPrior.[object_name]  
      AND pMon.[counter_name]  = pMonPrior.[counter_name] 
      AND pMon.[instance_name] = pMonPrior.[instance_name]
    WHERE DATEDIFF(MI, pMonPrior.CheckDate, pMon.CheckDate) BETWEEN 1 AND 60;
GO
