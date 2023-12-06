SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE VIEW [dbo].[vw_GrowthTrackDB]
AS
WITH cteDBGrowth
AS (
   SELECT 
          DatabaseName
        , LogicalName
        , FileType
        , [SpaceUsedGB] = LAST_VALUE(FileSizeGB) OVER (PARTITION BY DatabaseName, LogicalName, FileType
                                                           ORDER BY GatherDate DESC) - 
                          LAST_VALUE(FreeSpaceInFileGB) OVER (PARTITION BY DatabaseName, LogicalName, FileType
                                                                  ORDER BY GatherDate DESC)
        , LAST_VALUE(GatherDate) OVER (PARTITION BY DatabaseName, LogicalName, FileType
                                           ORDER BY GatherDate DESC) AS GatherDate
        , ROW_NUMBER() OVER (PARTITION BY DatabaseName, LogicalName, FileType
                                 ORDER BY GatherDate DESC) AS RowNum
   FROM dbo.DatabaseFiles
   WHERE FileType = 'Data'
   AND   DatabaseName NOT LIKE ('tempdb') -- Desconsidera tempdb
   ) 

SELECT DatabaseName
     , LogicalName
     , [Last3Months]
     , [Last2Months]
     , [LastMonth]
     , [Last15Days]
     , [LastWeek]
     , [Yesterday]
     , [LastMonthGrowth] AS [LastMonthGrowth2]
     , [LastMonthGrowth] = CASE 
                                 WHEN ABS([LastMonthGrowth]) < 1 
                                 THEN CONVERT(VARCHAR(13), (CONVERT(NUMERIC(13,0), ROUND(([LastMonthGrowth]) * 1024, 0)))) + ' MB'
                                 ELSE CONVERT(VARCHAR(13), (CONVERT(NUMERIC(13,2), (([LastMonthGrowth]))))) + ' GB'
                            END
     , [AvgGrowthMonthly] = CASE 
                                 WHEN ABS([AvgGrowthMonthly]) < 1 
                                 THEN CONVERT(VARCHAR(13), (CONVERT(NUMERIC(13,0), ROUND(([AvgGrowthMonthly]) * 1024, 0)))) + ' MB'
                                 ELSE CONVERT(VARCHAR(13), (CONVERT(NUMERIC(13,2), (([AvgGrowthMonthly]))))) + ' GB'
                            END 
     , [AvgGrowthOrderBy] = ROW_NUMBER() OVER (ORDER BY CONVERT(NUMERIC(13,2), AvgGrowthMonthly) DESC)

FROM (
     SELECT DatabaseName
          , LogicalName
          , [Last3Months]
          , [Last2Months]
          , [LastMonth]
          , [Last15Days]
          , [LastWeek]
          , [Yesterday]
          , [LastMonthGrowth] = [Yesterday] - [LastMonth]
          , [AvgGrowthMonthly] = ([Yesterday] - COALESCE([Last3Months], [Last2Months], [LastMonth])) / CASE WHEN [Last3Months] IS NOT NULL THEN 3.0
                                                                                                            WHEN [Last2Months] IS NOT NULL THEN 2.0
                                                                                                            ELSE 1.0
                                                                                                       END
     FROM (
          SELECT DatabaseName
               , LogicalName
               , SpaceUsedGB
               , [Position] = CASE 
                                   WHEN RowNum = 90
                                   THEN 'Last3Months'
                                   WHEN RowNum = 60
                                   THEN 'Last2Months'
                                   WHEN RowNum = 30
                                   THEN 'LastMonth'
                                   WHEN RowNum = 15
                                   THEN 'Last15Days'
                                   WHEN RowNum = 7
                                   THEN 'LastWeek'
                                   WHEN RowNum = 1
                                   THEN 'Yesterday'
                              END
          FROM cteDBGrowth 
          WHERE RowNum IN (1, 7, 15, 30, 60, 90) -- Ontem, última semana, última quinzena, último mês e penúltimo mês
          ) AS GrowthReport
     PIVOT 
         ( MAX(SpaceUsedGB)
           FOR [Position]
           IN ([Last3Months], [Last2Months], [LastMonth], [Last15Days], [LastWeek], [Yesterday])
         ) AS pvtGrowthReport
     ) AS FinalReport
WHERE AvgGrowthMonthly != 0
--ORDER BY CONVERT(NUMERIC(13,2), AvgGrowthMonthly) DESC 
     

GO
