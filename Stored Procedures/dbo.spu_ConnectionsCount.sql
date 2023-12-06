SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[spu_ConnectionsCount]
AS
BEGIN
     SET NOCOUNT ON;
     INSERT INTO Connections
     SELECT @@ServerName  AS server
          , name          AS dbname
          , COUNT(status) AS number_of_connections
          , GETDATE()     AS timestamp
     FROM sys.databases           sd
          LEFT JOIN
          master.dbo.sysprocesses sp
               ON sd.database_id = sp.dbid
     WHERE database_id NOT BETWEEN 1 AND 4
     GROUP BY name;
END;
GO
