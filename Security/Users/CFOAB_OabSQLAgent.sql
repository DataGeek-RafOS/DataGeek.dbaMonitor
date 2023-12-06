IF NOT EXISTS (SELECT * FROM master.dbo.syslogins WHERE loginname = N'CFOAB\oabsqlagent')
CREATE LOGIN [CFOAB\oabsqlagent] FROM WINDOWS
GO
CREATE USER [CFOAB\OabSQLAgent] FOR LOGIN [CFOAB\oabsqlagent]
GO
