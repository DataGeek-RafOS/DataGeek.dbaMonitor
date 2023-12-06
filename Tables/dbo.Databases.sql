CREATE TABLE [dbo].[Databases]
(
[srvId] [tinyint] NOT NULL,
[dbsId] [int] NOT NULL,
[dbsName] [sys].[sysname] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Databases] ADD CONSTRAINT [PK_Databases] PRIMARY KEY CLUSTERED ([srvId], [dbsId]) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [ukNCL_Databases_Server_Name] ON [dbo].[Databases] ([srvId], [dbsName]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Databases] ADD CONSTRAINT [FK_Databases_Servers] FOREIGN KEY ([srvId]) REFERENCES [dbo].[Servers] ([srvId])
GO
