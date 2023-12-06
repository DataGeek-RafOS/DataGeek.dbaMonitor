CREATE TABLE [dbo].[DDLEvents]
(
[EventID] [int] NOT NULL IDENTITY(1, 1),
[EventDate] [datetime] NOT NULL CONSTRAINT [DF__DDLEvents__Event__3C34F16F] DEFAULT (getdate()),
[EventType] [nvarchar] (64) COLLATE Latin1_General_CI_AI NULL,
[EventDDL] [nvarchar] (max) COLLATE Latin1_General_CI_AI NULL,
[EventXML] [xml] NULL,
[DatabaseName] [nvarchar] (255) COLLATE Latin1_General_CI_AI NULL,
[SchemaName] [nvarchar] (255) COLLATE Latin1_General_CI_AI NULL,
[ObjectName] [nvarchar] (255) COLLATE Latin1_General_CI_AI NULL,
[HostName] [varchar] (64) COLLATE Latin1_General_CI_AI NULL,
[IPAddress] [varchar] (48) COLLATE Latin1_General_CI_AI NULL,
[ProgramName] [nvarchar] (255) COLLATE Latin1_General_CI_AI NULL,
[LoginName] [nvarchar] (255) COLLATE Latin1_General_CI_AI NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[DDLEvents] ADD CONSTRAINT [PK_DDLEvents] PRIMARY KEY CLUSTERED ([EventID]) ON [PRIMARY]
GO
