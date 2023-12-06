CREATE TABLE [dbo].[ObjectChangeLog]
(
[EventType] [varchar] (250) COLLATE Latin1_General_CI_AI NULL,
[PostTime] [datetime] NULL,
[ServerName] [varchar] (250) COLLATE Latin1_General_CI_AI NULL,
[LoginName] [varchar] (250) COLLATE Latin1_General_CI_AI NULL,
[UserName] [varchar] (250) COLLATE Latin1_General_CI_AI NULL,
[DatabaseName] [varchar] (250) COLLATE Latin1_General_CI_AI NULL,
[SchemaName] [varchar] (250) COLLATE Latin1_General_CI_AI NULL,
[ObjectName] [varchar] (250) COLLATE Latin1_General_CI_AI NULL,
[ObjectType] [varchar] (250) COLLATE Latin1_General_CI_AI NULL,
[TSQLCommand] [varchar] (max) COLLATE Latin1_General_CI_AI NULL
) ON [PRIMARY]
GO
