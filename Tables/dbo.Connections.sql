CREATE TABLE [dbo].[Connections]
(
[server] [nvarchar] (130) COLLATE Latin1_General_CI_AI NOT NULL,
[name] [nvarchar] (130) COLLATE Latin1_General_CI_AI NOT NULL,
[number_of_connections] [int] NOT NULL,
[timestamp] [datetime] NOT NULL
) ON [PRIMARY]
GO
