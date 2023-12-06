CREATE TABLE [dbo].[Servers]
(
[srvId] [tinyint] NOT NULL,
[srvName] [varchar] (128) COLLATE Latin1_General_CI_AI NOT NULL,
[srvDescription] [varchar] (500) COLLATE Latin1_General_CI_AI NOT NULL,
[srvIP] [char] (16) COLLATE Latin1_General_CI_AI NOT NULL,
[srvEnvironment] [varchar] (15) COLLATE Latin1_General_CI_AI NOT NULL,
[srvCPUCount] [tinyint] NULL CONSTRAINT [DF_Servidor_srvCPUCount] DEFAULT ((0)),
[srvCoreCount] [tinyint] NOT NULL CONSTRAINT [DF_Servidor_srvCoreCount] DEFAULT ((0)),
[srvMemory] [smallint] NOT NULL CONSTRAINT [DF_Servidor_srvMemory] DEFAULT ((0))
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Servers] ADD CONSTRAINT [PK_Instance] PRIMARY KEY CLUSTERED ([srvId]) ON [PRIMARY]
GO
