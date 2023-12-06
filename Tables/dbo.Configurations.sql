CREATE TABLE [dbo].[Configurations]
(
[cnfAttribute] [varchar] (255) COLLATE Latin1_General_CI_AI NOT NULL,
[cnfValue] [varchar] (max) COLLATE Latin1_General_CI_AI NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Configurations] ADD CONSTRAINT [PK_Configurations] PRIMARY KEY CLUSTERED ([cnfAttribute]) ON [PRIMARY]
GO
