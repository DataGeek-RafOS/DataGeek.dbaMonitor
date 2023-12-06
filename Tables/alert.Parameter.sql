CREATE TABLE [alert].[Parameter]
(
[prmId] [smallint] NOT NULL IDENTITY(1, 1),
[prmName] [varchar] (100) COLLATE Latin1_General_CI_AI NOT NULL,
[prmProcedure] [varchar] (100) COLLATE Latin1_General_CI_AI NOT NULL,
[prmSolvedAlert] [bit] NOT NULL,
[prmValue] [int] NULL,
[prmMetric] [varchar] (50) COLLATE Latin1_General_CI_AI NULL,
[prmFileLocation] [varchar] (1000) COLLATE Latin1_General_CI_AI NULL,
[prmOperator] [varchar] (200) COLLATE Latin1_General_CI_AI NULL
) ON [PRIMARY]
GO
ALTER TABLE [alert].[Parameter] ADD CONSTRAINT [PK_Parameter] PRIMARY KEY CLUSTERED ([prmId]) ON [PRIMARY]
GO
