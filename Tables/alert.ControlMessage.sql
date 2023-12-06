CREATE TABLE [alert].[ControlMessage]
(
[alcId] [smallint] NOT NULL IDENTITY(1, 1),
[prmId] [smallint] NOT NULL,
[alcMessage] [varchar] (2000) COLLATE Latin1_General_CI_AI NULL,
[alcType] [char] (1) COLLATE Latin1_General_CI_AI NOT NULL CONSTRAINT [DF_ControlMessage_alcType] DEFAULT ('A'),
[alcCreationDate] [datetime2] (0) NOT NULL CONSTRAINT [DF_Settings_alcCreationDate] DEFAULT ('sysdatetime()'),
[alcResolutionDate] [datetime2] (0) NULL
) ON [PRIMARY]
GO
ALTER TABLE [alert].[ControlMessage] ADD CONSTRAINT [CKC_Settings_alcType] CHECK (([alcType]='R' OR [alcType]='A'))
GO
ALTER TABLE [alert].[ControlMessage] ADD CONSTRAINT [PK_AlertControl] PRIMARY KEY CLUSTERED ([alcId]) ON [PRIMARY]
GO
