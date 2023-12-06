CREATE TABLE [dbo].[DatabaseFiles]
(
[dbfId] [int] NOT NULL IDENTITY(1, 1),
[srvId] [tinyint] NOT NULL,
[dbsId] [int] NOT NULL,
[GatherDate] [date] NOT NULL,
[DatabaseName] [nvarchar] (256) COLLATE Latin1_General_CI_AI NOT NULL,
[LogicalName] [nvarchar] (520) COLLATE Latin1_General_CI_AI NOT NULL,
[FileType] [nvarchar] (120) COLLATE Latin1_General_CI_AI NOT NULL,
[FileSizeGB] [decimal] (15, 2) NOT NULL,
[FreeSpaceInFileGB] [decimal] (15, 2) NOT NULL,
[VolumeLabel] [varchar] (128) COLLATE Latin1_General_CI_AI NOT NULL,
[VolumeTotalSizeGB] [decimal] (15, 2) NOT NULL,
[VolumeAvailableSizeGB] [decimal] (15, 2) NOT NULL,
[FileStateDesc] [nvarchar] (120) COLLATE Latin1_General_CI_AI NOT NULL,
[FileGrowth] [decimal] (15, 2) NOT NULL,
[MaxFileSizeGB] [nvarchar] (120) COLLATE Latin1_General_CI_AI NOT NULL,
[IsReadOnly] [char] (3) COLLATE Latin1_General_CI_AI NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[DatabaseFiles] ADD CONSTRAINT [PK_DatabaseFiles] PRIMARY KEY CLUSTERED ([dbfId]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[DatabaseFiles] ADD CONSTRAINT [FK_DatabaseFiles_Databases] FOREIGN KEY ([srvId], [dbsId]) REFERENCES [dbo].[Databases] ([srvId], [dbsId])
GO
