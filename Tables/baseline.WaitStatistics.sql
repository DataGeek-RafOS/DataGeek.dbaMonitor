CREATE TABLE [baseline].[WaitStatistics]
(
[Priority] [int] NOT NULL,
[CollectionDate] [datetime2] (0) NOT NULL,
[ComparisonDate] [datetime2] (0) NULL,
[WaitType] [varchar] (120) COLLATE Latin1_General_CI_AI NULL,
[WaitsPerSec] [decimal] (14, 2) NULL,
[ResourceWaitPerSec] [decimal] (14, 2) NULL,
[SignalWaitPerSec] [decimal] (14, 2) NULL,
[WaitCount] [bigint] NULL,
[Percentage] [decimal] (5, 2) NULL,
[AvgWaitsPerSec] [decimal] (14, 2) NULL,
[AvgResourceWaitPerSec] [decimal] (14, 2) NULL,
[AvgSignalWaitPerSec] [decimal] (14, 2) NULL
) ON [PRIMARY]
GO
ALTER TABLE [baseline].[WaitStatistics] ADD CONSTRAINT [PK_WaitStatistics] PRIMARY KEY CLUSTERED ([Priority], [CollectionDate]) ON [PRIMARY]
GO
