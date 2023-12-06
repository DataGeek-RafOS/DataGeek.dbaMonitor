CREATE TABLE [baseline].[SystemStatistics]
(
[CollectionDate] [datetime2] NOT NULL,
[SQLServerProcessorTime] [tinyint] NOT NULL,
[SystemIdleProcessorTime] [tinyint] NOT NULL,
[OtherProcessesProcessorTime] [tinyint] NOT NULL,
[TransactionsSec] [decimal] (13, 2) NULL,
[BatchRequestsSec] [decimal] (13, 2) NOT NULL,
[SQLCompilationsSec] [decimal] (13, 2) NOT NULL,
[SQLReCompilationsSec] [decimal] (13, 2) NOT NULL,
[UserConnections] [decimal] (13, 2) NOT NULL,
[ProcessesBlocked] [decimal] (13, 2) NOT NULL,
[ActiveTempTables] [decimal] (13, 2) NOT NULL,
[FullScansSec] [decimal] (13, 2) NOT NULL,
[IndexSearchesSec] [decimal] (13, 2) NOT NULL,
[ForwardedRecordsSec] [decimal] (13, 2) NOT NULL,
[PageSplitsSec] [decimal] (13, 2) NOT NULL,
[FreeListStallsSec] [decimal] (13, 2) NOT NULL,
[LazyWritesSec] [decimal] (13, 2) NOT NULL,
[PageLifeExpectancy] [decimal] (13, 2) NOT NULL,
[PageReadsSec] [decimal] (13, 2) NOT NULL,
[PageWritesSec] [decimal] (13, 2) NOT NULL,
[TotalServerMemoryKB] [decimal] (13, 2) NOT NULL,
[TargetServerMemoryKB] [decimal] (13, 2) NOT NULL,
[MemoryGrantsPending] [decimal] (13, 2) NOT NULL,
[LatchWaitsSec] [decimal] (13, 2) NOT NULL,
[LockWaitsSec] [decimal] (13, 2) NOT NULL,
[NumberOfDeadlocksSec] [decimal] (13, 2) NOT NULL,
[LockWaitTimems] [decimal] (13, 2) NOT NULL,
[DatabaseCount] [smallint] NOT NULL,
[DatabaseTotalSize] [decimal] (13, 2) NOT NULL,
[SignalWaitsPerc] [numeric] (5, 2) NOT NULL CONSTRAINT [DF_SystemStatistics_SignalWaitsPerc] DEFAULT ('0.00'),
[ResourceWaitsPerc] [numeric] (5, 2) NOT NULL CONSTRAINT [DF_SystemStatistics_ResourceWaitsPerc] DEFAULT ('0.00')
) ON [PRIMARY]
GO
ALTER TABLE [baseline].[SystemStatistics] ADD CONSTRAINT [PK_SystemStatistics] PRIMARY KEY CLUSTERED ([CollectionDate]) WITH (FILLFACTOR=90, PAD_INDEX=ON) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [ixNCL_SystemStatistics_CollectionDate] ON [baseline].[SystemStatistics] ([CollectionDate]) ON [PRIMARY]
GO
