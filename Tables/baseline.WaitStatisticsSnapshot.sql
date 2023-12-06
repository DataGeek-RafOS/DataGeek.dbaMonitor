CREATE TABLE [baseline].[WaitStatisticsSnapshot]
(
[wait_type] [varchar] (120) COLLATE Latin1_General_CI_AI NOT NULL,
[collection_date] [datetime2] (0) NOT NULL,
[waiting_tasks_count] [bigint] NULL,
[wait_time_ms] [bigint] NULL,
[max_wait_time_ms] [bigint] NULL,
[signal_wait_time_ms] [bigint] NULL
) ON [PRIMARY]
GO
ALTER TABLE [baseline].[WaitStatisticsSnapshot] ADD CONSTRAINT [PK_WaitStatisticsSnapshot] PRIMARY KEY CLUSTERED ([collection_date], [wait_type]) ON [PRIMARY]
GO
