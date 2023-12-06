SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [baseline].[spu_GetWaitStatistics] 
AS 
BEGIN

   /***********************************************************************************************   
   **   
   **  Name         : spu_GetWaitStatistics
   **
   **  Database     : dbaMonitor
   **   
   **  Descrição....: 
   **   
   **  Return values: N/A   
   **    
   **  Chamada por..: 
   **   
   **  Parâmetros:   
   **  Entradas           Descrição   
   **  ------------------ -------------------------------------------------------------------------   
   **   
   **   
   **  Saídas             Descrição   
   **  ------------------ -------------------------------------------------------------------------   
   **   
   **  Detalhes: 
   **  ## DMVs consideradas para o Data Capture (dbo) ##
   **  
   **  * sys.dm_os_sys_info
   **       - CPU, memory and SQL Server start time
   **       
   **  * sys.dm_os_sys_memory
   **       - Available physical memory, page file and memory state  
   **       
   **  * sys.dm_os_process_memory
   **       - Memory currently in use, large page allocations and whether OS has notified SQL Server tha memory is low         
   **  
   **  * sys.dm_os_performance_counters
   **       - Current value for a performance counter
   **  
   **  * sys.dm_os_wait_stats
   **       - Aggregated waits for the instance
   **  
   **  * sys.dm_db_file_space_usage
   **       - Lists file size and used space for every database file
   **       - Works for tempdb only prior to SQLServer 2012
   **  
   **  * sys.dm_io_virtual_file_stats
   **       - Reads, writes, latency and current size for every database file
   **  
   **  * sys.dm_db_index_physical_stats
   **       - Size, level of fragmentation, forwarded rows for any index or table
   **  
   **  * sys.dm_db_index_usage_stats
   **       - Cumulative seeks, scans, lookups and updates for an index
   **  
   **  * sys.dm_db_missing_index_details
   **       - Lists indexes the Query Optimizer has determined are missing
   **       - Join with sys.dm_db_missing_index_group_stats to understand cost impact
   **  
   **  * sys.dm_exec_requests
   **       - Lists queries that are currently executing
   **  
   **  * sys.dm_exec_query_stats
   **       - Aggregated statistics for cached query plans including execution count, reads, writes, duration and number of rows returned
   **  
   **  * sys.dm_exec_procedure_stats
   **       - Aggregated statistics for cached stored procedures including execution count, reads, writes and duration
   **  
   **  * sys.dm_exec_sql_text
   **       - Provides the text for a currently executing or previously executed query, based on plan_handle or sql_handle
   **       (commonly obtained from sys.dm_exec_requests)
   **  
   **  * sys.dm_exec_query_plan
   **       - Provides the showplan XML for a currently executing or previously executed query, based on plan_handle 
   **       (commonly obtained from sys.dm_exec_requests)
   **  
   **  ## Other Data
   **  
   **  * System configuration
   **       - sys.configurations (Max memory, Min Memory, Max DOP, Cost Threshould, Optimize for Ad Hoc
   **       - SERVERPROPERTY 
   **       - DBCC TRACESTATUS
   **       - sys.databases
   **  
   **  * Database and file sizes
   **       - sys.master_files
   **       - sys.database_files
   **       - DBCC SQLPERF (Size of log size and usage)
   **  
   **  * Database maintenance history
   **       - msdb.dbo.backupset
   **       - msdb.dbo.sysjobhistory
   ** 
   **   
   **  Autor........: Rafael Rodrigues
   **  Data.........: 16/02/2018
   **
   ************************************************************************************************   
   **  Histórico de Alterações   
   ************************************************************************************************   
   **  Data:    Autor:             Descrição:                                                Versão   
   **  -------- ------------------ --------------------------------------------------------- ------   
   **   
   ************************************************************************************************   
   **            © Rafael Rodrigues de Oliveira Silva. Todos os direitos reservados.   
   ************************************************************************************************/      

     SET NOCOUNT ON 
   
     DECLARE @vn_Error          INT   
           , @vn_RowCount       INT   
           , @vn_TranCount      INT   
           , @vn_ErrorState     INT   
           , @vn_ErrorSeverity  INT   
           , @vc_ErrorProcedure VARCHAR(256)   
           , @vc_ErrorMsg       VARCHAR(MAX);

     DECLARE @vc_SQLCmd            NVARCHAR(MAX)
           , @vc_IgnorableWaits    VARCHAR(MAX);
     
     DECLARE @vn_DaysToPurge_WaitStats INT = 90
           , @vn_DaysToPurge_WaitStatSnapshot int = 30;

     -- Verifica a existência das tabelas
     IF ( SELECT OBJECT_ID('dbaMonitor.dbo.WaitStatistics') ) IS NULL
     BEGIN
          RAISERROR('A tabela dbo.WaitStatistics não foi encontrada no banco de dados [dbaMonitor].', 16, 1);
          RETURN -1;
     END;

     -- Verifica a existência das tabelas
     IF ( SELECT OBJECT_ID('dbaMonitor.baseline.WaitStatisticsSnapshot') ) IS NULL
     BEGIN
          RAISERROR('A tabela baseline.WaitStatisticsSnapshot não foi encontrada no banco de dados [dbaMonitor].', 16, 1);
          RETURN -1;
     END;

	 IF @@TRANCOUNT = 0
		BEGIN TRANSACTION;

     -- Purge Wait Statistics
     DELETE FROM dbo.WaitStatistics
     WHERE CollectionDate < DATEADD(DAY, (-1 * @vn_DaysToPurge_WaitStats), CURRENT_TIMESTAMP);

     IF @@ERROR = 0
     BEGIN
          COMMIT TRANSACTION;
     END 
     ELSE
     BEGIN
          ROLLBACK TRANSACTION;
     END;

     -- Tabela de Waits Types que não são relevantes do ponto de vista de performance
     CREATE TABLE #IgnorableWaits
     ( wait_type NVARCHAR(256) PRIMARY KEY );

     INSERT INTO #IgnorableWaits
               ( wait_type )
          VALUES    
               (N'BROKER_EVENTHANDLER'),
               (N'BROKER_RECEIVE_WAITFOR'),
               (N'BROKER_TASK_STOP'),
               (N'BROKER_TO_FLUSH'),
               (N'BROKER_TRANSMITTER'),
               (N'CHECKPOINT_QUEUE'), 
               (N'CHKPT'),
               (N'CLR_AUTO_EVENT'), 
               (N'CLR_MANUAL_EVENT'),
               (N'CLR_SEMAPHORE'),
               (N'DBMIRROR_DBM_EVENT'),
               (N'DBMIRROR_EVENTS_QUEUE'),
               (N'DBMIRROR_WORKER_QUEUE'),
               (N'DBMIRRORING_CMD'), 
               (N'DIRTY_PAGE_POLL'),
               (N'DISPATCHER_QUEUE_SEMAPHORE'),
               (N'EXECSYNC'),
               (N'FSAGENT'),
               (N'FT_IFTS_SCHEDULER_IDLE_WAIT'),
               (N'FT_IFTSHC_MUTEX'),
               (N'HADR_CLUSAPI_CALL'),
               (N'HADR_FILESTREAM_IOMGR_IOCOMPLETION'),
               (N'HADR_LOGCAPTURE_WAIT'),
               (N'HADR_NOTIFICATION_DEQUEUE'),
               (N'HADR_TIMER_TASK'), 
               (N'HADR_WORK_QUEUE'),
               (N'KSOURCE_WAKEUP'), 
               (N'LAZYWRITER_SLEEP'),
               (N'LOGMGR_QUEUE'),
               (N'ONDEMAND_TASK_QUEUE'),
               (N'PWAIT_ALL_COMPONENTS_INITIALIZED'),
               (N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP'),
               (N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'),
               (N'QDS_SHUTDOWN_QUEUE'),
               (N'REQUEST_FOR_DEADLOCK_SEARCH'),
               (N'RESOURCE_QUEUE'),
               (N'SERVER_IDLE_CHECK'),
               (N'SLEEP_BPOOL_FLUSH'),
               (N'SLEEP_DBSTARTUP'),
               (N'SLEEP_DCOMSTARTUP'),
               (N'SLEEP_MASTERDBREADY'),
               (N'SLEEP_MASTERMDREADY'),
               (N'SLEEP_MASTERUPGRADED'),
               (N'SLEEP_MSDBSTARTUP'),
               (N'SLEEP_SYSTEMTASK'), 
               (N'SLEEP_TASK'),
               (N'SLEEP_TEMPDBSTARTUP'),
               (N'SNI_HTTP_ACCEPT'),
               (N'SP_SERVER_DIAGNOSTICS_SLEEP'),
               (N'SQLTRACE_BUFFER_FLUSH'),
               (N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP'),
               (N'SQLTRACE_WAIT_ENTRIES'),
               (N'WAIT_FOR_RESULTS'), 
               (N'WAITFOR'),
               (N'WAITFOR_TASKSHUTDOWN'),
               (N'WAIT_XTP_HOST_WAIT'),
               (N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG'),
               (N'WAIT_XTP_CKPT_CLOSE'),
               (N'XE_DISPATCHER_JOIN'),
               (N'XE_DISPATCHER_WAIT'),
               (N'XE_TIMER_EVENT'),
               (N'QDS_ASYNC_QUEUE');


      -- Pivot dinâmico de Wait types a serem desconsiderados
      SELECT @vc_IgnorableWaits = '' + STUFF(( SELECT TOP 100 PERCENT ''', N''' + ISNULL(wait_type, '') + ''
                                               FROM #IgnorableWaits
                                               FOR XML PATH('')
                                             ), 1, 2, '');

     SET @vc_IgnorableWaits += '''';

     -- Controle de Transações 
     SET @vn_TranCount = @@TRANCOUNT;
   
     BEGIN TRY;

          -- Insere as estatísticas
          INSERT INTO dbo.WaitStatistics
                   (
                     [Priority]
                   , [CollectionDate]
                   , [ComparisonDate]
                   , [WaitType]
                   , [WaitsPerSec]
                   , [ResourceWaitPerSec]
                   , [SignalWaitPerSec]
                   , [WaitCount] 
                   , [Percentage]
                   , [AvgWaitsPerSec]
                   , [AvgResourceWaitPerSec]
                   , [AvgSignalWaitPerSec]    
                   )
          EXEC ('WITH [Waits] 
                 AS
                 ( SELECT curr.wait_type AS wait_type
                        , curr.waiting_tasks_count - ISNULL(snap.waiting_tasks_count, 0) AS WaitingTasksCount_Delta
                        , curr.wait_time_ms - ISNULL(snap.wait_time_ms, 0) AS WaitTime_ms_Delta
                        , curr.max_wait_time_ms - ISNULL(snap.max_wait_time_ms, 0) AS MaxWaitTime_ms_Delta
                        , curr.signal_wait_time_ms - ISNULL(snap.signal_wait_time_ms, 0) AS SignalWaitTime_ms_Delta
                        , MIN(snap.collection_date) AS interval_start
                        , MAX(CURRENT_TIMESTAMP) AS interval_end
                   FROM sys.dm_os_wait_stats curr
                        LEFT JOIN
                        baseline.WaitStatisticsSnapshot snap
                             ON  snap.wait_type = curr.wait_type
                             AND snap.collection_date IN ( SELECT MAX(collection_date)
                                                           FROM baseline.WaitStatisticsSnapshot
                                                           WHERE collection_date < DATEADD(HOUR, -24, CURRENT_TIMESTAMP)
                                                         ) 
                   WHERE curr.wait_type NOT IN ( ' + @vc_IgnorableWaits + ')
                   GROUP BY curr.wait_type
                          , curr.waiting_tasks_count
                          , curr.wait_time_ms
                          , curr.max_wait_time_ms
                          , curr.signal_wait_time_ms
                          , snap.collection_date
                          , snap.waiting_tasks_count
                          , snap.wait_time_ms
                          , snap.max_wait_time_ms
                          , snap.signal_wait_time_ms
                   HAVING (curr.waiting_tasks_count - snap.waiting_tasks_count) > 0
                 )

                 SELECT TOP (15)
                        Priority = ROW_NUMBER() OVER (ORDER BY Percentage DESC)
                      , CollectionDate
                      , ComparisonDate
                      , WaitType
                      , WaitsPerSec
                      , ResourceWaitBySec
                      , SignalWaitBySec
                      , WaitCount 
                      , Percentage = CONVERT(NUMERIC(5,2), Percentage)
                      , AvgWaitsBySec
                      , AvgResourceWaitBySec
                      , AvgSignalWaitBySec 
                 FROM (
                      SELECT [ComparisonDate] = interval_start 
                           , [CollectionDate] = interval_end
                           , [WaitType] = wait_type
                           , [WaitsPerSec] = CAST((WaitTime_ms_Delta / 1000.0) AS DECIMAL(14, 2)) 
                           , [ResourceWaitBySec] = CAST(((WaitTime_ms_Delta - SignalWaitTime_ms_Delta) / 1000.0) AS DECIMAL(14, 2)) 
                           , [SignalWaitBySec] = CAST((SignalWaitTime_ms_Delta / 1000.0) AS DECIMAL(14, 2)) 
                           , [WaitCount] = WaitingTasksCount_Delta
                           , [Percentage] = 100.0 * WaitTime_ms_Delta / SUM (WaitTime_ms_Delta) OVER()
                           , [AvgWaitsBySec] = CAST(((WaitTime_ms_Delta / 1000.0) / WaitingTasksCount_Delta) AS DECIMAL (14, 4)) 
                           , [AvgResourceWaitBySec] = CAST((((WaitTime_ms_Delta - SignalWaitTime_ms_Delta) / 1000.0) / WaitingTasksCount_Delta) AS DECIMAL (14, 4)) 
                           , [AvgSignalWaitBySec] = CAST (((SignalWaitTime_ms_Delta / 1000.0) / WaitingTasksCount_Delta) AS DECIMAL (14, 4)) 
                      FROM Waits
                      ) _
                 WHERE Percentage >= 1.0
                 ORDER BY [Percentage] DESC
                 OPTION (RECOMPILE);'
            ); 

          -- Limpa as Wait Stats do sistema para agregação até a próxima coleta
          BEGIN TRY

               -- Remove o snapshot de waits stats salvo
               DELETE FROM dbaMonitor.baseline.WaitStatisticsSnapshot
               WHERE collection_date < DATEADD(DAY, (-1 * @vn_DaysToPurge_WaitStatSnapshot), CURRENT_TIMESTAMP);;

               -- Captura as wait stats de hoje para uso futuro
               INSERT INTO baseline.WaitStatisticsSnapshot 
                         (
                           wait_type
                         , collection_date
                         , waiting_tasks_count
                         , wait_time_ms
                         , max_wait_time_ms
                         , signal_wait_time_ms
                         )
               EXEC ('SELECT wait_type
                           , CURRENT_TIMESTAMP
                           , CONVERT(BIGINT, waiting_tasks_count)
                           , wait_time_ms
                           , max_wait_time_ms
                           , signal_wait_time_ms
                      FROM sys.dm_os_wait_stats
                      WHERE [waiting_tasks_count] > 0
                      AND   [wait_type] NOT IN ( ' + @vc_IgnorableWaits + ');'
                    );

          END TRY
          BEGIN CATCH
               SET @vc_ErrorMsg = ERROR_MESSAGE();
               RAISERROR('Falha ao limpar os Wait Stats do servidor. [%s]', 16, 1, @vc_ErrorMsg) WITH NOWAIT;
          END CATCH;

     END TRY 
  
     BEGIN CATCH 
  
          -- Recupera informações originais do erro 
          SELECT @vc_ErrorMsg       = ERROR_MESSAGE() 
               , @vn_ErrorSeverity  = ERROR_SEVERITY() 
               , @vn_ErrorState     = ERROR_STATE() 
               , @vc_ErrorProcedure = ERROR_PROCEDURE(); 
  
          -- Tratamento Para ErrorState, retorna a procedure de execução em junção com o erro. 
          SELECT @vc_ErrorMsg = CASE WHEN @vn_ErrorState = 1 
                                     THEN @vc_ErrorMsg + CHAR(13) + 'O erro ocorreu em ' + @vc_ErrorProcedure + ' ( ' + LTRIM(RTRIM(STR(ERROR_LINE() ) ) ) + ' )'
                                     WHEN @vn_ErrorState = 3 
                                     THEN @vc_ErrorProcedure + ' - ' + @vc_ErrorMsg 
                                     ELSE @vc_ErrorMsg 
                                END; 
  
          RAISERROR ( @vc_ErrorMsg 
                    , @vn_ErrorSeverity 
                    , @vn_ErrorState ); 
  
          IF  @vn_TranCount  = 0   -- Transação feita no escopo da procedure 
          AND XACT_STATE() != 0      -- Transação ativa existente 
          BEGIN
               ROLLBACK TRANSACTION; 
          END
  
     END CATCH 
     
     IF  @vn_TranCount  = 0  -- Transação feita no escopo da procedure 
     AND XACT_STATE()  = 1     -- Transação com sucesso de execução 
     BEGIN
          COMMIT TRANSACTION; 
     END
  
END
GO
