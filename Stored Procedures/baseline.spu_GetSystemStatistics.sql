SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [baseline].[spu_GetSystemStatistics] 
( 
  @Interval SMALLINT = 15 -- seconds
) 
AS 
BEGIN

   /***********************************************************************************************   
   **   
   **  Name         : spu_Baseline_GetSystemStatistics
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
   **  ## DMVs consideradas para o Data Capture (Baseline) ##
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
   **  Data.........: 16/12/2013
   **
   ************************************************************************************************   
   **  Histórico de Alterações   
   ************************************************************************************************   
   **  Data:    Autor:             Descrição:                                                Versão   
   **  -------- ------------------ --------------------------------------------------------- ------   
   **   
   ************************************************************************************************   
   **      © Conselho Federal da Ordem dos Advogados do Brasil. Todos os direitos reservados.   
   ************************************************************************************************/      

     SET NOCOUNT ON 
   
     DECLARE @vn_Error          INT   
           , @vn_RowCount       INT   
           , @vn_TranCount      INT   
           , @vn_ErrorState     INT   
           , @vn_ErrorSeverity  INT   
           , @vc_ErrorProcedure VARCHAR(256)   
           , @vc_ErrorMsg       VARCHAR(MAX);

     DECLARE @vc_SQLCmd                      NVARCHAR(MAX)
           , @vd_StartTime                   DATETIME2
           , @vd_SQLStatsDuration            INT 
           , @vd_CollectionDate              DATETIME2
           , @vn_SQLServerProcessorTime      TINYINT
           , @vn_SystemIdleProcessorTime     TINYINT
           , @vn_OtherProcessesProcessorTime TINYINT
           , @vn_DBCount                     SMALLINT
           , @vn_DBSize                      INT
           , @vc_CounterPrefix               NVARCHAR(30)
           , @vd_DelayDeltaSec               DATETIME
           , @vn_SignalWaitsPerc             NUMERIC(5,2)
           , @vn_ResourceWaitsPerc           NUMERIC(5,2)

     -- Verifica a existência das tabelas
     IF ( SELECT OBJECT_ID('dbaMonitor.baseline.SystemStatistics') ) IS NULL
     BEGIN
          RAISERROR('A tabela baseline.SystemStatistics não foi encontrada no banco de dados [dbaMonitor].', 16, 1);
          RETURN -1;
     END

     -- Identifica o prefixo para captura dos contadores
     SET @vc_CounterPrefix = IIF(@@SERVICENAME = 'MSSQLSERVER', 'SQLServer:', 'MSSQL$' + @@SERVICENAME +':');

     -- Converte o parâmetro de entrada no intervalo de espera entre os coletores
     SELECT @vd_DelayDeltaSec = DATEADD(s, @Interval, '00:00:00');

     -- Recupera informações de arquivos de BD. Tratamento para o SQL Azure
	IF (OBJECT_ID('tempdb..#MasterFiles') IS NOT NULL)
     BEGIN     
          DROP TABLE #MasterFiles;
     END 

     -- Arquivos de bancos de dados
	CREATE TABLE #MasterFiles 
     ( database_id INT
     , file_id INT
     , type_desc NVARCHAR(50)
     , name NVARCHAR(255)
     , physical_name NVARCHAR(255)
     , size BIGINT
     );

	IF CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) = 'SQL Azure'
     BEGIN
		SET @vc_SQLCmd = 'INSERT INTO #MasterFiles (database_id, file_id, type_desc, name, physical_name, size) SELECT DB_ID(), file_id, type_desc, name, physical_name, size FROM sys.database_files;'
     END
	ELSE
     BEGIN
		SET @vc_SQLCmd = 'INSERT INTO #MasterFiles (database_id, file_id, type_desc, name, physical_name, size) SELECT database_id, file_id, type_desc, name, physical_name, size FROM sys.master_files;'
     END

	EXEC(@vc_SQLCmd);


     -- Tabela de Waits Types não importantes (podem ser ignorados)
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
               (N'XE_TIMER_EVENT');

     -- Controle de Transações 
     SET @vn_TranCount = @@TRANCOUNT;
   
     BEGIN TRY;

          /* Recupera utilização de CPU */
          SELECT @vn_SQLServerProcessorTime      = [SQLServerProcessCpuUtilization]
               , @vn_SystemIdleProcessorTime     = [SystemIdleProcess]
               , @vn_OtherProcessesProcessorTime = [OtherProcessCpuUtilization]
          FROM (
               SELECT TOP (1) -- Mais recente
                      SQLProcessUtilization AS [SQLServerProcessCpuUtilization] 
                    , SystemIdle AS [SystemIdleProcess]
                    , 100 - SystemIdle - SQLProcessUtilization AS [OtherProcessCpuUtilization]
               FROM (
                    SELECT record.value ('(./Record/@id)[1]', 'int') AS record_id 
                         , record.value( '(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]' , 'INT' )AS [SystemIdle]
                         , record.value( '(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]' , 'INT' ) AS [SQLProcessUtilization], [timestamp]
                    FROM (
                         SELECT [timestamp]
                              , CONVERT(xml , record) AS [record]
                         FROM sys.dm_os_ring_buffers WITH (NOLOCK)
                         WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                         AND   record LIKE N'%<SystemHealth>%'
                         ) AS RingBuffers 
                    ) AS xmlRingBuf
               ORDER BY record_id DESC 
               ) AS CPU
          OPTION (RECOMPILE);


          /* Contagem de banco de dados */
          SELECT @vn_DBCount = COUNT(1)
          FROM sys.databases
          WHERE database_id > 4;

          /* Tamanho acumulado dos bancos de dados */
          SELECT @vn_DBSize = SUM(size) * 8./ 1024. /1024. 
          FROM #MasterFiles
          WHERE database_id > 4
	
          /* Signal Waits vs Resource Waits */
          SELECT @vn_SignalWaitsPerc  = CAST(100.0 * SUM(signal_wait_time_ms) / SUM(wait_time_ms) AS NUMERIC(5,2)) 
               , @vn_ResourceWaitsPerc = CAST(100.0 * SUM(wait_time_ms - signal_wait_time_ms) / SUM(wait_time_ms) AS NUMERIC(5, 2)) 
          FROM sys.dm_os_wait_stats WITH (NOLOCK)
          WHERE wait_type NOT IN (SELECT wait_type FROM #IgnorableWaits);

          SET @vd_CollectionDate = SYSDATETIME();

          -- Recupera Performance Monitor Counters
          /* Perfmon Counter Analysis

          Access Methods: Modo como as tabelas estão sendo acessadas (FullScans * 800-1000 = IndexSearch)   
               SQLServer:Access Methods\Full Scans/sec
               SQLServer:Access Methods\Index Searches/sec

          Buffer Manager: Memory Pressure
          SQLServer:Buffer Manager\Lazy Writes/sec
          SQLServer:Buffer Manager\Page life expectancy ('sp_configure max server memory'/4 * 300, 4 quer dizer 4GB)
          SQLServer:Buffer Manager\Free list stalls/sec

          SQLServer:General Statistics\Processes Blocked
          SQLServer:General Statistics\User Connections
          SQLServer:Locks\Lock Waits/sec
          SQLServer:Locks\Lock Wait Time (ms)

          Memory Manager: Falta de memória ou grande uso de sorts e hashes (spill)
               SQLServer:Memory Manager\Memory Grants Pending

          SQL Statistics: Proporção entre Batch Requests e Compilações (Ad Hoc Workload sem uso adequado do plan cache) e ReCompilações (Codicação pobre)
               SQLServer:SQL Statistics\Batch Requests/sec
               SQLServer:SQL Statistics\SQL Compilations/sec
               SQLServer:SQL Statistics\SQL Re-Compilations/sec

          */

          -- Captura o primeiro conjunto de dados - Performance Counters
          SELECT CAST (1 AS INT) AS collection_instance 
               , [object_name] 
               , counter_name 
               , instance_name 
               , cntr_value 
               , cntr_type 
               , CURRENT_TIMESTAMP AS collection_time
          INTO #BaseSetPerfCounters
          FROM sys.dm_os_performance_counters
          WHERE -- Access Methods
              ( 
              object_name = @vc_CounterPrefix + 'Access Methods'
          AND counter_name = 'Full Scans/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Access Methods'
          AND counter_name = 'Index Searches/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Access Methods'
          AND counter_name = 'Forwarded Records/sec                                                                                                           '
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Access Methods'
          AND counter_name = 'Page Splits/sec'
              )
               -- Buffer Manager
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Free List Stalls/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Lazy Writes/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Page life expectancy'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Page Reads/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Page Writes/sec'
              )
               -- Memory Manager
          OR  ( 
              object_name = @vc_CounterPrefix + 'Memory Manager'
          AND counter_name = 'Total Server Memory (KB)'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Memory Manager'
          AND counter_name = 'Target Server Memory (KB)'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Memory Manager'
          AND counter_name = 'Memory Grants Pending'
              )
               -- SQL Statistics
          OR  ( 
              object_name = @vc_CounterPrefix + 'SQL Statistics'
          AND counter_name = 'Batch Requests/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'SQL Statistics'
          AND counter_name = 'SQL Compilations/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'SQL Statistics'
          AND counter_name = 'SQL Re-Compilations/sec'
              )
               -- General Statistics
          OR  ( 
              object_name = @vc_CounterPrefix + 'General Statistics'
          AND counter_name = 'Processes Blocked'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'General Statistics'
          AND counter_name = 'User Connections'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'General Statistics'
          AND counter_name = 'Active Temp Tables'
              )
               -- Latches
          OR  ( 
              object_name = @vc_CounterPrefix + 'Latches'
          AND counter_name = 'Latch Waits/sec'
              )
               -- Locks
          OR  ( 
              object_name = @vc_CounterPrefix + 'Locks'
          AND counter_name = 'Lock Waits/sec'
          AND instance_name = '_Total'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Locks'
          AND counter_name = 'Lock Waits/sec'
          AND instance_name = '_Total'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Locks'
          AND counter_name = 'Number of Deadlocks/sec'
          AND instance_name = '_Total'
              )          
          OR  ( 
              object_name = @vc_CounterPrefix + 'Locks'
          AND counter_name = 'Lock Wait Time (ms)'
          AND instance_name = '_Total'
              )
               -- Databases
          OR  ( 
              object_name = @vc_CounterPrefix + 'Databases'
          AND counter_name = 'Transactions/sec'
          AND instance_name = '_Total'
              );

          -- Marca o tempo de execução para que seja subtraído do delay das demais estatístics
          SET @vd_StartTime = SYSDATETIME();

          SELECT @vd_SQLStatsDuration = DATEDIFF(s, @vd_StartTime, SYSDATETIME());

          -- Se o tempo para gravar o resultado da proc. anterior for menor que o Delta Delay configurado, subtrai e aguarda até o tempo configurado
          IF (DATEADD(s, @vd_SQLStatsDuration, '00:00:00') <= @vd_DelayDeltaSec)
          BEGIN
               SET @vd_DelayDeltaSec = DATEADD(s, -1 * @vd_SQLStatsDuration, @vd_DelayDeltaSec)
     
               -- Intervalo entre as coleções
               WAITFOR DELAY @vd_DelayDeltaSec;

          END 

          -- Captura o segundo conjunto de dados - Performance Counters
          SELECT CAST (2 AS INT) AS collection_instance 
               , [object_name] 
               , counter_name 
               , instance_name 
               , cntr_value 
               , cntr_type 
               , CURRENT_TIMESTAMP AS collection_time
          INTO #LastSetPerfCounters
          FROM sys.dm_os_performance_counters
          WHERE -- Access Methods
              ( 
              object_name = @vc_CounterPrefix + 'Access Methods'
          AND counter_name = 'Full Scans/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Access Methods'
          AND counter_name = 'Index Searches/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Access Methods'
          AND counter_name = 'Forwarded Records/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Access Methods'
          AND counter_name = 'Page Splits/sec'
              )
               -- Buffer Manager
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Free List Stalls/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Lazy Writes/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Page life expectancy'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Page Reads/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Buffer Manager'
          AND counter_name = 'Page Writes/sec'
              )
               -- Memory Manager
          OR  ( 
              object_name = @vc_CounterPrefix + 'Memory Manager'
          AND counter_name = 'Total Server Memory (KB)'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Memory Manager'
          AND counter_name = 'Target Server Memory (KB)'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Memory Manager'
          AND counter_name = 'Memory Grants Pending'
              )
               -- SQL Statistics
          OR  ( 
              object_name = @vc_CounterPrefix + 'SQL Statistics'
          AND counter_name = 'Batch Requests/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'SQL Statistics'
          AND counter_name = 'SQL Compilations/sec'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'SQL Statistics'
          AND counter_name = 'SQL Re-Compilations/sec'
              )
               -- General Statistics
          OR  ( 
              object_name = @vc_CounterPrefix + 'General Statistics'
          AND counter_name = 'Processes Blocked'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'General Statistics'
          AND counter_name = 'User Connections'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'General Statistics'
          AND counter_name = 'Active Temp Tables'
              )
               -- Latches
          OR  ( 
              object_name = @vc_CounterPrefix + 'Latches'
          AND counter_name = 'Latch Waits/sec'
              )
               -- Locks
          OR  ( 
              object_name = @vc_CounterPrefix + 'Locks'
          AND counter_name = 'Lock Waits/sec'
          AND instance_name = '_Total'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Locks'
          AND counter_name = 'Lock Waits/sec'
          AND instance_name = '_Total'
              )
          OR  ( 
              object_name = @vc_CounterPrefix + 'Locks'
          AND counter_name = 'Number of Deadlocks/sec'
          AND instance_name = '_Total'
              )          
          OR  ( 
              object_name = @vc_CounterPrefix + 'Locks'
          AND counter_name = 'Lock Wait Time (ms)'
          AND instance_name = '_Total'
              )
               -- Databases
          OR  ( 
              object_name = @vc_CounterPrefix + 'Databases'
          AND counter_name = 'Transactions/sec'
          AND instance_name = '_Total'
              )

          -- Comparação e Inclusão dos dados do baseline com contadores acumulados

          -- Estatísticas do servidor
          INSERT INTO baseline.SystemStatistics
                    (
                      CollectionDate
                    , SQLServerProcessorTime
                    , SystemIdleProcessorTime
                    , OtherProcessesProcessorTime
                    , TransactionsSec
                    , BatchRequestsSec
                    , SQLCompilationsSec
                    , SQLReCompilationsSec
                    , UserConnections
                    , ProcessesBlocked
                    , ActiveTempTables
                    , FullScansSec
                    , IndexSearchesSec
                    , ForwardedRecordsSec
                    , PageSplitsSec
                    , FreeListStallsSec
                    , LazyWritesSec
                    , PageLifeExpectancy
                    , PageReadsSec
                    , PageWritesSec
                    , TotalServerMemoryKB
                    , TargetServerMemoryKB
                    , MemoryGrantsPending
                    , LatchWaitsSec
                    , LockWaitsSec
                    , NumberOfDeadlocksSec
                    , LockWaitTimems
                    , DatabaseCount
                    , DatabaseTotalSize
                    , SignalWaitsPerc
                    , ResourceWaitsPerc
                    )
          SELECT [CollectionDate] = @vd_CollectionDate
               , [SQLServerProcessorTime] = @vn_SQLServerProcessorTime -- % Time
               , [SystemIdleProcessorTime] = @vn_SystemIdleProcessorTime     
               , [OtherProcessesProcessorTime] = @vn_OtherProcessesProcessorTime 
               -- SQL Databases
               , [Transactions/sec (SQLServer:Databases)]
               -- SQL Statistics
               , [Batch Requests/sec (SQLServer:SQL Statistics)]
               , [SQL Compilations/sec (SQLServer:SQL Statistics)]
               , [SQL Re-Compilations/sec (SQLServer:SQL Statistics)]
               -- General Statistics
               , [User Connections (SQLServer:General Statistics)]
               , [Processes Blocked (SQLServer:General Statistics)]
               , [Active Temp Tables (SQLServer:General Statistics)]
               -- Acess Methods
               , [Full Scans/sec (SQLServer:Access Methods)]
               , [Index Searches/sec (SQLServer:Access Methods)]
               , [Forwarded Records/sec (SQLServer:Access Methods)]
               , [Page Splits/sec (SQLServer:Access Methods)]
               -- Buffer Manager
               , [Free List Stalls/sec (SQLServer:Buffer Manager)]
               , [Lazy Writes/sec (SQLServer:Buffer Manager)]
               , [Page life expectancy (SQLServer:Buffer Manager)]
               , [Page Reads/sec (SQLServer:Buffer Manager)]
               , [Page Writes/sec (SQLServer:Buffer Manager)]
               -- Memory Manager 
               , [Total Server Memory (KB) (SQLServer:Memory Manager)]
               , [Target Server Memory (KB) (SQLServer:Memory Manager)]
               , [Memory Grants Pending (SQLServer:Memory Manager)]
               -- Latches
               , [Latch Waits/sec (SQLServer:Latches)]
               -- Locks
               , [Lock Waits/sec (SQLServer:Locks)]
               , [Number of Deadlocks/sec (SQLServer:Locks)]
               , [Lock Wait Time (ms) (SQLServer:Locks)]
               , [Database Count] = @vn_DBCount
               , [Database Size] = @vn_DBSize
               , [% Signal (CPU) Waits] = @vn_SignalWaitsPerc
               , [% Resource Waits] = @vn_ResourceWaitsPerc
          FROM (
               SELECT CounterName, CONVERT(DECIMAL(13,2), CounterValue) AS CounterValue
               FROM (
                    SELECT CounterName   = LTRIM(RTRIM(Delta.counter_name)) + ' (' + LTRIM(RTRIM(Delta.object_name)) + ')'
                         , CounterValue = REPLACE(FORMAT(Delta.cntr_value, 'N'), ',', '')
                    FROM (
                         SELECT Base.object_name
                              , Base.counter_name
                              , Base.instance_name
                              , CASE WHEN Base.cntr_type = 272696576 /* PERF_COUNTER_BULK_COUNT (272696576) – Average number of operations completed during each second of the sample interval. */
                                     THEN (Final.cntr_value - Base.cntr_value) / CONVERT(DECIMAL, @Interval)
                                     WHEN Base.cntr_type = 65792 /* PERF_COUNTER_LARGE_RAWCOUNT (65792) – Returns the last observed value for the counter. */
                                     THEN Final.cntr_value
                                END AS cntr_value
                         FROM #BaseSetPerfCounters AS Base
                              INNER JOIN 
                              #LastSetPerfCounters AS Final
                                   ON  Final.collection_instance = Base.collection_instance + 1
                                   AND Final.object_name   = Base.object_name
                                   AND Final.counter_name  = Base.counter_name
                                   AND Final.instance_name = Base.instance_name
                         ) AS Delta
                    ) AS Perf
               ) AS Report
          PIVOT ( MAX(CounterValue) 
                  FOR CounterName 
                  IN ( -- SQL Statistics
                       [Batch Requests/sec (SQLServer:SQL Statistics)]
                     , [SQL Compilations/sec (SQLServer:SQL Statistics)]
                     , [SQL Re-Compilations/sec (SQLServer:SQL Statistics)]
                     -- SQL Databases
                     , [Transactions/sec (SQLServer:Databases)]
                     -- General Statistics
                     , [User Connections (SQLServer:General Statistics)]
                     , [Processes Blocked (SQLServer:General Statistics)]
                     , [Active Temp Tables (SQLServer:General Statistics)]
                     -- Acess Methods
                     , [Full Scans/sec (SQLServer:Access Methods)]
                     , [Index Searches/sec (SQLServer:Access Methods)]
                     , [Forwarded Records/sec (SQLServer:Access Methods)]
                     , [Page Splits/sec (SQLServer:Access Methods)]
                       -- Buffer Manager
                     , [Free List Stalls/sec (SQLServer:Buffer Manager)]
                     , [Lazy Writes/sec (SQLServer:Buffer Manager)]
                     , [Page life expectancy (SQLServer:Buffer Manager)]
                     , [Page Reads/sec (SQLServer:Buffer Manager)]
                     , [Page Writes/sec (SQLServer:Buffer Manager)]
                     -- Memory Manager 
                     , [Total Server Memory (KB) (SQLServer:Memory Manager)]
                     , [Target Server Memory (KB) (SQLServer:Memory Manager)]
                     , [Memory Grants Pending (SQLServer:Memory Manager)]
                     -- Latches
                     , [Latch Waits/sec (SQLServer:Latches)]
                     -- Locks
                     , [Lock Waits/sec (SQLServer:Locks)]
                     , [Number of Deadlocks/sec (SQLServer:Locks)]
                     , [Lock Wait Time (ms) (SQLServer:Locks)]
                     )
                ) AS PvtPerf;

          -- Remove as tabelas temporárias
          DROP TABLE #BaseSetPerfCounters;
          DROP TABLE #LastSetPerfCounters;

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
