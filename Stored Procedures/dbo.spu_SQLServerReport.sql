SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[spu_SQLServerReport]
(
  @pc_EmailOperator  VARCHAR(2000)     
, @pc_EmailProfile   VARCHAR(100)  = NULL
, @pc_CheckBackupSet BIT           = 0         
)
WITH EXECUTE AS CALLER
AS
BEGIN
/******************************************************************************************************
**
**  Name.........: spu_SQLServerReport
**
**  Descrição....: Gera um relatório de informações sobre o servidor em HTML para envio por E-mail.
**
**  Return values:
**
**  Chamada por..:
**
**  Parâmetros:
**  Entradas            Descrição
**  ------------------- -------------------------------------------------------------------------------
**  @pc_EmailOperator   Operadores que deverão receber o e-mail
**  @pc_EmailProfile    Profile de E-mail do SQL Agent (se não informado, qualquer um será utilizado)
**  @pc_CheckBackupSet  Indicador de retorno de informações de Backup Set 
**
**  Saídas              Descrição
**  ------------------- -------------------------------------------------------------------------------
**
**  Observações..:
**
**  Autor........: Rafael Rodrigues
**  Data.........: 18/02/2015
******************************************************************************************************
**  Histórico de Alterações
******************************************************************************************************
** Data:    Autor:                Descrição:                                                    Versão
** -------- --------------------- ------------------------------------------------------------- ------
**
**
******************************************************************************************************/
 
   SET NOCOUNT ON
   SET ANSI_WARNINGS ON -- Remoção causará erro
 
   DECLARE @vn_Error           INT
         , @vn_RowCount        INT
         , @vn_TranCount       INT
         , @vn_ErrorState      INT
         , @vn_ErrorSeverity   INT
         , @vc_ErrorProcedure  VARCHAR(256)
         , @vc_ErrorMsg        VARCHAR(MAX);                                                      
    
   DECLARE @vc_dSQL                NVARCHAR(MAX)
         , @vc_HtmlScript          VARCHAR(MAX)
         , @vc_HtmlTableStyle      VARCHAR(MAX)
         , @vc_HtmlThStyle         VARCHAR(MAX)
         , @vc_HtmlTdStyle         VARCHAR(MAX)
         , @vx_DiskInfo            XML
         , @vx_SpaceUsedBD         XML
         , @vx_SpaceUsedFile       XML
         , @vc_AlertThreshold      TINYINT
         , @vc_Server              VARCHAR(100)
         , @vc_ServerIP            VARCHAR(21)
         , @vc_Version             VARCHAR(250)
         , @vc_Edition             VARCHAR(100)
         , @vc_ServicePack         VARCHAR(100)
         , @vc_Subject             VARCHAR(100)
         , @vc_Collation           VARCHAR(100)
         , @vc_IsClustered         CHAR(3)
         , @vd_BackupStart         DATETIME
         , @vd_BackupEnd           DATETIME
         , @vd_JobStart            DATETIME
         , @vd_JobEnd              DATETIME
         , @vn_MinRuntimeSec       INT
         , @vd_LastRestart         CHAR(16)
         , @vn_IdleCPUPercent      INT
         , @vn_IdleCPUDuration     INT
         , @vc_QtdDeadlocks        VARCHAR(15);

   -- Variáveis utilizadas na rotina do anexo
   DECLARE @vd_cGraphDateStartDate DATETIME
         , @vd_cGraphDateEndDate   DATETIME
         , @vn_GraphId             INT = 1
         , @vn_CategoryCount       INT = 0
         , @vn_JobCountByCategory  INT = 1
         , @vn_WidthInPixels       INT = 1280 -- Largura do gráfico
         , @vn_HeightInPixels      INT
         , @vn_RowHeightInPixels   INT = 28 -- base para cálculo de altura do gráfico

   -- Atribução de variáveis
   SELECT @vc_AlertThreshold  = 10
        , @vc_Server          = UPPER(RTRIM(LTRIM(CONVERT(VARCHAR(50),SERVERPROPERTY('ServerName')))))
        , @vc_ServerIP        = RTRIM(CONVERT(CHAR(16), CONNECTIONPROPERTY('local_net_address'))) + ':' + CONVERT(CHAR(5), CONNECTIONPROPERTY('local_tcp_port'))
        , @vc_Subject         = 'Gerenciamento de Servidores de Bancos de Dados - SQL Server ( ' + @vc_Server + ' )'
        , @vc_Version         = @@VERSION
        , @vc_Edition         = CONVERT( VARCHAR( 100), SERVERPROPERTY('Edition' ) )
        , @vc_ServicePack     = CONVERT( VARCHAR( 100), SERVERPROPERTY ( 'ProductLevel') )
        , @vc_Collation       = CONVERT( VARCHAR( 100), SERVERPROPERTY ( 'Collation') )
        , @vd_BackupStart     = CAST( CONVERT( VARCHAR( 4), DATEPART( yyyy, GETDATE()) ) + '-' + CONVERT ( VARCHAR (2), DATEPART( mm, GETDATE()) ) + '-01' AS DATETIME )
        , @vd_BackupStart     = @vd_BackupStart - 1
        , @vd_BackupEnd       = CAST( CONVERT( VARCHAR(5 ), DATEPART ( yyyy , GETDATE () + 1) ) + '-' + CONVERT( VARCHAR(2 ), DATEPART ( mm , GETDATE () + 1) ) + '-' + CONVERT ( VARCHAR(2 ), DATEPART ( dd , GETDATE () + 1) ) AS DATETIME )
        , @vn_MinRuntimeSec   = 60 -- Tempo (em segundos) para desconsiderar duração de jobs

   SELECT @vd_LastRestart = CONVERT(CHAR(10), sqlserver_start_time, 103) + ' ' + CONVERT(CHAR(5), sqlserver_start_time, 108) FROM sys.dm_os_sys_info

   IF SERVERPROPERTY('IsClustered' ) = 0
   BEGIN
      SELECT @vc_IsClustered = 'Não'
   END
      ELSE
   BEGIN
      SELECT @vc_IsClustered = 'Sim'
   END       

   -- Recupera o profile de e-mail caso o mesmo não tenha sido informado 
   IF @pc_EmailProfile IS NULL
   BEGIN
      SELECT TOP 1 @pc_EmailProfile = name
      FROM msdb.dbo.sysmail_profile WITH (NOLOCK);
   END 

   -- Recupera informações de configuração do SQL Agent
   EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent', N'IdleCPUPercent', @vn_IdleCPUPercent OUTPUT, N'no_output';
   EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent', N'IdleCPUDuration', @vn_IdleCPUDuration OUTPUT, N'no_output';         

   -- Exclusão das tabelas temporárias
   IF EXISTS ( SELECT 1
               FROM TempDB ..SysObjects
               WHERE name = '##StatusJob')  
   BEGIN  
      DROP TABLE ##StatusJob;   
   END   
 
   -- Criação de tabelas temporárias
   DECLARE @vt_EstimatedUsageBD TABLE
   ( [Porcentagem (%)] DECIMAL( 10, 2 )
   , [Banco de Dados]  VARCHAR(128)
   , [Usado (%)]       VARCHAR(24)
   , [Livre (%)]       VARCHAR(24)      
   , [Total]           VARCHAR(24)
   , [DATA (Usado, %)] VARCHAR(24)
   , [LOG (Usado, %)]  VARCHAR(24)
   );
      

   IF (SELECT OBJECT_ID('tempdb..#BackupLog')) IS NOT NULL
   BEGIN
       DROP TABLE #BackupLog;
   END 

   CREATE TABLE #BackupLog 
   ( ExecutionDateTime      DATETIME
   , DBType                 VARCHAR(6)
   , BackupType             VARCHAR(20)
   , ExecutionEndDateTime   DATETIME
   , ExecutionRunTimeInSecs INT
   , Status                 VARCHAR(MAX)
   , Warnings               VARCHAR(MAX)
   );

   IF (SELECT OBJECT_ID('tempdb..#Directories')) IS NOT NULL
   BEGIN  
      DROP TABLE #Directories;   
   END  

   CREATE TABLE #Directories
   (
      Arquivo VARCHAR (2000)
   );

   -- Recupera informações dos databases
   EXEC dbaMonitor.dbo.spu_SpaceUsageDB 
        @pc_Database    = 'ALL'       
      , @pc_Level       = 'DATABASE'   
      , @pn_UpdateUsage = 0          
      , @pc_Unit        = 'GB'      
      , @pn_Summary     = 0          
      , @pn_ReturnXML   = 1          
      , @px_XMLOutput   = @vx_SpaceUsedBD OUTPUT

   -- Recupera informações dos databases
   EXEC dbaMonitor.dbo.spu_SpaceUsageDB 
        @pc_Database    = 'ALL'       
      , @pc_Level       = 'FILE'  
      , @pn_UpdateUsage = 0          
      , @pc_Unit        = 'GB'      
      , @pn_Summary     = 0          
      , @pn_ReturnXML   = 1          
      , @px_XMLOutput   = @vx_SpaceUsedFile OUTPUT

   -- Controle de Transações
   SET @vn_TranCount = @@TRANCOUNT

   -- Estrutura do CSS - Formatação HTML
   SELECT @vc_HtmlTableStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'tableHTML';
   SELECT @vc_HtmlThStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'thHTML';
   SELECT @vc_HtmlTdStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'tdHTML';
                                                                           
   -- Recupera informações de deadlocks
   IF EXISTS ( 
             SELECT 1 
             FROM sys.dm_xe_sessions   
             WHERE name = 'ReportDeadlocks'
             )
   BEGIN
   
       SELECT @vc_QtdDeadlocks = CONVERT(VARCHAR, COUNT(1)) 
       FROM (
            SELECT CONVERT(XML, fileDesc.event_data) AS event_data
            FROM ( 
                 SELECT target_data = CONVERT(XML, xeTarget.target_data)
                 FROM sys.dm_xe_session_targets xeTarget
                      INNER JOIN 
                      sys.dm_xe_sessions xeSession
                      ON xeTarget.event_session_address = xeSession.address
                 WHERE xeTarget.target_name = 'event_file'
                 AND   xeSession.name = 'ReportDeadlocks'
                 ) sessionData
                 CROSS APPLY 
                 sessionData.target_data.nodes('//EventFileTarget/File') FileEvent (FileTarget)
                 CROSS APPLY
                 sys.fn_xe_file_target_read_file(FileEvent.FileTarget.value('@name', 'varchar(1000)'), NULL, NULL, NULL) fileDesc
            ) AS xeEvents(event_data)
            CROSS APPLY
            xeEvents.event_data.nodes('(event/data[@name="xml_report"]/value)[last()]/*') AS xmlreport (deadlock)
       WHERE xeEvents.event_data.value('(event/data/value/deadlock/process-list/process/@lasttranstarted)[1]', 'datetime2(0)') >= DATEADD(DAY, -1, CURRENT_TIMESTAMP);

   END     
   ELSE
   BEGIN
     SET @vc_QtdDeadlocks = 'Sessão XE inativa';
   END 

   BEGIN TRY
  
                                       /***** Monta a estruta do E-mail em HTML *****/
                                   
      /*
      ###########################
      # Informações do servidor #
      ###########################
      */                                    
      SET @vc_HtmlScript =                                                         
         '<size="4">Informações do Servidor</font>
         <table ' + @vc_HtmlTableStyle + ' cellpadding="0" cellspacing="0" width="47%" height="50">
         <tr align="center">
            <th ' + @vc_HtmlThStyle + 'width="25%" height="22">Endereço IP</font></th>
            <th ' + @vc_HtmlThStyle + 'width="25%" height="22">Nome do Servidor</font></th>
            <th ' + @vc_HtmlThStyle + 'width="25%" height="22">Nome do Serviço</font></th>          
            <th ' + @vc_HtmlThStyle + 'width="25%" height="22">Último Restart</font></th> 
         </tr>
         <tr align="center">
            <td ' + @vc_HtmlTdStyle + 'nowrap width="25%" height="27">' + ISNULL(@vc_ServerIP, '-')  + '</font></td>
            <td ' + @vc_HtmlTdStyle + 'nowrap width="25%" height="27">' + ISNULL(@vc_Server, '') + '</font></td>
            <td ' + @vc_HtmlTdStyle + 'nowrap width="25%" height="27">' + ISNULL(@@ServiceName, '') + '</font></td>
            <td ' + @vc_HtmlTdStyle + 'nowrap width="25%" height="27">' + ISNULL(@vd_LastRestart, '') + '</font></td>
         </tr>
         </table>
         <br>

     <table ' + @vc_HtmlTableStyle + ' height="40" cellSpacing="0" cellPadding="0" width="933" >
     <tr align="center">
        <th ' + @vc_HtmlThStyle + 'width="45%" height="15">Versão</font></th>
        <th ' + @vc_HtmlThStyle + 'width="15%" height="15">Edição</font></th>
        <th ' + @vc_HtmlThStyle + 'width="8%" height="15">Service Pack</font></th>
        <th ' + @vc_HtmlThStyle + 'width="15%" height="15">Collation</font></th>
        <th ' + @vc_HtmlThStyle + 'width="18%" height="15">Clusterizado</font></td>
     </tr>
     <tr align="center">
        <td ' + @vc_HtmlTdStyle + 'width="45%" height="27">' + ISNULL(@vc_Version, '') + '</font></td>
        <td ' + @vc_HtmlTdStyle + 'width="15%" height="27">' + ISNULL(@vc_Edition, '') + '</font></td>
        <td ' + @vc_HtmlTdStyle + 'width="8%" height="27">'  + ISNULL(@vc_ServicePack, '') + '</font></td>
        <td ' + @vc_HtmlTdStyle + 'width="15%" height="27">' + ISNULL(@vc_Collation, '') + '</font></td>
        <td ' + @vc_HtmlTdStyle + 'width="18%" height="27">' + ISNULL(@vc_IsClustered, '') + '</font></td>
     </tr>
     </table><br>'

     -- Verifica se houve concatenação com NULL
     IF (@vc_HtmlScript IS NULL)
     BEGIN
          RAISERROR('O HTML gerado retornou vazio após montar as informações do servidor. Verifique! %s', 16, 1, @vc_HtmlScript);
     END

      /*
      ###########################
      # Performance #
      ###########################
      */                        
/* -- Comentário temporário                  
      SET @vc_HtmlScript +=                                                         
         '<size="4">Estatísticas de performance </font>
         <table ' + @vc_HtmlTableStyle + ' cellpadding="0" cellspacing="0" width="75%" height="50">
         <tr align="center">
            <th ' + @vc_HtmlThStyle + 'width="25%" height="22">
               % Processador SQL Server</font>
            </th>
            <th ' + @vc_HtmlThStyle + 'width="25%" height="22">
               Batch Requests</font>
            </th>          
            <th ' + @vc_HtmlThStyle + 'width="25%" height="22">
               User Connections</font>
            </th> 
            <th ' + @vc_HtmlThStyle + 'width="25%" height="22">
               Page Life Expectancy</font>
            </th> 
            <th ' + @vc_HtmlThStyle + 'width="12%" height="22">
               Deadlocks (24 horas)</font>
            </th>
            <th ' + @vc_HtmlThStyle + 'width="25%" height="22">
               Tamanho dos bancos de dados (GB)</font>
            </th> 
         </tr>
         '
     SELECT @vc_HtmlScript += 
            '<tr>' +
            '<td ' + @vc_HtmlTdStyle + 'align="center">' + 'min:' + LTRIM(STR(MIN(SQLServerProcessorTime))) + ' / ' + 'max:' +LTRIM(STR(MAX(SQLServerProcessorTime))) + ' / ' + 'avg:' + LTRIM(STR(AVG(SQLServerProcessorTime))) + '</font></td>' +  
            '<td ' + @vc_HtmlTdStyle + 'align="center">' + 'min:' + LTRIM(STR(MIN(BatchRequestsSec))) + ' / ' + 'max:' +LTRIM(STR(MAX(BatchRequestsSec))) + ' / ' + 'avg:' + LTRIM(STR(AVG(BatchRequestsSec))) + '</font></td>' +  
            '<td ' + @vc_HtmlTdStyle + 'align="center">' + 'min:' + LTRIM(STR(MIN(UserConnections))) + ' / ' + 'max:' +LTRIM(STR(MAX(UserConnections))) + ' / ' + 'avg:' + LTRIM(STR(AVG(UserConnections))) + '</font></td>' +  
            '<td ' + @vc_HtmlTdStyle + 'align="center">' + 'min:' + LTRIM(STR(MIN(PageLifeExpectancy))) + ' / ' + 'max:' +LTRIM(STR(MAX(PageLifeExpectancy))) + ' / ' + 'avg:' + LTRIM(STR(AVG(PageLifeExpectancy))) + '</font></td>' +  
            '<td ' + @vc_HtmlTdStyle + 'nowrap width="25%" height="27" >' + @vc_QtdDeadlocks  + '</font></td>' +
            '<td ' + @vc_HtmlTdStyle + 'align="center">' + LTRIM(RTRIM(STR(CONVERT(INT, MAX(DatabaseTotalSize))))) + '</font></td>' +  
            '</tr>'
     FROM dbaMonitor.baseline.SystemStatistics
     WHERE CollectionDate >= DATEADD(DAY, -1, CURRENT_TIMESTAMP);

     SELECT @vc_HtmlScript = @vc_HtmlScript + N'</table><br>'  
                      
     -- Verifica se houve concatenação com NULL
     IF (@vc_HtmlScript IS NULL)
     BEGIN
          RAISERROR('O HTML gerado retornou vazio após montar as informações do servidor. Verifique! %s', 16, 1, @vc_HtmlScript);
     END
*/
     /*
     ##########################
     # Estatísticas dos Waits #
     ##########################
     */
     IF EXISTS ( SELECT 1 
                 FROM INFORMATION_SCHEMA.TABLES 
                 WHERE TABLE_CATALOG = 'dbaMonitor'
                 AND   TABLE_SCHEMA  = 'dbo'
                 AND   TABLE_NAME    = 'WaitStatistics'
               )
     BEGIN

          SELECT DISTINCT 
                 @vc_HtmlScript = @vc_HtmlScript +
                 N'<size="4" color="#000000">Wait Statistics</font>' +
                 N'<table ' + @vc_HtmlTableStyle + ' height="40" cellSpacing="0" cellPadding="0" width="80%" >' +
                 N'<tr align="center">' + 
                 N'<th ' + @vc_HtmlThStyle + 'width="36%" height="25">Wait Type</font></th>' +
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Porcentagem</font></th>' +
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Waits/Seg</font></th>' +
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Waits (resource)/Seg</font></th>' +
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Waits (signal)/Seg</font></th>' +
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Quantidade no período</font></th>' +
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Média de Waits/Seg</font></th>' +
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Média de Waits (resource)/Seg</font></th>' +
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Média de Waits (signal)/Seg</font></th>'+
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Início da amostragem</font></th>'+
                 N'<th ' + @vc_HtmlThStyle + 'width="8%" height="25">Data da captura</font></th>'+
                 N'</tr>'; 

           SELECT @vc_HtmlScript = @vc_HtmlScript +
                  '<tr>
                   <td ' + @vc_HtmlTdStyle + 'align="center">' + WaitType + '</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR(5), ISNULL(Percentage, 0)) + ' %</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR, ISNULL(WaitsPerSec, 0)) + '</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR, ISNULL(ResourceWaitPerSec, 0)) + '</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR, ISNULL(SignalWaitPerSec, 0)) + '</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR, ISNULL(WaitCount, 0)) + '</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR, ISNULL(AvgWaitsPerSec, 0)) + '</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR, ISNULL(AvgResourceWaitPerSec, 0)) + '</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR, ISNULL(AvgSignalWaitPerSec, 0)) + '</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR(10), ComparisonDate, 103) + ' ' + CONVERT(CHAR(5), CollectionDate, 108) + '</font></td>' +  
                  '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR(10), CollectionDate, 103) + ' ' + CONVERT(CHAR(5), CollectionDate, 108) + '</font></td>' +  
                  '</tr>'
          FROM dbaMonitor.dbo.WaitStatistics
          WHERE CollectionDate >= DATEADD(DAY, -1, CURRENT_TIMESTAMP)
          ORDER BY Percentage DESC;

          SELECT @vc_HtmlScript = @vc_HtmlScript + N'</table><br>'  

     END;
   
     /*
     ###########################
     # Estatísticas dos Discos #
     ###########################
     */

      SELECT @vc_HtmlScript = @vc_HtmlScript +
             N'<size="4">Estatísticas dos Discos</font>' +
             N'<table ' + @vc_HtmlTableStyle + ' height="40" cellSpacing="0" cellPadding="0" width="58%" >' +
             N'<tr align="center">' + 
             N'<th ' + @vc_HtmlThStyle + 'width="12%" height="15">' +
             N'Drive</font>' +
             N'</th>' +
             N'<th ' + @vc_HtmlThStyle + 'width="38%" height="15">' +
             N'   Label</font>' +
             N'</th>' +
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   Capacidade (GB)</font>' +
             N'</th>' +
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   Espaço Livre (GB)</font>' +
             N'</th>' +
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   Percentagem Livre</font>' +
             N'</th>'+
             N'</tr>' 

     ;WITH cteStorageAlloc 
     AS ( 
     	SELECT DISTINCT
                 [Drive]       = s.volume_mount_point
               , [Label]       = s.logical_volume_name
               , [Capacity]    = CONVERT(VARCHAR(10), CONVERT(NUMERIC(13,2), s.total_bytes / 1073741824.0))
     		, [FreeSpace]   = CONVERT(VARCHAR(10), CONVERT(NUMERIC(13,2), s.available_bytes / 1073741824.0))
               , [PercentFree] = CONVERT(NUMERIC(13,1), ((s.available_bytes * 1.0 / s.total_bytes) * 100.0))
     	FROM sys.master_files f
     		CROSS APPLY 
               sys.dm_os_volume_stats(f.database_id, f.[file_id]) s
        )

      SELECT @vc_HtmlScript = @vc_HtmlScript +
             '<tr>
              <td ' + @vc_HtmlTdStyle + 'align="center">' + REPLICATE(' ', 3) + ISNULL(Drive, '') + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL(Label, '' ) + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL(Capacity, 0) + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL(FreeSpace, 0) + '</font></td>' +  
             CASE WHEN PercentFree <= 10.0
                  THEN '<td ' + @vc_HtmlTdStyle + 'align="center" height="21" bgColor="#ff0000">' + CONVERT(CHAR(5), PercentFree) + '</font></td>'
                  ELSE '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(CHAR(5), PercentFree) + '</font></td>' 
             END + 
             '</tr>'
      FROM cteStorageAlloc
      ORDER BY Drive
    
      SELECT @vc_HtmlScript = @vc_HtmlScript + N'</table><br>'  

      -- Verifica se houve concatenação com NULL
      IF (ISNULL(@vc_HtmlScript, '') = '')
      BEGIN
           RAISERROR('O HTML gerado retornou vazio após montar as estatísticas dos disco. Verifique!', 16, 1);
      END

      /*
      ##########################################################
      # Arquivos com espaço livre abaixo do limite configurado #
      ##########################################################
      */

      IF EXISTS ( SELECT 1
                  FROM (
                         SELECT al.value('Free_Percent[1]', 'DECIMAL(10,2)')  AS Livre_Percent
                         FROM @vx_SpaceUsedFile.nodes('/file/diskspace') AS root(al)
                       ) Aloc
                  WHERE Livre_Percent < @vc_AlertThreshold
               )
      BEGIN 

           SELECT @vc_HtmlScript = @vc_HtmlScript +
           '<size="4">Arquivos com espaço livre abaixo do limite configurado</font>' +
           '<table ' + @vc_HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1150" >
              <tr align="center">
           <th ' + @vc_HtmlThStyle + 'width="15%" height="15">
           Nome</font></th>
           <th ' + @vc_HtmlThStyle + 'width="9%" height="15">
           Tipo</font></th>
           <th ' + @vc_HtmlThStyle + 'width="9%" height="15">
           Tamanho (GB)</font></th>
           <th ' + @vc_HtmlThStyle + 'width="9%" height="15">
           Usado (GB)</font></th>
           <th ' + @vc_HtmlThStyle + 'width="9%" height="15">
           Usado (%)</font></th>
           <th ' + @vc_HtmlThStyle + 'width="9%" height="15">
           Livre (GB)</font></th>
           <th ' + @vc_HtmlThStyle + 'width="40%" height="15">
           Nome do Arquivo</font></th>       
              </tr>'

           SELECT @vc_HtmlScript = @vc_HtmlScript +
           '<tr>
            <td ' + @vc_HtmlTdStyle + 'align="left">' + ISNULL( LEFT(DBName, 35), '' ) +'</font></td>' +  
           '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( Tipo, '' ) +'</font></td>' +  
           '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( Total, '' ) +'</font></td>' +  
           '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( Usado, '' ) +'</font></td>' +  
           '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( Usado_Percent, '' ) +'</font></td>' +  
           '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( Livre, '' ) +'</font></td>' +  
           '<td ' + @vc_HtmlTdStyle + 'align="left" height="21">' + ISNULL( NomeFisico, '' ) +'</font></td>'  
           FROM (
                  SELECT al.value('Database[1]', 'varchar(128)')     AS DBName
                       , al.value('Type[1]', 'varchar(24)')          AS Tipo
                       , al.value('LogicalName[1]', 'varchar(24)')   AS NomeLogico
                       , al.value('Total[1]', 'varchar(24)')         AS Total
                       , al.value('Used[1]', 'varchar(24)')          AS Usado
                       , al.value('Used_Percent[1]', 'varchar(24)')  AS Usado_Percent
                       , al.value('Free[1]', 'varchar(24)')          AS Livre
                       , al.value('Free_Percent[1]', 'varchar(24)')  AS Livre_Percent
                       , al.value('physicalname[1]', 'varchar(128)') AS NomeFisico
                  FROM @vx_SpaceUsedFile.nodes('/file/diskspace') AS root(al)
                ) Aloc
           WHERE CONVERT(DECIMAL(10,2), Livre_Percent)  < @vc_AlertThreshold;

           SELECT @vc_HtmlScript = @vc_HtmlScript + '</table><br>'

           -- Verifica se houve concatenação com NULL
           IF (@vc_HtmlScript IS NULL)
           BEGIN
                RAISERROR('O HTML gerado retornou vazio após montar as informações de arquivos com espaço livre abaixo do configurado. Verifique!', 16, 1);
           END

     END;
    
      /*
      ###################
      # Bancos de Dados #
      ###################
      */
      /*
      SELECT @vc_HtmlScript = @vc_HtmlScript +
      '<size="4">Bancos de Dados</font>' +
      '<table ' + @vc_HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1150" >
         <tr align="center">
      <th ' + @vc_HtmlThStyle + 'width="13%" height="15">
      </font>Nome</th>
      <th ' + @vc_HtmlThStyle + 'width="10%" height="15">
      Tamanho (GB)</font></th>
      <th ' + @vc_HtmlThStyle + 'width="10%" height="15">
      DATA (Usado, %)</font></th>
      <th ' + @vc_HtmlThStyle + 'width="11%" height="15">
      LOG (Usado, %)</font></th>     
      <th ' + @vc_HtmlThStyle + 'width="10%" height="15">
      Crescimento Mensal</font></th>       
      <th ' + @vc_HtmlThStyle + 'width="10%" height="15">
      Crescimento Semanal</font></th>       
         </tr>';

      --# Recupera dados de crescimento de banco de dados
      WITH cteDBGrowth
      AS ( 
           SELECT DbName
                , DataGrowthMonthly = LTRIM(STR([DataGrowthMonthly])) + ' MB'
                , LogGrowthMonthly = LTRIM(STR([LogGrowthMonthly])) + ' MB'
                , DataAvgGrowthWeekly = LTRIM(STR([DataAvgGrowthWeekly])) + ' MB' 
                , LogAvgGrowthWeekly = LTRIM(STR([LogAvgGrowthWeekly])) + ' MB'
           FROM ( 
                 SELECT [DbName] = db.dbsName
                      , [FileType] = CASE WHEN dbf.dbfType = 'ROWS'
                                          THEN 'DataGrowthMonthly'
                                          ELSE 'LogGrowthMonthly'
                                     END
                      , [DeltaGrowthMonthly] = SUM(dbf.dbfDeltaGrowthMonthly) 
                 FROM dbaMonitor.dbo.Databases db
                      INNER JOIN dbaMonitor.dbo.DatabaseFiles dbf
                            ON  db.dbsId = dbf.dbsId
                 WHERE db.dbsId > 4 
                 GROUP BY db.dbsName
                        , dbf.dbfType
 
                 UNION ALL
 
                 SELECT [DbName] = db.dbsName
                      , [FileType] = CASE WHEN dbf.dbfType = 'ROWS'
                                           THEN 'DataAvgGrowthWeekly'
                                           ELSE 'LogAvgGrowthWeekly'
                                     END
                      , [AvgGrowthWeekly] = SUM(CAST(dbf.dbfAvgGrowthWeekly AS DECIMAL(9,2))) 
                 FROM dbaMonitor.dbo.Databases db
                      INNER JOIN dbaMonitor.dbo.DatabaseFiles dbf
                            ON  db.dbsId = dbf.dbsId
                 WHERE db.dbsId > 4 
                 GROUP BY db.dbsName
                        , dbf.dbfType
                ) AS Grow
           PIVOT ( SUM(DeltaGrowthMonthly)
                   FOR [FileType]
                   IN ( [DataGrowthMonthly]
                      , [LogGrowthMonthly]
                      , [DataAvgGrowthWeekly]
                      , [LogAvgGrowthWeekly]
                      ) 
 
               ) PvtGrow
           WHERE DataGrowthMonthly   != 0
           AND   LogGrowthMonthly    != 0
           AND   DataAvgGrowthWeekly != 0
           AND   LogAvgGrowthWeekly  != 0
         ) 

      SELECT @vc_HtmlScript = @vc_HtmlScript +
      '<tr>
       <td ' + @vc_HtmlTdStyle + 'align="left">' + ISNULL( LEFT(db.name, 35), '' ) +'</font></td>' +  
      '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( Total, '' ) +'</font></td>' +  
      '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( DATA_Usado_Percent, '' ) +'</font></td>' +  
      '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( LOG_Usado_Percent, '' ) +'</font></td>' +  
      '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( Grow.DataGrowthMonthly, '' ) +'</font></td>' +
      '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL( Grow.DataAvgGrowthWeekly, '' ) +'</font></td></tr>'

      FROM (
             SELECT al.value('Banco_de_Dados[1]', 'varchar(128)')     AS DBName
                  , al.value('Porcentagem[1]', 'varchar(24)')         AS Porcentagem
                  , al.value('Usado_Percent[1]', 'varchar(24)')       AS Usado_Percent
                  , al.value('Livre_Percent[1]', 'varchar(24)')       AS Livre_Percent
                  , al.value('Total[1]', 'varchar(24)')               AS Total
                  , al.value('DATA_Usado_Percent[1]', 'varchar(24)')  AS DATA_Usado_Percent
                  , al.value('LOG_Usado_Percent[1]', 'varchar(24)')   AS LOG_Usado_Percent
             FROM @vx_SpaceUsedBD.nodes('/db/diskspace') AS root(al)
           ) Aloc
           INNER JOIN master.sys.databases db
                  ON  Aloc.DbName = db.name
           INNER JOIN cteDBGrowth Grow
                  ON  Grow.DbName = db.name
      ORDER BY DATA_Usado_Percent DESC;

      SELECT @vc_HtmlScript = @vc_HtmlScript + '</table><br>' 
*/                

  
     /*
     #################################
     # Crescimento de banco de dados #
     #################################
     */

      SELECT @vc_HtmlScript = @vc_HtmlScript +
             N'<size="4">Crescimento de banco de dados</font>' +
             N'<table ' + @vc_HtmlTableStyle + ' height="40" cellSpacing="0" cellPadding="0" width="85%" >' +
             N'<tr align="center">' + 
             N'<th ' + @vc_HtmlThStyle + 'width="12%" height="15">' +
             N'Banco de dados</font>' +
             N'</th>' +
             N'<th ' + @vc_HtmlThStyle + 'width="38%" height="15">' +
             N'   Arquivo lógico</font>' +
             N'</th>' +
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   3 meses atrás</font>' +
             N'</th>' +
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   2 meses atrás</font>' +
             N'</th>' +
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   Último mês</font>' +
             N'</th>'+
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   15 dias atrás </font>' +
             N'</th>'+
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   Ontem</font>' +
             N'</th>'+
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   Cresc. no último mês</font>' +
             N'</th>'+
             N'<th ' + @vc_HtmlThStyle + 'width="16%" height="15">' +
             N'   Média mensal</font>' +
             N'</th>'+
             N'</tr>' 

      SELECT @vc_HtmlScript = @vc_HtmlScript +
             '<tr>
              <td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + DatabaseName + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + LogicalName + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL(CONVERT(VARCHAR, Last3Months), 'Sem info.') + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL(CONVERT(VARCHAR, Last2Months), 'Sem info.') + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL(CONVERT(VARCHAR, LastMonth)  , 'Sem info.') + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL(CONVERT(VARCHAR, Last15Days) , 'Sem info.') + '</font></td>' +  
             --'<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + LastWeek + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + CONVERT(VARCHAR, Yesterday) + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + LastMonthGrowth + '</font></td>' +  
             '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + AvgGrowthMonthly + '</font></td>' +  
             '</tr>'
      FROM dbo.vw_GrowthTrackDB
      ORDER BY AvgGrowthOrderBy ASC;

      SELECT @vc_HtmlScript = @vc_HtmlScript + N'</table><br>'  

      -- Verifica se houve concatenação com NULL
      IF (@vc_HtmlScript IS NULL)
      BEGIN
           RAISERROR('O HTML gerado retornou vazio após montar as estatísticas dos disco. Verifique!', 16, 1);
      END

      /*
      ########################
      #       SQL Jobs       #      
      ########################
      */

      SELECT @vc_HtmlScript = @vc_HtmlScript +
         '<size="4">SQL Agent - Execução de Jobs</font> (falha, cancelados ou execução superior à 5 minutos)</font>
          <table ' + @vc_HtmlTableStyle + ' cellPadding="0" height="40" width="933"  >
          <tr align="center">
             <th ' + @vc_HtmlThStyle + 'width="15%">
             Nome</font></th>
             <th ' + @vc_HtmlThStyle + 'width="5%">
             Habilitado</font></th>
             <th ' + @vc_HtmlThStyle + 'width="5%">
             Última execução</font></th>
             <th ' + @vc_HtmlThStyle + 'width="5%">
             Data da última execução</font></th>
             <th ' + @vc_HtmlThStyle + 'width="5%">
             Duração</font></th>
          </tr>'

      SELECT @vc_HtmlScript = @vc_HtmlScript +
                '<tr>
                <td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL(CONVERT (VARCHAR( 100), SQLJobs.name ), '' ) + '</font></td>' +  
                CASE SQLJobs.enabled
                     WHEN 0
                     THEN '<td ' + @vc_HtmlTdStyle + 'align="center" height="21" bgcolor="#FFCC99">Não</font></td>'
                     WHEN 1
                     THEN '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">Sim</font></td>'
                     ELSE '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">Desconhecido</font></td>'
                END  + 
                CASE SQLJobs.last_run_outcome   
                     WHEN 0
                     THEN '<td ' + @vc_HtmlTdStyle + 'align="center" height="21" bgColor="#ff0000">Falha</font></td>'
                     WHEN 1
                     THEN '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">Sucesso</font></td>' 
                     WHEN 3
                     THEN '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">Cancelado</font></td>'
                     WHEN 5
                     THEN '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">Desconhecido</font></td>'
                     ELSE '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">Outros</font></td>' 
                END  + 
                '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + ISNULL(CONVERT(VARCHAR(10), SQLJobs.last_run_date, 103) + ' ' + CONVERT(VARCHAR(8), SQLJobs.last_run_time), '') + '</font></td>' +
                '<td ' + @vc_HtmlTdStyle + 'align="center" height="21">' + IIF(duration_in_mins >= 0, LTRIM(STR((duration_in_mins / 60))) + 'h ' +  LTRIM(STR(duration_in_mins % 60)) + 'mins', '-') + '</font></td>' +
            '</tr>'                 
      FROM (
           SELECT sjob.job_id
                , sjob.name
                , CASE WHEN last_run_date != 0
                       THEN CONVERT(DATE, CAST(last_run_date AS CHAR(10)) )
                       ELSE '1900-01-01'
                  END AS last_run_date
                , LEFT(REPLICATE('0', 6 - LEN(last_run_time)) + CAST(last_run_time AS CHAR(6)), 2 ) + ':'
                + SUBSTRING(REPLICATE('0', 6 - LEN(last_run_time)) + CAST(last_run_time AS CHAR(6)), 3, 2 ) + ':'
                + SUBSTRING(REPLICATE('0', 6 - LEN(last_run_time)) + CAST(last_run_time AS CHAR(6)), 5, 2 ) AS last_run_time
                , scat.name AS category
                , sjob.enabled
                , jsrv.last_run_outcome 
                , ISNULL(sja.DurationInMins, -1) AS duration_in_mins
           FROM msdb.dbo.sysjobs sjob
                INNER JOIN 
                msdb.dbo.sysjobServers AS jsrv ON sjob.job_id = jsrv.job_id
                INNER JOIN 
                msdb.dbo.syscategories AS scat ON sjob.category_id = scat.category_id
                INNER JOIN 
                (
                SELECT DISTINCT 
                        job_id
                      , DATEDIFF(minute, MAX(last_executed_step_date) OVER (PARTITION BY job_id), MAX(stop_execution_date) OVER (PARTITION BY job_id)) DurationInMins
                FROM msdb.dbo.sysjobactivity  
                ) AS sja ON sjob.job_id = sja.job_id
           ) AS SQLJobs
      WHERE duration_in_mins > 5
      OR    SQLJobs.last_run_outcome != 1
      ORDER BY SQLJobs.enabled DESC
             , SQLJobs.last_run_outcome   
             , ISNULL( CONVERT( VARCHAR(10), SQLJobs.last_run_date, 103 ) + ' ' + CONVERT( VARCHAR(8), SQLJobs.last_run_time ), '' )
                                
      SELECT @vc_HtmlScript = @vc_HtmlScript + '</table><br><br>'

      -- Verifica se houve concatenação com NULL
      IF (@vc_HtmlScript IS NULL)
      BEGIN
           RAISERROR('O HTML gerado retornou vazio após montar dados do SQL Agent - Jobs. Verifique!', 16, 1);
      END;


      /*
      #####################################
      # Agendamento de JOBs do SQL Agent  #
      #####################################      


      IF (SELECT ISNULL(OBJECT_ID('tempdb..#JobSchedules'), 0)) != 0
      BEGIN 
          DROP TABLE tempdb..#JobSchedule;
      END;

      WITH cteSchedules
      AS (
         SELECT j.job_id 
              , [schedule_name] = ss.name 
              , CASE ss.freq_type
                     WHEN 0x01 THEN N'Once on ' + FORMAT(msdb.dbo.agent_datetime(ss.active_start_date, ss.active_start_time),'MMM dd yyyy hh:mm:ss.')
                     WHEN 0x04 THEN CASE ss.freq_interval WHEN 1 THEN N'Todo dia'
                                                                 ELSE N'A cada ' + CONVERT(nvarchar, ss.freq_interval) + N' dias' END
                                  + IIF(ss.freq_subday_type IN (0x02,0x04,0x08),N', ',N' ')
                     WHEN 0x08 THEN CASE ss.freq_recurrence_factor WHEN 1 THEN N'Durante a semana no(a) '
                                                                          ELSE N'Todas as ' + CONVERT(nvarchar, ss.freq_recurrence_factor) + N' semanas em ' END
                                  + STUFF( IIF(ss.freq_interval & 0x01 = 0x01, N', domingo'    ,'') 
                                         + IIF(ss.freq_interval & 0x02 = 0x02, N', segunda-feira'    ,'')
                                         + IIF(ss.freq_interval & 0x04 = 0x04, N', terça-feira'   ,'')
                                         + IIF(ss.freq_interval & 0x08 = 0x08, N', quarta-feira' ,'')
                                         + IIF(ss.freq_interval & 0x10 = 0x10, N', quinta-feira'  ,'')
                                         + IIF(ss.freq_interval & 0x20 = 0x20, N', sexta-feira'    ,'') 
                                         + IIF(ss.freq_interval & 0x40 = 0x40, N', sábado'  ,''), 1, 1, '') + ' '
                     WHEN 0x10 THEN IIF(ss.freq_recurrence_factor = 1, N'Todo mês no dia  ', N'Todos os ' + CONVERT(nvarchar,ss.freq_recurrence_factor) + N' meses no dia ')
                                  + CONVERT(nvarchar,ss.freq_interval) + N' deste mês '
                     WHEN 0x20 THEN IIF(ss.freq_recurrence_factor = 1, N'A cada mês em  ', N'A cada ' + CONVERT(nvarchar,ss.freq_recurrence_factor) + N' mês no(a) ')
                                  + CASE ss.freq_relative_interval WHEN 0x01 THEN N'primeiro(a) '
                                                                   WHEN 0x02 THEN N'segundo(a) '
                                                                   WHEN 0x04 THEN N'terceiro(a) '
                                                                   WHEN 0x08 THEN N'quarto(a) '
                                                                   WHEN 0x10 THEN N'último(a) ' END
                                  + CASE WHEN ss.freq_interval BETWEEN 1 AND 7 THEN DATENAME(dw, N'1996120' + CONVERT(nvarchar, ss.freq_interval))
                                         WHEN ss.freq_interval =  8 THEN N'dia'
                                         WHEN ss.freq_interval =  9 THEN N'dia da semana'
                                         WHEN ss.freq_interval = 10 THEN N'final de semana' END
                                  + N' do mês'
                                  + IIF(ss.freq_subday_type IN (0x02,0x04,0x08),N', ',N' ')
                     WHEN 0x40 THEN FORMATMESSAGE(14579)
                     WHEN 0x80 THEN FORMATMESSAGE(14578, ISNULL(@vn_IdleCPUPercent,10), ISNULL(@vn_IdleCPUDuration,600))
                END
                /* Parte do intervalos */
              + IIF( ss.freq_type IN (0x04, 0x08, 0x10, 0x20)
                   , CASE ss.freq_subday_type WHEN 0x1 THEN N'às ' + CONVERT(nvarchar, RIGHT('00'+CONVERT(varchar(10),ss.active_start_time/10000),2) + ':' + RIGHT('00' + CONVERT(varchar(10),(ss.active_start_time % 10000) / 100),2) ) 
                                              WHEN 0x2 THEN IIF(ss.freq_subday_interval = 1,N'cada segundo,',N'cada ' + CONVERT(nvarchar, ss.freq_subday_interval) + N' segundos,') 
                                              WHEN 0x4 THEN IIF(ss.freq_subday_interval = 1,N'cada minuto,',N'cada ' + CONVERT(nvarchar, ss.freq_subday_interval) + N' minutos,')
                                              WHEN 0x8 THEN IIF(ss.freq_subday_interval = 1,N'cada hora,',N'cada ' + CONVERT(nvarchar, ss.freq_subday_interval) + N' horas,') END
                   + IIF( ss.freq_subday_type IN (0x02, 0x04, 0x08)
                        , N' entre '
                        + CONVERT(nvarchar, RIGHT('00'+CONVERT(varchar(10),ss.active_start_time / 10000),2) + ':' + RIGHT('00'+CONVERT(varchar(10),(ss.active_start_time % 10000) / 100),2) )
                        + N' e '
                        + CONVERT(nvarchar, RIGHT('00'+CONVERT(varchar(10),ss.active_end_time / 10000),2) + ':' + RIGHT('00'+CONVERT(varchar(10),(ss.active_end_time % 10000) / 100),2) ) 
                        , N'')
                   , N'') As [Description]
           FROM msdb.dbo.sysschedules ss
                INNER JOIN 
                msdb.dbo.sysjobschedules js
                    ON js.schedule_id = ss.schedule_id
                INNER JOIN 
                msdb.dbo.sysjobs j
                    ON j.job_id = js.job_id
         ),
         cteHistory
         AS (
            SELECT dt_h.job_id
                 , [LastRunDate] = CONVERT(varchar,dt_h.RunDate,100) 
                 , [User] = IIF(PATINDEX('% invocado pelo usuário %', dt_h.[message]) = 0, '', SUBSTRING(dt_h.[message]
                               , PATINDEX('% invocado pelo usuário %', dt_h.[message]) + 17
                               , PATINDEX('%.  O último passo a executar %',dt_h.[message])-PATINDEX('% invocado pelo usuário %',dt_h.[message]) - 17)) 
            FROM (
                 SELECT [Row] = ROW_NUMBER() OVER( PARTITION BY h.job_id ORDER BY h.run_date DESC, h.run_time DESC )
                      , h.job_id
                      , [RunDate] = msdb.dbo.agent_datetime(h.run_date , h.run_time)
                      , h.[message]
                  FROM msdb.dbo.sysjobhistory h
                  WHERE step_id = 0
                  ) dt_h
            WHERE [Row] = 1
            )

          SELECT [JobName] = j.name 
               , [Owner] = SUSER_SNAME(j.owner_sid) 
               , [Enabled] = CASE WHEN j.[enabled] = 1
                                  THEN 'Sim'
                                  ELSE 'Não'
                              END 
               , [Schedule] = CASE WHEN s.Description IS NULL 
                                   THEN 'Não agendado.' + ISNULL(' Última execução '+h.LastRunDate+ ISNULL(' pelo usuário '+h.[User]+'.','.'),'')
                                   ELSE s.Description
                              END
               , [Description] = CASE WHEN j.[description] = 'No description available.'
                                      THEN 'Sem descrição disponível.' 
                                       ELSE REPLACE(REPLACE(j.[description],CHAR(13),''), CHAR(10),'') 
                                 END
          INTO #JobSchedule
          FROM msdb.dbo.sysjobs j
                 LEFT OUTER JOIN cteSchedules s
                   ON s.job_id = j.job_id
                 LEFT OUTER JOIN cteHistory h
                   ON h.job_id = j.job_id

      SET @vc_HtmlScript += 
         '<size="4">SQL Agent - Configuração de agendamento de Jobs</font></font>
          <table ' + @vc_HtmlTableStyle + ' cellPadding="0" height="40" width="1200"  >
          <tr align="center">
             <th ' + @vc_HtmlThStyle + 'width="25%">
             Nome do Job</font></th>
             <th ' + @vc_HtmlThStyle + 'width="15%">
             Proprietário</font></th>
             <th ' + @vc_HtmlThStyle + 'width="7%">
             Habilitado</font></th>
             <th ' + @vc_HtmlThStyle + 'width="23%">
             Agendamento</font></th>
             <th ' + @vc_HtmlThStyle + 'width="35%">
             Descrição</font></th>
        </tr>';
        
     SELECT @vc_HtmlScript +=                 
            '<tr>' +
                '<td ' + @vc_HtmlTdStyle + 'align="center" height="40">' + [JobName] + '</font></td>' +           
                '<td ' + @vc_HtmlTdStyle + 'align="center" height="40">' + [Owner] + '</font></td>' +           
                '<td ' + @vc_HtmlTdStyle + 'align="center" height="40">' + [Enabled] + '</font></td>' +           
                '<td ' + @vc_HtmlTdStyle + 'align="center" height="40">' + [Schedule] + '</font></td>' +           
                '<td ' + @vc_HtmlTdStyle + 'align="center" height="40">' + [Description] + '</font></td>' +           
            '</tr>'   
     FROM (
          SELECT [JobName] 
               , [Owner] 
               , [Enabled] 
               , [Schedule] 
               , [Description] = ISNULL([Description], '')
          FROM #JobSchedule
          ) _
     ORDER BY [JobName]


      SELECT @vc_HtmlScript += N'</table><br><br>';

      -- Verifica se houve concatenação com NULL
      IF (@vc_HtmlScript IS NULL)
      BEGIN
           RAISERROR('O HTML gerado retornou vazio após montar as informações de estatísticas de backups. Verifique!', 16, 1);
      END
      */

      /*
      #########################################
      # Backup com falha nas últimas 48 horas #
      #########################################      
      
      INSERT INTO #BackupLog
                ( ExecutionDateTime
                , DBType
                , BackupType
                , ExecutionEndDateTime
                , ExecutionRunTimeInSecs
                , Status
                , Warnings
                )
      SELECT BackupLog.ExecutionDateTime
           , BackupLog.DBType
           , BackupLog.BackupType
           , ExecutionEndDateTime
           , ExecutionRunTimeInSecs
           , Details.Status
           , Details.Warnings
      FROM Minion.BackupLog
           CROSS APPLY
           (
           SELECT 1 AS output
                , Status
                , Warnings
           FROM Minion.BackupLogDetails
           WHERE ExecutionDateTime = Minion.BackupLog.ExecutionDateTime
           AND   STATUS != 'All Complete'
           ) AS Details
      WHERE BackupLog.STATUS != 'All Complete'
      AND   BackupLog.ExecutionDateTime BETWEEN DATEADD(DAY, -2, SYSDATETIME())
                                            AND DATEADD(MINUTE, -60, SYSDATETIME()) -- Desconsidera os últimos 60 minutos
      OPTION (MAXDOP 1);

      -- Somente processa se houveram erros
      IF @@ROWCOUNT > 0
      BEGIN

         SELECT @vc_HtmlScript = @vc_HtmlScript +
            '<size="4">Execução de backup</font> (com erros)</font>
             <table ' + @vc_HtmlTableStyle + ' cellPadding="0" height="40" width="933"  >
             <tr align="center">
             <th ' + @vc_HtmlThStyle + 'width="15%">
             Data da execução</font></th>
             <th ' + @vc_HtmlThStyle + 'width="5%">
             Tipo de Backup</font></th>
             <th ' + @vc_HtmlThStyle + 'width="5%">
             Tipo de Banco</font></th>
             <th ' + @vc_HtmlThStyle + 'width="5%">
             Duração (segundos)</font></th>
             <th ' + @vc_HtmlThStyle + 'width="5%">
             Status</font></th>
             <th ' + @vc_HtmlThStyle + 'width="5%">
             Erro</font></th>
             </tr>'

         SELECT @vc_HtmlScript = @vc_HtmlScript +
            '<tr align="center">
                <td ' + @vc_HtmlTdStyle + '>' + ISNULL(FORMAT(ExecutionDateTime, 'D', 'pt-br'), '') + '</font></td>' +    
               '<td ' + @vc_HtmlTdStyle + '>' + DBType + '</font></td>' +    
               '<td ' + @vc_HtmlTdStyle + '>' + BackupType + '</font></td>' + 
--               '<td ' + @vc_HtmlTdStyle + '>' + ISNULL(FORMAT(ExecutionEndDateTime, 'D', 'pt-br'), '') + '</font></td>' +
               '<td ' + @vc_HtmlTdStyle + '>' + ISNULL(STR(ExecutionRunTimeInSecs), '') + '</font></td>' +
               '<td ' + @vc_HtmlTdStyle + '>' + ISNULL(Status, '') + '</font></td>' + 
               '<td ' + @vc_HtmlTdStyle + '>' + ISNULL(Warnings, '') + '</font></td>' + 
             '</tr>' 
         FROM #BackupLog

         SELECT @vc_HtmlScript = @vc_HtmlScript + '</table><br><br>'

      END

      -- Verifica se houve concatenação com NULL
      IF (@vc_HtmlScript IS NULL)
      BEGIN
           RAISERROR('O HTML gerado retornou vazio após montar as informações de backup com falha. Verifique!', 16, 1);
      END
      */

      /*    
      ##############
      # Backup Set #
      ##############
      */

      IF @pc_CheckBackupSet = 1
      BEGIN
        
         SELECT @vc_HtmlScript = @vc_HtmlScript +
            '<size="4">Backup - SQL Server</font>' +
            '<table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="933" bgColor="#ffffff" borderColorLight="#000000" >  
            <tr align="center">  
               <th ' + @vc_HtmlThStyle + 'align="left" width="91">  
               Data</font></th>  
               <th ' + @vc_HtmlThStyle + 'align="left" width="105">  
               Banco de Dados</font></th>  
               <th ' + @vc_HtmlThStyle + 'align="left" width="165">  
                Nome do Arquivo</font></th>  
               <th ' + @vc_HtmlThStyle + 'align="left" width="75">  
                Tipo</font></th>  
               <th ' + @vc_HtmlThStyle + 'align="left" width="165">
               Início</font></th>  
               <th ' + @vc_HtmlThStyle + 'align="left" width="165">  
               Fim</font></th>  
               <th ' + @vc_HtmlThStyle + 'align="left" width="136">  
               Tamanho (GB)</font></th>  
            </tr>'

         SELECT @vc_HtmlScript = @vc_HtmlScript +
            '<tr align="center">
               <td ' + @vc_HtmlTdStyle + '>'   + ISNULL( CONVERT( VARCHAR(2 ), DATEPART( dd, Bks.Backup_Start_Date ) ) + '-' + CONVERT ( VARCHAR (3), DATENAME( mm, Bks.backup_start_date ) ) + '-' + CONVERT ( VARCHAR (4), DATEPART( yyyy, Bks.backup_start_date ) ),'' ) + '</font></td>' +    
               '<td ' + @vc_HtmlTdStyle + '>' + ISNULL( CONVERT( VARCHAR(100 ), Bks .Database_Name ), '' ) + '</font></td>' +    
               '<td ' + @vc_HtmlTdStyle + '>' + ISNULL( CONVERT( VARCHAR(100 ), Bks .Name ), '' ) + '</font></td>' + 
               CASE Type
               WHEN 'D' THEN '<td ' + @vc_HtmlTdStyle + '>' + 'Full' + '</font></td>'  
               WHEN 'I' THEN '<td ' + @vc_HtmlTdStyle + '>' + 'Diferencial' + '</font></td>'
               WHEN 'L' THEN '<td ' + @vc_HtmlTdStyle + '>' + 'Log' + '</font></td>'
               WHEN 'F' THEN '<td ' + @vc_HtmlTdStyle + '>' + 'Arquivo ou Filegroup' + '</font></td>'
               WHEN 'G' THEN '<td ' + @vc_HtmlTdStyle + '>' + 'Arquivo Diferencial' + '</font></td>'
               WHEN 'P' THEN '<td ' + @vc_HtmlTdStyle + '>' + 'Parcial' + '</font></td>'
               WHEN 'Q' THEN '<td ' + @vc_HtmlTdStyle + '>' + 'Parcial Diferencial' + '</font></td>'
               ELSE '<td ' + @vc_HtmlTdStyle + '>' + 'Desconhecido' +'</font></td>'
               END +
               '<td ' + @vc_HtmlTdStyle + '>' + ISNULL( CONVERT( VARCHAR(50 ), Bks .Backup_Start_Date), '' ) + '</font></td>' +
               '<td ' + @vc_HtmlTdStyle + '>' + ISNULL( CONVERT( VARCHAR(50 ), Bks .Backup_Finish_Date), '' ) + '</font></td>' +
               '<td ' + @vc_HtmlTdStyle + '>' + ISNULL( CONVERT( VARCHAR(10 ), CAST ( ( Bks.backup_size /1024)/ 1024/1024 AS DECIMAL (10, 2)) ), '' ) + '</font></td>' + 
             '</tr>'   
         FROM msdb.dbo.BackupSet Bks
         WHERE Bks.Backup_Start_Date BETWEEN @vd_BackupStart
                                         AND @vd_BackupEnd
         ORDER BY
            Bks.Backup_Start_Date DESC

         SELECT @vc_HtmlScript = @vc_HtmlScript + '</table><br>'            
       
     END

     /*    
     ########################
     # Anexo : Job Timeline #
     ########################
     */
     -- Limpeza de objetos
     IF OBJECT_ID('tempdb..#JobRuntime') IS NOT NULL 
          DROP TABLE #JobRuntime;

     IF OBJECT_ID('tempdb..##FinalGraph') IS NOT NULL 
          DROP TABLE ##FinalGraph;

     IF OBJECT_ID('tempdb..##GraphDiv') IS NOT NULL
          DROP TABLE ##GraphDiv;

     IF OBJECT_ID('tempdb..##GraphDraw') IS NOT NULL
          DROP TABLE ##GraphDraw;

     IF OBJECT_ID('tempdb..#GraphDates') IS NOT NULL
          DROP TABLE #GraphDates

     -- Criação da tabelas que irãos conter os dados dos gráficos
     CREATE TABLE ##FinalGraph 
     ( 
       [IdOrder]  SMALLINT IDENTITY(1, 1) NOT NULL
     , [html] VARCHAR(8000) NULL
     );

     CREATE TABLE ##GraphDiv 
     ( 
       [IdOrder]  SMALLINT IDENTITY(1, 1) NOT NULL
     , [html] VARCHAR(8000) NULL
     );

     CREATE TABLE ##GraphDraw 
     ( 
       [IdOrder]  SMALLINT IDENTITY(1, 1) NOT NULL
     , [html] VARCHAR(8000) NULL
     );

     CREATE TABLE #GraphDates 
     ( 
       [StartDate] DATETIME NOT NULL
     , [EndDate]   DATETIME NULL
     );

     -- Define o range de datas
     ;WITH GraphDates 
     AS (	
          SELECT [sDate] = CONVERT(DATETIME,DATEADD(day, -2, CONVERT(DATE,SYSDATETIME()))) -- Alterar "-2" para a qtd. de dias requeridos
		UNION ALL
		SELECT DATEADD(day, 1, [sDate]) 
		FROM	GraphDates 
		WHERE [sDate] < CONVERT(DATETIME, SYSDATETIME()) - 1  
        )

     INSERT INTO #GraphDates ( StartDate, EndDate )
     SELECT [sDate]
		, DATEADD(day, 1, sDate) 
     FROM	GraphDates 
     OPTION (MAXRECURSION 1000);

     SELECT @vd_JobStart = MIN(StartDate)
          , @vd_JobEnd = MAX(EndDate)
     FROM #GraphDates;

     -- Inclusão de dados de jobs
     SELECT JobName   = job.name
		, CatName   = cat.name 
		, StartDate = CONVERT(DATETIME, CONVERT(CHAR(8), run_date, 112) + ' ' + STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), run_time), 6), 5, 0, ':'), 3, 0, ':'), 120)  
		, EndDate   = DATEADD(second, ((run_duration/10000) % 100 * 3600) + ((run_duration/100)%100 * 60) + run_duration % 100
                                      , CONVERT(DATETIME, CONVERT(CHAR(8), run_date, 112) + ' ' + STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), run_time), 6), 5, 0, ':'), 3, 0, ':'), 120))
     INTO	#JobRuntime
     FROM	msdb.dbo.sysjobs job 
	     LEFT JOIN 
          msdb.dbo.sysjobhistory his
		     ON his.job_id = job.job_id
          INNER JOIN 
          msdb.dbo.syscategories cat
		     ON job.category_id = cat.category_id
     WHERE CONVERT(DATETIME, CONVERT(CHAR(8), run_date, 112) + ' ' + STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), run_time), 6), 5, 0, ':'), 3, 0, ':'), 120) BETWEEN @vd_JobStart and @vd_JobEnd
     AND	 step_id = 0 -- (step_id = 0) entire job - (step_id > 0) actual step number
     AND	((run_duration/10000) % 100 * 3600) + ((run_duration/100) % 100 * 60) + run_duration % 100 > @vn_MinRuntimeSec  
     ORDER BY StartDate
     
     IF @@ROWCOUNT > 0
     BEGIN

          -- Formatação para utilização do Google Graphs
          -- 1. Header
          INSERT INTO ##FinalGraph 
                    ( html ) 
          SELECT '<html>
	               <head>
	               <!--<META HTTP-EQUIV="refresh" CONTENT="1">-->
	              <script type="text/javascript" src="https://www.google.com/jsapi?autoload={''modules'':[{''name'':''visualization'', ''version'':''1'',''packages'':[''timeline'']}]}"></script>
                    <script type="text/javascript">'
          
          INSERT INTO ##FinalGraph 
                    ( html ) 
          SELECT 'google.setOnLoadCallback(drawChart);
	             function drawChart() {'

          -- Percorre as datas para geração do gráfico por dia
          DECLARE cGraphDate CURSOR READ_ONLY
          FOR
               SELECT StartDate, EndDate
               FROM #GraphDates         
               ORDER BY StartDate;
               
          OPEN cGraphDate;
          
          FETCH NEXT 
          FROM cGraphDate                
          INTO @vd_cGraphDateStartDate
             , @vd_cGraphDateEndDate

          WHILE (@@FETCH_STATUS = 0)
          BEGIN

               INSERT INTO ##FinalGraph 
                         ( html ) 
               SELECT ' //** Data: ' + FORMAT(@vd_cGraphDateStartDate, 'd', 'pt-br') + '
                        var container' + CONVERT(VARCHAR(10), @vn_GraphId) + ' = document.getElementById(''JobTimeline' + CONVERT(VARCHAR(10), @vn_GraphId) + ''');
	                   var chart' + CONVERT(VARCHAR(10), @vn_GraphId) + ' = new google.visualization.Timeline(container' + CONVERT(VARCHAR(10), @vn_GraphId) + ');
	                   var dataTable' + CONVERT(VARCHAR(10), @vn_GraphId) + ' = new google.visualization.DataTable();'
          
               INSERT INTO ##FinalGraph 
                         ( html ) 
               SELECT ' dataTable' + CONVERT(VARCHAR(10), @vn_GraphId) + '.addColumn({ type: ''string'', id: ''Position'' });
	                   dataTable' + CONVERT(VARCHAR(10), @vn_GraphId) + '.addColumn({ type: ''string'', id: ''Name'' });
	                   dataTable' + CONVERT(VARCHAR(10), @vn_GraphId) + '.addColumn({ type: ''date'', id: ''Start'' });
	                   dataTable' + CONVERT(VARCHAR(10), @vn_GraphId) + '.addColumn({ type: ''date'', id: ''End'' });
	                   dataTable' + CONVERT(VARCHAR(10), @vn_GraphId) + '.addRows([
               '

               -- 2. Dados
               INSERT INTO ##FinalGraph 
                         ( html ) 
               SELECT  '		[ ' 
		               +'''' + CatName  + ''', '
		               +'''' + JobName  + ''', '
		               +'new Date('
		               +        CAST(DATEPART(year ,  StartDate) AS VARCHAR(4))
		               + ', ' + CAST(DATEPART(month,  StartDate) -1 AS VARCHAR(4)) -- Meses iniciam em 0 (Java)
		               + ', ' + CAST(DATEPART(day,    StartDate) AS VARCHAR(4))
		               + ', ' + CAST(DATEPART(hour,   StartDate) AS VARCHAR(4))
		               + ', ' + CAST(DATEPART(minute, StartDate) AS VARCHAR(4))
		               + ', ' + CAST(DATEPART(second, StartDate) AS VARCHAR(4)) 
		               +'), '

		               +'new Date('
		               +        CAST(DATEPART(year,   EndDate) AS VARCHAR(4))
		               + ', ' + CAST(DATEPART(month,  EndDate) -1 AS VARCHAR(4)) -- Meses iniciam em 0 (Java)
		               + ', ' + CAST(DATEPART(day,    EndDate) AS VARCHAR(4))
		               + ', ' + CAST(DATEPART(hour,   EndDate) AS VARCHAR(4))
		               + ', ' + CAST(DATEPART(minute, EndDate) AS VARCHAR(4))
		               + ', ' + CAST(DATEPART(second, EndDate) AS VARCHAR(4)) 
		               + ') ],' 
               FROM	#JobRuntime 
               WHERE StartDate BETWEEN @vd_cGraphDateStartDate
                                   AND @vd_cGraphDateEndDate
               ORDER BY CatName, JobName, StartDate;

               SELECT @vn_CategoryCount = COUNT(DISTINCT CatName)
               FROM	#JobRuntime 
               WHERE StartDate BETWEEN @vd_cGraphDateStartDate
                                   AND @vd_cGraphDateEndDate; 

               SELECT @vn_JobCountByCategory = AVG(JobCount * 1.0)
               FROM ( 
                      SELECT CatName, COUNT(JobName) AS JobCount
                      FROM (
                             SELECT DISTINCT CatName, JobName
                             FROM #JobRuntime
                             WHERE StartDate BETWEEN @vd_cGraphDateStartDate
                                                 AND @vd_cGraphDateEndDate
                           ) AS jDistinct
                      GROUP BY CatName                                   
                    ) AS jCount

	          SET @vn_HeightInPixels = @vn_RowHeightInPixels * (((@vn_CategoryCount * 1.0) * @vn_JobCountByCategory) + 2)

              -- 3. Scripts
               INSERT INTO ##FinalGraph 
                         ( html ) 
               SELECT '	]);

	               var options' + CONVERT(VARCHAR(10), @vn_GraphId) + ' = 
	               {
		               timeline: 	{ 
					               groupByRowLabel: true,
					               colorByRowLabel: false,
					               singleColor: false,
					               rowLabelStyle: {fontName: ''Lucida Sans Unicode'', fontSize: 11 },
					               barLabelStyle: {fontName: ''Lucida Sans Unicode'', fontSize: 11 }					
					               },
                         "height": ' + CONVERT(VARCHAR(10), @vn_HeightInPixels) + ',
                         "width": ' + CONVERT(VARCHAR(10), @vn_WidthInPixels) + '
	               };
                    '

               INSERT INTO ##GraphDiv 
                         ( html ) 
               SELECT '    <hr>de ' + FORMAT(@vd_cGraphDateStartDate, 'd', 'pt-br') + ' até ' + FORMAT(@vd_cGraphDateEndDate, 'd', 'pt-br') + '<hr> <div id="JobTimeline' + CONVERT(VARCHAR(10), @vn_GraphId) + '"></div>'

               INSERT INTO ##GraphDraw 
                         ( html ) 
               SELECT '    chart' + CONVERT(VARCHAR(10), @vn_GraphId) + '.draw(dataTable' + CONVERT(VARCHAR(10), @vn_GraphId) + ', options' + CONVERT(VARCHAR(10), @vn_GraphId) + ');'
               
               SET @vn_GraphId += 1;

               FETCH NEXT 
               FROM cGraphDate                
               INTO @vd_cGraphDateStartDate
                  , @vd_cGraphDateEndDate


          END

          CLOSE cGraphDate;
          DEALLOCATE cGraphDate;

          -- Realiza a inclusão dos desenhos
          INSERT INTO ##FinalGraph (html)
          SELECT html FROM ##GraphDraw;

          -- Última parte do script e inicio do corpo do html
          INSERT INTO ##FinalGraph 
                    ( html ) 
          SELECT '
          }
	          </script>
	          </head>
	          <body>'
	          + '<font face="Lucida Sans Unicode" size="2" >'
	          + 'Timeline de jobs do servidor: '+@@servername + ' (Node: ' + CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS VARCHAR(128)) + ')' 
               + ' de ' + FORMAT(@vd_cGraphDateStartDate, 'd', 'pt-br')
               + ' até ' + FORMAT(@vd_cGraphDateEndDate, 'd', 'pt-br')
	          + CASE WHEN @vn_MinRuntimeSec = 0 
                         THEN '' 
                         ELSE ' (Jobs com menos de ' + CAST(@vn_MinRuntimeSec AS VARCHAR(10)) + ' segundos foram ignorados.)' 
                    END
	          + '</font>'

          -- Divs
          INSERT INTO ##FinalGraph (html)
          SELECT html FROM ##GraphDiv;

          INSERT INTO ##FinalGraph (html)
          SELECT '  </body>
          </html>'

     END

      -- Rodapé do E-mail    
      SELECT @vc_HtmlScript = @vc_HtmlScript + '</table>' + 
     '<p style="margin-top: 0; margin-bottom: 0">&nbsp;</p>
     <hr color="#000000">
        <p style="margin-top: 0; margin-bottom: 0"><size="2">Administrador Responsável: Rafael Rodrigues</font></p>
     <p>&nbsp;</p>'     

      -- Realiza o Envio do E-mail
      EXEC msdb.dbo.sp_send_dbmail
                    @profile_name = @pc_EmailProfile  
                  , @recipients = @pc_EmailOperator  
                  , @subject = @vc_Subject  
                  , @body = @vc_HtmlScript   
                  , @body_format = 'HTML'
                  , @importance = 'Normal' 
                  , @sensitivity = 'Normal' 
 /* -- Anexo
                  , @execute_query_database = 'dbaMonitor'
                  , @query_result_header = 1
                  , @query = 'SET NOCOUNT ON; SELECT html FROM ##FinalGraph ORDER BY IdOrder;SELECT 1 AS t;'
                  , @query_result_no_padding = 1  -- Previne adicionar espaços no resultado
                  , @attach_query_result_as_file = 1
                  , @query_attachment_filename= 'SQLServerJobs.html'
-- */

     -- Limpeza de objetos
     IF OBJECT_ID('tempdb..#JobRuntime') IS NOT NULL 
          DROP TABLE #JobRuntime;

     IF OBJECT_ID('tempdb..##FinalGraph') IS NOT NULL 
          DROP TABLE ##FinalGraph;

     IF OBJECT_ID('tempdb..##GraphDiv') IS NOT NULL
          DROP TABLE ##GraphDiv;

     IF OBJECT_ID('tempdb..##GraphDraw') IS NOT NULL
          DROP TABLE ##GraphDraw;

     IF OBJECT_ID('tempdb..#GraphDates') IS NOT NULL
          DROP TABLE #GraphDates


    END TRY

    BEGIN CATCH

        -- Recupera informações originais do erro
        SELECT @vc_ErrorMsg       = ERROR_MESSAGE()
             , @vn_ErrorSeverity  = ERROR_SEVERITY ()
             , @vn_ErrorState     = ERROR_STATE ()
             , @vc_ErrorProcedure = ERROR_PROCEDURE ();

        -- Tratamento Para ErrorState, retorna a procedure de execução em junção com o erro.
        SELECT @vc_ErrorMsg = CASE WHEN @vn_ErrorState = 1
                  THEN @vc_ErrorMsg + CHAR(13 ) + 'O erro ocorreu em ' + @vc_ErrorProcedure + ' ( ' + LTRIM ( RTRIM ( STR ( ERROR_LINE () ) ) ) + ' )'
              WHEN @vn_ErrorState = 3
                  THEN @vc_ErrorProcedure + ' - ' + @vc_ErrorMsg
              ELSE @vc_ErrorMsg
             END;

        RAISERROR (@vc_ErrorMsg
             ,@vn_ErrorSeverity
             ,@vn_ErrorState);

        IF @vn_TranCount   = 0 AND  -- Transação feita no escopo da procedure
            XACT_STATE() != 0      -- Transação ativa existente
        BEGIN
            ROLLBACK TRANSACTION ;
        END


    END CATCH

    IF @vn_TranCount  = 0 AND -- Transação feita no escopo da procedure
        XACT_STATE()  = 1     -- Transação com sucesso de execução
    BEGIN
        COMMIT TRANSACTION ;
    END

 END



 /*
 
 EXEC [dbaMonitor].[dbo].[spu_SQLServerReport]
       @pc_EmailOperator  = 'rafael.rodrigues@oab.org.br'
     , @pc_EmailProfile   = NULL
     , @pc_CheckBackupSet = 0


 */


 


 
GO
