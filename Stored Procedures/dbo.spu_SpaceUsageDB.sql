SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[spu_SpaceUsageDB] 
( @pc_Database    VARCHAR(128) = 'ALL'        
, @pc_Level       CHAR(8)      = 'DATABASE'   
, @pn_UpdateUsage BIT          = 0            
, @pc_Unit        CHAR(2)      = 'GB'         
, @pn_Summary     BIT          = 1            
, @pn_ReturnXML   BIT          = 0            
, @px_XMLOutput   XML OUTPUT
) 
AS
BEGIN
/******************************************************************************************************
**
**  Name.........: spu_SpaceUsageDB
**
**  Descrição....: Gera um relatório de informações de espaço utilizado por databases ou arquivos.
**
**  Return values:
**
**  Chamada por..:
**
**  Parâmetros:
**  Entradas            Descrição
**  ------------------- -------------------------------------------------------------------------------
**  @pc_Database        Nome do Database
**  @pc_Level           Nível de verificação - Database ou File
**  @pn_UpdateUsage     Atualização de relatório de estatísticas de páginas
**  @pc_Unit            Unidade de visualização de espaço alocado - GB ou MB
**  @pn_Summary         Lista sumário com totais de espaço utilizado
**  @pn_ReturnXML       
**  @px_XMLOutput       Retorno para XML ou não
**
**  Saídas              Descrição
**  ------------------- -------------------------------------------------------------------------------
**
**  Observações..:
**
**  Autor........: Rafael Rodrigues
**  Data.........: 18/02/2010
******************************************************************************************************
**  Histórico de Alterações
******************************************************************************************************
** Data:    Autor:                Descrição:                                                    Versão
** -------- --------------------- ------------------------------------------------------------- ------
**
**
******************************************************************************************************/
 
   SET NOCOUNT ON

   DECLARE @vn_Error           INT
         , @vn_RowCount        INT
         , @vn_TranCount       INT
         , @vn_ErrorState      INT
         , @vn_ErrorSeverity   INT
         , @vc_ErrorProcedure  VARCHAR( 256)
         , @vc_ErrorMsg        VARCHAR( MAX);

   -- Definição dos parâmetros de entrada
   SET @pc_Database    = ISNULL( @pc_Database, 'ALL' );
   SET @pc_Level       = ISNULL( @pc_Level, 'DATABASE' );
   SET @pn_UpdateUsage = ISNULL( @pn_UpdateUsage, 0 );
   SET @pc_Unit        = ISNULL( @pc_Unit, 'GB' );
 
   IF  ( ISNULL(@pc_Database, 'ALL') != ('ALL' )
   AND NOT EXISTS ( SELECT 1
                   FROM Master.sys.Databases
                   WHERE name = @pc_Database
                 )
       )
   BEGIN
      RAISERROR('O parâmetro @pc_Database deve conter o nome de um database existente no servidor ou ALL para listar todos os databases.', 16, 1);
      RETURN;
   END

   IF ( ISNULL(@pc_Level, 'DATABASE') NOT IN ('DATABASE', 'FILE' ) )
   BEGIN
      RAISERROR('O parâmetro @pc_Level deve ser DATABASE para listagem por banco de dados ou FILE para listagem por arquivos.', 16, 1);
      RETURN;
   END

   IF ( ISNULL(@pc_Unit, 'GB') NOT IN ('GB', 'MB' ) )
   BEGIN
      RAISERROR('O parâmetro @pc_Unit ser GB para listagem em gigabytes de dados ou MB para listagem por megabytes.', 16, 1);
      RETURN;
   END

   -- Se o retorno for por XML, o sumário não é retornado
   IF @pn_ReturnXML = 1
   BEGIN
      SET @pn_Summary = 0
   END
   -- Declaração de variáveis
   DECLARE @vc_Version      VARCHAR(10)
         , @vc_DBName       NVARCHAR(128)
         , @vn_LastIdentity INT
         , @vc_String       VARCHAR(2000)
         , @vc_StringBase   NVARCHAR(2000)
         , @vc_ParamExec    NVARCHAR(500) 
         , @vn_GrantTotal   DECIMAL(11, 2);

   DECLARE @vt_Databases TABLE
   (
     id     INT IDENTITY(1, 1)
   , dbname SYSNAME NULL
   );

   DECLARE @vt_EstatisticaLog TABLE
   (
     dbname              SYSNAME        NULL
   , logsize             DECIMAL(10, 2) NULL
   , logspaceusedpercent DECIMAL(5, 2)  NULL
   , status              INT            NULL
   );

   DECLARE @vt_StatisticsDB TABLE
   (
     Databasename  SYSNAME NULL
   , [Type]        VARCHAR (10)   NULL
   , Logicalname   SYSNAME        NULL
   , Size          DECIMAL(10, 2) NULL
   , DataUsed      DECIMAL(10, 2) NULL
   , [DataUsed(%)] DECIMAL(5, 2)  NULL
   , DataFree      DECIMAL(10, 2) NULL
   , [DataFree(%)] DECIMAL(5, 2)  NULL
   , Physicalname  SYSNAME        NULL
   );

   DECLARE @vt_StatisticsArch TABLE
   (
     id           INT IDENTITY(1, 1)
   , Databasename SYSNAME       NULL
   , Fileid       INT           NULL
   , Filegroup    INT           NULL
   , TotalExtents BIGINT        NULL
   , UsedExtents  BIGINT        NULL
   , Name         SYSNAME       NULL
   , Filename     VARCHAR (255) NULL
   );

   DECLARE @vt_Report TABLE
   (
     databasename    SYSNAME NULL
   , total           DECIMAL(10, 2)
   , used            DECIMAL(10, 2)
   , [used (%)]      DECIMAL (5, 2)
   , free            DECIMAL(10, 2)
   , [free (%)]      DECIMAL(5, 2)
   , data            DECIMAL(10, 2)
   , data_used       DECIMAL(10, 2)
   , [data_used (%)] DECIMAL(5, 2)
   , data_free       DECIMAL(10, 2)
   , [data_free (%)] DECIMAL(5, 2)
   , log             DECIMAL(10, 2)
   , log_used        DECIMAL(10, 2)
   , [log_used (%)]  DECIMAL (5, 2)
   , log_free        DECIMAL(10, 2)
   , [log_free (%)]  DECIMAL (5, 2)
   );


   -- Definição de variáveis
   SET @vc_DBName        = '';
   SET @vn_LastIdentity  = 0;
   SET @vc_String        = '';
   SET @vc_Version       = CONVERT(VARCHAR(128), SERVERPROPERTY('ProductVersion'));
   SELECT @vc_Version = CASE WHEN @vc_Version LIKE '%9.0%' 
                            THEN 'SQL 2005'
                            WHEN @vc_Version LIKE '%8.0%' 
                            THEN 'SQL 2000'
                            WHEN @vc_Version LIKE '%10.0%'
                            THEN 'SQL 2008'
                            WHEN @vc_Version LIKE '%10.5%'
                            THEN 'SQL 2008R2'
                       END;

   BEGIN TRY

      SELECT @vc_StringBase = CASE WHEN @vc_Version = 'SQL 2000'
                                   THEN 'SELECT '''' + @vc_DBName + '''' , ' 
                                   ELSE 'SELECT databases.name ,'
                              END +
                              CASE WHEN @vc_Version = 'SQL 2000'
                                   THEN 'CASE WHEN status & 0x40 = 0x40
                                              THEN ''Log''
                                              ELSE ''Data''
                                         END'
                                   ELSE ' CASE type WHEN 0
                                                    THEN ''Data''
                                                    WHEN 1
                                                    THEN ''Log''
                                                    WHEN 4
                                                    THEN ''Full-text''
                                                    ELSE ''reserved''
                                           END'
                               END + ', sysfiles.name, ' +
                               CASE WHEN @vc_Version = 'SQL 2000'
                                    THEN 'Filename'
                                    ELSE 'Physical_name'
                               END
                               + ', size * 8.0/1024.0
                         FROM ' + CASE WHEN @vc_Version = 'SQL 2000'
                                       THEN 'sysfiles'
                                       ELSE 'Master.sys.master_files sysfiles
                                             INNER JOIN Master.sys.databases
                                                   ON sysfiles.database_id = databases.database_id'
                                 END + ' WHERE '
                                + CASE WHEN @vc_Version = 'SQL 2000'
                                       THEN ' HAS_DBACCESS(DB_NAME()) = 1'
                                       ELSE 'sysfiles.state_desc = ''ONLINE'''
                                 END + '';

      SET @vc_ParamExec = N'@vc_DBName SYSNAME';

      -- Armazena lista de databases ativos
      IF ( @vc_Version = 'SQL 2000' )
      BEGIN

         INSERT INTO @vt_Databases (dbname)
            SELECT name
            FROM Master.dbo.sysDatabases
            WHERE HAS_DBACCESS(name) = 1
            ORDER BY name ASC

      END
      ELSE
      BEGIN

         INSERT INTO @vt_Databases (dbname)
            SELECT name
            FROM Master.sys.Databases
            WHERE HAS_DBACCESS(name) = 1
            ORDER BY name ASC

      END

      -- Recupera estatísticas de logs
      INSERT INTO @vt_EstatisticaLog
         EXEC ('DBCC sqlperf (logspace) WITH NO_INFOMSGS' );

      /*
      ##################################
      # Informações de bancos de dados #
      ##################################
      */

      -- Busca informações de arquivos de bancos de dados
      INSERT INTO @vt_StatisticsDB ( DatabaseName
                                    , Type
                                    , LogicalName
                                    , PhysicalName
                                    , Size
                                    )
         EXEC Master.dbo.sp_ExecuteSQL @vc_StringBase
                                       , @vc_ParamExec
                                       , @vc_DBName = @vc_DBName;

      DECLARE cur_Databases CURSOR
      FOR
         SELECT dbname
         FROM @vt_Databases
         WHERE @pc_Database = 'ALL'
         OR    dbname = @pc_Database

      OPEN cur_Databases

      FETCH NEXT FROM cur_Databases
      INTO @vc_DBName
     
      WHILE @@FETCH_STATUS = 0
      BEGIN
     
         -- Caso requerido, atualiza contadores de uso de arquivos
         IF  @pn_UpdateUsage != 0
         AND DATABASEPROPERTYEX(@vc_DBName , 'Status') = 'ONLINE'             -- O Banco de Dados deve estar online
         AND DATABASEPROPERTYEX(@vc_DBName , 'Updateability') != 'READ_ONLY'  -- O Banco de Dados não pode estar Read_Only
         BEGIN

            SET @vc_String = 'DBCC UPDATEUSAGE (''' + @vc_DBName + ''') ';

            EXEC Master.dbo.sp_ExecuteSQL @vc_String;

         END

         INSERT INTO @vt_StatisticsArch ( Fileid
                                        , Filegroup
                                        , TotalExtents
                                        , usedextents
                                        , Name
                                        , Filename
                                        )
            EXEC ( 'USE [' + @vc_DBName + '] DBCC SHOWFILESTATS WITH NO_INFOMSGS' );

            UPDATE @vt_StatisticsArch
               SET Databasename = @vc_DBName
            WHERE Databasename IS NULL

            FETCH NEXT FROM cur_Databases
            INTO @vc_DBName

      END

      CLOSE cur_Databases
      DEALLOCATE cur_Databases

      /*
      #################################
      # Ajuste de informações geradas #
      #################################
      */

      -- Define o espaço utilizado por datafiles.
      UPDATE @vt_StatisticsDB
         SET DataUsed = sf.usedextents * 8 * 8 / 1024.0
      FROM @vt_StatisticsDB db
       INNER JOIN @vt_StatisticsArch sf
               ON db.logicalname = sf.name
                  AND sf.Databasename = db.Databasename;

      -- Define tamanho utilizado e valores em % para log files
      UPDATE @vt_StatisticsDB
         SET [DataUsed(%)] = logspaceusedpercent,
             DataUsed = Size * logspaceusedpercent / 100.0
      FROM @vt_StatisticsDB db
             INNER JOIN @vt_EstatisticaLog lg
                     ON lg.dbname = db.Databasename
      WHERE db.type = 'Log' ;

      UPDATE @vt_StatisticsDB
      SET    DataFree = Size - DataUsed,
             [DataUsed(%)] = DataUsed * 100.0 / Size;

      UPDATE @vt_StatisticsDB
      SET    [DataFree(%)] = DataFree * 100.0 / Size;

      -- Gera relatório por arquivos de dados
      IF UPPER(ISNULL(@pc_Level, 'DATABASE')) = 'FILE'
      BEGIN
     
         -- Relatório em Kilobytes
         IF @pc_Unit = 'KB'
         BEGIN
            UPDATE @vt_StatisticsDB
               SET Size     = Size * 1024
                 , DataUsed = DataUsed * 1024
                 , DataFree = DataFree * 1024;
         END

         -- Relatório em Gigabytes
         IF @pc_Unit = 'GB'
         BEGIN
              UPDATE @vt_StatisticsDB
                 SET Size     = Size / 1024
                   , DataUsed = DataUsed / 1024
                   , DataFree = DataFree / 1024;
         END

         IF ( @pn_ReturnXML = 0 )
         BEGIN

            SELECT Databasename        AS 'Database'
                 , Type                AS 'Type'
                 , Logicalname
                 , Size                AS 'Total'
                 , DataUsed            AS 'Used'
                 , [DataUsed(%)]       AS 'Used (%)'
                 , DataFree            AS 'Free'
                 , [DataFree(%)]       AS 'Free (%)'
                 , physicalname
            FROM @vt_StatisticsDB
            WHERE  ( Databasename LIKE ISNULL( @pc_Database, '%')
                  OR @pc_Database = 'ALL' )
            ORDER BY Databasename ASC
                   , Type ASC;

         END
         ELSE
         BEGIN

            SET @px_XMLOutput =
            (
              SELECT Databasename        AS 'Database'
                   , Type                AS 'Type'
                   , Logicalname
                   , Size                AS 'Total'
                   , DataUsed            AS 'Used'
                   , [DataUsed(%)]       AS 'Used_Percent'
                   , DataFree            AS 'Free'
                   , [DataFree(%)]       AS 'Free_Percent'
                   , physicalname
              FROM @vt_StatisticsDB
              WHERE  ( Databasename LIKE ISNULL( @pc_Database, '%')
                    OR @pc_Database = 'ALL' )
              ORDER BY Databasename ASC
                     , type ASC
              FOR XML PATH('diskspace'), ROOT('file')
            )

         END

         IF @pn_Summary = 1
         BEGIN

            SELECT CASE WHEN @pc_Unit NOT IN ( 'GB', 'KB' )
                        THEN 'MB'
                        ELSE @pc_Unit
                     END     AS 'Unidade',
                     Sum (Size) AS 'Total' ,
                     Sum (DataUsed) AS 'Usado' ,
                     Sum (DataFree) AS 'Livre'
            FROM @vt_StatisticsDB;

         END
      END

      -- Gera relatório por Databases
      IF UPPER(ISNULL(@pc_Level, 'DATABASE')) = 'DATABASE'
      BEGIN

         INSERT INTO @vt_Report (
                                     databasename   
                                   , total          
                                   , used           
                                   , [used (%)]     
                                   , free           
                                   , [free (%)]     
                                   , data           
                                   , data_used      
                                   , [data_used (%)]
                                   , data_free      
                                   , [data_free (%)]
                                   , log            
                                   , log_used       
                                   , [log_used (%)] 
                                   , log_free       
                                   , [log_free (%)] 
                                   )
            SELECT db.databasename
                 , db.data + lg.LogS                                            
                 , db.Data_Used + lg.Log_Used                                  
                 , ( db.Data_Used + lg.Log_Used ) * 100.0 / ( db.data + lg.LogS )
                 , db.Data_Free + lg.Log_Free                                  
                 , ( db.Data_Free + lg.Log_Free ) * 100.0 / ( db.data + lg.LogS )
                 , db.data
                 , db.Data_Used
                 , db.Data_Used * 100 / db.Data                              
                 , db.Data_Free
                 , db.Data_Free * 100 / db.Data                              
                 , lg.LogS
                 , lg.Log_Used
                 , lg.Log_Used * 100 / lg.LogS                               
                 , lg.Log_Free
                 , lg.Log_Free * 100 / lg.LogS                               
            FROM ( SELECT db.Databasename DatabaseName
                        , SUM(db.Size) AS 'Data'
                        , SUM(db.DataUsed) AS 'Data_Used'
                        , SUM(db.DataFree) AS 'Data_Free'
                  FROM @vt_StatisticsDB db
                  WHERE db.Type = 'Data'
                  GROUP BY db.Databasename
                 ) AS db INNER JOIN ( SELECT lg.Databasename
                                           , SUM(lg.Size) AS 'LogS'
                                           , SUM(lg.DataUsed) AS 'Log_Used'
                                           , SUM(lg.DataFree) AS 'Log_Free'
                                     FROM @vt_StatisticsDB lg
                                     WHERE lg.type = 'Log'
                                     GROUP BY lg.databasename
                                   ) AS lg
                     ON db.Databasename = lg.Databasename;

         IF @pc_Unit = 'KB'
         BEGIN
            UPDATE @vt_Report
               SET total     = total * 1024,
                   used      = used * 1024,
                   free      = free * 1024,
                   data      = data * 1024,
                   data_used = data_used * 1024,
                   data_free = data_free * 1024,
                   log       = log * 1024,
                   log_used  = log_used * 1024,
                   log_free  = log_free * 1024;
         END

         IF @pc_Unit = 'GB'
         BEGIN
            UPDATE @vt_Report
               SET total     = total / 1024,
                   used      = used / 1024,
                   free      = free / 1024,
                   data      = data / 1024,
                   data_used = data_used / 1024,
                   data_free = data_free / 1024,
                   log       = log / 1024,
                   log_used  = log_used / 1024,
                   log_free  = log_free / 1024;
         END

         SELECT @vn_GrantTotal = SUM(total)
         FROM   @vt_Report ;

         IF ( @pn_ReturnXML = 0 )
         BEGIN

            SELECT 'Porcentagem (%)' = CONVERT (DECIMAL( 10, 2 ), total * 100.0 / @vn_GrantTotal ) 
                 , 'Banco de Dados'  = Databasename
                 , 'Usado (%)'       = CONVERT(VARCHAR(12), used) + ' (' + CONVERT(VARCHAR(12), [used (%)]) + ' %)'
                 , 'Livre (%)'       = CONVERT(VARCHAR(12), free) + ' (' + CONVERT(VARCHAR(12), [free (%)]) + ' %)'          
                 , 'Total'           = total
                 , 'DATA (Usado, %)' = CONVERT(VARCHAR (12), data) + ' (' + CONVERT (VARCHAR( 12), data_used) + ', ' + CONVERT (VARCHAR( 12), [data_used (%)]) + '%)'
                 , 'LOG (Usado, %)'  = CONVERT(VARCHAR (12), log) + ' (' + CONVERT (VARCHAR( 12), log_used) + ', ' + CONVERT (VARCHAR( 12), [log_used (%)]) + '%)'
            FROM   @vt_Report
            WHERE  ( Databasename LIKE ISNULL( @pc_Database, '%')
                  OR @pc_Database = 'ALL' )
            ORDER  BY Databasename ASC;

         END
         ELSE
         BEGIN

            SET @px_XMLOutput =
            (
              SELECT 'Porcentagem'        = CONVERT (DECIMAL( 10, 2 ), total * 100.0 / @vn_GrantTotal ) 
                   , 'Banco_de_Dados'     = Databasename
                   , 'Usado_Percent'      = CONVERT(VARCHAR(12), used) + ' (' + CONVERT(VARCHAR(12), [used (%)]) + ' %)'
                   , 'Livre_Percent'      = CONVERT(VARCHAR(12), free) + ' (' + CONVERT(VARCHAR(12), [free (%)]) + ' %)'          
                   , 'Total'              = total
                   , 'DATA_Usado_Percent' = CONVERT(VARCHAR (12), data) + ' (' + CONVERT (VARCHAR( 12), data_used) + ', ' + CONVERT (VARCHAR( 12), [data_used (%)]) + '%)'
                   , 'LOG_Usado_Percent'  = CONVERT(VARCHAR (12), log) + ' (' + CONVERT (VARCHAR( 12), log_used) + ', ' + CONVERT (VARCHAR( 12), [log_used (%)]) + '%)'
              FROM   @vt_Report
              WHERE  ( Databasename LIKE ISNULL( @pc_Database, '%')
                 OR @pc_Database = 'ALL' )
              FOR XML PATH('diskspace'), ROOT('db')
            )

         END

         IF @pn_Summary = 1
         BEGIN

            IF ISNULL(@pc_Database, 'ALL') = 'ALL'
            BEGIN

               SELECT CASE WHEN @pc_Unit NOT IN ( 'GB', 'KB' )
                           THEN 'MB'
                           ELSE @pc_Unit
                        END     AS 'Unidade'
                    , SUM(used)  AS 'Usado'
                    , SUM(free)  AS 'Livre'
                    , SUM(total) AS 'Total'
                    , SUM(data)  AS 'DATA'
                    , SUM(log)   AS 'LOG'
               FROM @vt_Report;

            END

         END

      END

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

      RAISERROR( @vc_ErrorMsg
               , @vn_ErrorSeverity
               , @vn_ErrorState );

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
DECLARE @xml xml
EXEC [spu_SpaceUsageDB] @px_XMLOutput = @xml output

*/
GO
