SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[spu_TrackDatabaseFiles]
AS
BEGIN

   /* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~   
   **   
   **  Name         : spu_TrackDatabaseFiles
   **
   **  Database     : 
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
   **   
   **   
   **  Autor........: Rafael Rodrigues
   **  Data.........: 02/05/2014
   **
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~   
   **  Histórico de Alterações   
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~   
   **  Data:    Autor:             Descrição:                                                Versão   
   **  -------- ------------------ --------------------------------------------------------- ------   
   **   
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~   
   **                              © dbaScripts - Rafael Rodrigues   
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */       

     SET NOCOUNT ON 
   
     DECLARE @vn_Error          INT   
           , @vn_RowCount       INT   
           , @vn_TranCount      INT   
           , @vn_ErrorState     INT   
           , @vn_ErrorSeverity  INT   
           , @vc_ErrorProcedure VARCHAR(256)   
           , @vc_ErrorMsg       VARCHAR(MAX);

     DECLARE @vn_srvId        TINYINT
           , @vd_GatherDate   DATE = CONVERT(DATE, SYSDATETIME())
           , @vc_DatabaseName NVARCHAR(128);

     DECLARE @vc_SQLCmd     NVARCHAR(MAX)
           , @vc_ExecSQLCmd NVARCHAR(4000);

     -- Caso a tabela temporária exista, remove..
     IF (SELECT ISNULL(OBJECT_ID('tempdb..#dbFileSpecs'), 0)) != 0
     BEGIN
          DROP TABLE #dbFileSpecs
     END
   
     -- Criação de tabela temporária para filtro e ordenação
     CREATE TABLE #dbFileSpecs 
     (
       [dbsId]                 INT           NOT NULL
     , [GatherDate]            DATE          NOT NULL
     , [DatabaseName]          NVARCHAR(128) NOT NULL
     , [LogicalName]           NVARCHAR(260) NOT NULL     
     , [FileType]              NVARCHAR(60)  NOT NULL
     , [FileSizeGB]            DECIMAL(15,2) NOT NULL
     , [FreeSpaceInFileGB]     DECIMAL(15,2) NOT NULL
     , [VolumeLabel]           VARCHAR(128)  NOT NULL
     , [VolumeTotalSizeGB]     DECIMAL(15,2) NOT NULL
     , [VolumeAvailableSizeGB] DECIMAL(15,2) NOT NULL
     , [FileStateDesc]         NVARCHAR(60)  NOT NULL
     , [FileGrowth]            DECIMAL(15,2) NOT NULL
     , [MaxFileSizeGB]         NVARCHAR(60)  NOT NULL
     , [IsReadOnly]            CHAR(3)       NOT NULL
     ) 

     -- Query modelo para pesquisa de informações de arquivos
     SET @vc_SQLCmd = 'USE {databasename};
                       SELECT [dbsId] = dbs.database_id
                            , [GatherDate] = CONVERT(DATE, SYSDATETIME())
                            , [DatabaseName] = dbs.name
                            , [LogicalName] = dbfs.name
                            , [FileType] = CASE WHEN dbfs.type = 0
                                                THEN ''Data''
                                                WHEN dbfs.type = 1
                                                THEN ''Log''
                                                ELSE dbfs.type_desc
                                           END
                            , [FileSizeGB] = CONVERT(DECIMAL(15,2), (CONVERT(BIGINT, dbfs.size) * 8192.0) / POWER(1024.0, 3))
                            , [FreeSpaceInFileGB] = CONVERT(DECIMAL(15,2), (CONVERT(BIGINT, dbfs.size) - FILEPROPERTY(dbfs.name, ''SPACEUSED'')) * 8192.0 / POWER(1024.0, 3))
                            , [VolumeLabel] = osvs.logical_volume_name + '' [ '' + osvs.volume_mount_point + '']''
                            , [VolumeTotalSizeGB] = CONVERT(DECIMAL(15,2), ROUND(osvs.total_bytes / POWER(1024.0, 3), 0))
                            , [VolumeAvailableSizeGB] = CONVERT(DECIMAL(15,2), osvs.available_bytes / POWER(1024.0, 3))
                            , [FileStateDesc] = dbfs.state_desc
                            , [FileGrowth] = CONVERT(DECIMAL(15,2), CONVERT(BIGINT, dbfs.growth) * 8192.0 / POWER(1024.0, 2))
                            , [MaxFileSizeGB] = CASE WHEN dbfs.max_size = -1
                                                     THEN ''Unlimited''
                                                     ELSE LTRIM(STR(CONVERT(DECIMAL(15,2), CONVERT(BIGINT, dbfs.max_size) * 8192.0 / POWER(1024.0, 3))))
                                                END
                            , [IsReadOnly] = CASE WHEN dbfs.is_read_only = 1
                                                THEN ''Yes''
                                                ELSE ''No''
                                           END
                       FROM sys.database_files dbfs
                            INNER JOIN 
                            sys.databases dbs
                                 ON DB_ID() = dbs.database_id
                            CROSS APPLY
                            sys.dm_os_volume_stats(dbs.database_id, dbfs.file_id) osvs
                       WHERE dbfs.type = 0                         
                      '; 

     -- Controle de Transações 
     SET @vn_TranCount = @@TRANCOUNT;

     BEGIN TRY;

          -- Recupera o identificador do servidor
          SELECT @vn_srvId = srvId
          FROM dbaMonitor.dbo.Servers
          WHERE srvName = @@SERVERNAME;

          IF ( ISNULL( @vn_srvId, 0 ) = 0 ) 
          BEGIN
          RAISERROR('O servidor %s ainda não encontra-se cadastrado no banco de dados dbaMonitor.', 16, 1, @@SERVERNAME);
          END

          -- Verifica se os databases do servidor Host já estão na tabela dbaMonitor.dbo.Databases, senão, realiza a inclusão
          INSERT INTO dbaMonitor.dbo.Databases ( srvId, dbsId, dbsName ) 
          SELECT @vn_srvId, database_id, db.name
          FROM master.sys.databases db
          WHERE NOT EXISTS ( SELECT 1 
                             FROM dbaMonitor.dbo.Databases MonDB
                             WHERE MonDB.srvId = @vn_srvId
                             AND   MonDB.dbsId = db.database_id
                           )
          ORDER BY db.database_id;

          -- Cadastra na tabela temporária, os dados dos arquivos dos bancos de dados
          -- Obs.: Utilizado cursor devido a limitação do sp_msforeachdb (@command1 - 2000 caracteres)
          DECLARE curDatabase CURSOR
               LOCAL FORWARD_ONLY STATIC READ_ONLY
               FOR
                    SELECT name 
                    FROM sys.Databases
                    WHERE state_desc != 'OFFLINE'
                    ORDER BY 1 ASC;

          OPEN curDatabase;

          FETCH NEXT 
          FROM curDatabase
          INTO @vc_DatabaseName

          WHILE (@@FETCH_STATUS = 0)
          BEGIN     

               -- Altera o contexto do banco de dados para consulta
               SET @vc_ExecSQLCmd = REPLACE(@vc_SQLCmd, '{databasename}', QUOTENAME(@vc_DatabaseName));

               INSERT INTO #dbFileSpecs 
                         (
                           [dbsId]
                         , [GatherDate]   
                         , [DatabaseName]          
                         , [LogicalName]                   
                         , [FileType]              
                         , [FileSizeGB]            
                         , [FreeSpaceInFileGB]     
                         , [VolumeLabel]            
                         , [VolumeTotalSizeGB]     
                         , [VolumeAvailableSizeGB] 
                         , [FileStateDesc]         
                         , [FileGrowth]            
                         , [MaxFileSizeGB]         
                         , [IsReadOnly]            
                         )
               EXEC ( @vc_ExecSQLCmd )
                 
               IF ( ERROR_NUMBER() != 0 )
               BEGIN
                    SET @vc_ErrorMsg = ERROR_MESSAGE();                   
                    RAISERROR('Falha ao registrar dados em cursor curDatabases [%s].', 16, 1, @vc_ErrorMsg);
               END 
                   
               FETCH NEXT 
               FROM curDatabase
               INTO @vc_DatabaseName

          END

          CLOSE curDatabase;
          DEALLOCATE curDatabase;

          select * from #dbFileSpecs

          -- Se os dados para o dia não foram incluídos, insere. Caso contrário, atualiza. 
          -- Somente uma atualização diária
          MERGE dbo.DatabaseFiles AS target
          USING ( SELECT [srvId] = @vn_srvId
                       , [dbsId]
                       , [GatherDate]   
                       , [DatabaseName]          
                       , [LogicalName]                   
                       , [FileType]              
                       , [FileSizeGB]            
                       , [FreeSpaceInFileGB]     
                       , [VolumeLabel]            
                       , [VolumeTotalSizeGB]     
                       , [VolumeAvailableSizeGB] 
                       , [FileStateDesc]         
                       , [FileGrowth]            
                       , [MaxFileSizeGB]         
                       , [IsReadOnly] 
                  FROM #dbFileSpecs
                ) AS source
          ON ( target.srvId        = source.srvId
          AND  target.dbsId        = source.dbsId
          AND  target.GatherDate   = source.GatherDate
          AND  target.DatabaseName = source.DatabaseName
          AND  target.LogicalName  = source.LogicalName
             )
          WHEN MATCHED
          THEN UPDATE 
                  SET [FileType]              = source.[FileType]
                    , [FileSizeGB]            = source.[FileSizeGB] 
                    , [FreeSpaceInFileGB]     = source.[FreeSpaceInFileGB]
                    , [VolumeLabel]           = source.[VolumeLabel] 
                    , [VolumeTotalSizeGB]     = source.[VolumeTotalSizeGB] 
                    , [VolumeAvailableSizeGB] = source.[VolumeAvailableSizeGB]
                    , [FileStateDesc]         = source.[FileStateDesc] 
                    , [FileGrowth]            = source.[FileGrowth] 
                    , [MaxFileSizeGB]         = source.[MaxFileSizeGB]
                    , [IsReadOnly]            = source.[IsReadOnly]
          WHEN NOT MATCHED
          THEN INSERT ( [srvId]
                      , [dbsId]
                      , [GatherDate]   
                      , [DatabaseName]          
                      , [LogicalName]                   
                      , [FileType]              
                      , [FileSizeGB]            
                      , [FreeSpaceInFileGB]     
                      , [VolumeLabel]            
                      , [VolumeTotalSizeGB]     
                      , [VolumeAvailableSizeGB] 
                      , [FileStateDesc]         
                      , [FileGrowth]            
                      , [MaxFileSizeGB]         
                      , [IsReadOnly] 
                     ) 
              VALUES ( source.[srvId]
                     , source.[dbsId]
                     , source.[GatherDate]   
                     , source.[DatabaseName]          
                     , source.[LogicalName]                   
                     , source.[FileType]              
                     , source.[FileSizeGB]            
                     , source.[FreeSpaceInFileGB]     
                     , source.[VolumeLabel]            
                     , source.[VolumeTotalSizeGB]     
                     , source.[VolumeAvailableSizeGB] 
                     , source.[FileStateDesc]         
                     , source.[FileGrowth]            
                     , source.[MaxFileSizeGB]         
                     , source.[IsReadOnly] 
                     );

          -- Exclui os registros com mais de 6 meses
          DELETE FROM dbo.DatabaseFiles
          WHERE GatherDate <= DATEADD(MONTH, -6, SYSDATETIME());

     END TRY 
  
   BEGIN CATCH 
  
      -- Recupera informações originais do erro 
      SELECT @vc_ErrorMsg       = ERROR_MESSAGE() 
           , @vn_ErrorSeverity  = ERROR_SEVERITY() 
           , @vn_ErrorState     = ERROR_STATE() 
           , @vc_ErrorProcedure = ERROR_PROCEDURE(); 
  
      -- Tratamento Para ErrorState, retorna a procedure de execução em junção com o erro. 
      SELECT @vc_ErrorMsg = CASE WHEN @vn_ErrorState = 1 
                                 THEN @vc_ErrorMsg + CHAR(13) + 'O erro ocorreu em ' + @vc_ErrorProcedure + ' ( ' + LTRIM( RTRIM( STR( ERROR_LINE() ) ) ) + ' )'
                                 WHEN @vn_ErrorState = 3 
                                 THEN @vc_ErrorProcedure + ' - ' + @vc_ErrorMsg 
                                 ELSE @vc_ErrorMsg 
                            END; 
  
      RAISERROR ( @vc_ErrorMsg 
                , @vn_ErrorSeverity 
                , @vn_ErrorState ); 
  
        IF @vn_TranCount  = 0 AND  -- Transação feita no escopo da procedure 
            XACT_STATE() != 0      -- Transação ativa existente 
        BEGIN
            ROLLBACK TRANSACTION; 
        END
  
   END CATCH 
     
   IF @vn_TranCount  = 0 AND -- Transação feita no escopo da procedure 
      XACT_STATE()  = 1     -- Transação com sucesso de execução 
   BEGIN
      COMMIT TRANSACTION; 
   END
  
END
GO
