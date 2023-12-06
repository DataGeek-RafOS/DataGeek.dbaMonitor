SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[spu_ClearSingleUsePlanCache]
( 
  @PercentualUtilizado  TINYINT
, @TotalGBParaSingleUse DECIMAL(9, 1)
) 
AS 
BEGIN

   /***********************************************************************************************   
   **   
   **  Name         : spu_ClearSingleUsePlanCache
   **
   **  Database     : dbaMonitor
   **   
   **  Descrição....: Verifica a situação do Plan Cache por quantidade de Single Uses Plan e 
   **                 limpa o cache se necessário.
   **   
   **  Return values: N/A   
   **    
   **  Chamada por..: Job
   **   
   **  Parâmetros:   
   **  Entradas           Descrição   
   **  ------------------ -------------------------------------------------------------------------   
   **   
   **   
   **  Saídas             Descrição   
   **  ------------------ -------------------------------------------------------------------------   
   **   
   **   Observação..: Todos os contadores de memória estão em GB
   **   
   **  Autor........: Rafael Rodrigues
   **  Data.........: 10/04/2016
   **
   ************************************************************************************************   
   **  Histórico de Alterações   
   ************************************************************************************************   
   **  Data:    Autor:             Descrição:                                                Versão   
   **  -------- ------------------ --------------------------------------------------------- ------   
   **   
   ************************************************************************************************/      

     SET NOCOUNT ON 
   
     DECLARE @vn_Error          INT   
           , @vn_RowCount       INT   
           , @vn_TranCount      INT   
           , @vn_ErrorState     INT   
           , @vn_ErrorSeverity  INT   
           , @vc_ErrorProcedure VARCHAR(256)   
           , @vc_InfoMsg        VARCHAR(MAX)
           , @vc_ErrorMsg       VARCHAR(MAX);

     DECLARE @vMemoriaConfigurada      DECIMAL(9, 2)
           , @vMemoriaFisica           DECIMAL(9, 2)
           , @vMemoriaEmUso            DECIMAL(9, 2)
           , @vQtdeSingleUsePlanGB     DECIMAL(9, 2)
           , @vQtdeSingleUsePlan       INT
           , @PercentualSingleUsePlans DECIMAL(5,2);

     IF (SELECT OBJECT_ID('tempdb..#ServerConfig')) IS NOT NULL
     BEGIN 
          DROP TABLE #ServerConfig;
     END

     CREATE TABLE #ServerConfig
     (
       name         NVARCHAR(35)
     , minimum      INT 
     , maximum      INT
     , config_value INT 
     , run_value    INT 
     );

     -- Controle de Transações 
     SET @vn_TranCount = @@TRANCOUNT;
   
     BEGIN TRY;
   
          /* Retorna informações de memória (física e configurada) para o servidor */

          -- Memória configurada
          INSERT INTO #ServerConfig
                  ( name, minimum, maximum, config_value, run_value )
          EXECUTE ('sp_configure ''max server memory''');

          SELECT @vMemoriaConfigurada = run_value / 1024.0 
          FROM #ServerConfig
          WHERE name = 'max server memory (MB)';

          -- Memória Física
          SELECT @vMemoriaFisica = total_physical_memory_kb / 1024.0 / 1024.0
          FROM sys.dm_os_sys_memory;

          -- Memória em uso
          SELECT @vMemoriaEmUso = physical_memory_in_use_kb / 1024.0 / 1024.0
          FROM sys.dm_os_process_memory;

          -- Utilização no Plan Cache
          SELECT @vQtdeSingleUsePlanGB = SUM(CONVERT(BIGINT, size_in_bytes)) / 1024.0 / 1024.0 / 1024.0
               , @vQtdeSingleUsePlan   = COUNT(1)
          FROM sys.dm_exec_cached_plans
          WHERE objtype IN ('AdHoc', 'Prepared')
          AND   usecounts = 1

          -- Verifica o percentual de memória utilizada para "single-use plans"
          SET @PercentualSingleUsePlans = CONVERT(DECIMAL(5,2), (@vQtdeSingleUsePlanGB / @vMemoriaConfigurada) * 100);

          -- Se o tamanho alocado para planos de single use for maior que 20%... Libera
          IF ( @PercentualSingleUsePlans >= @PercentualUtilizado )
          OR (@vQtdeSingleUsePlanGB >= @TotalGBParaSingleUse)
          BEGIN 

               -- Limpa o cache store SQL Plans para liberar os planos de execução única
               DBCC FREESYSTEMCACHE('SQL Plans');

               SET @vc_InfoMsg = RTRIM(LTRIM(CONVERT(VARCHAR(9), @vQtdeSingleUsePlanGB))) + ' GB (' + CONVERT(VARCHAR(6), @PercentualSingleUsePlans) + '% of memory) was allocated to "single-use plans". "Single-use plans" were cleared from cache in spu_ClearSingleUsePlanCache [Config values: ' + RTRIM(LTRIM(CONVERT(VARCHAR(9), @TotalGBParaSingleUse))) + ' GB or ' + CONVERT(VARCHAR(6), @PercentualUtilizado) + '% of memory)].';
               RAISERROR('%s', 10, 1, @vc_InfoMsg);

          END
          ELSE
          BEGIN

               SET @vc_InfoMsg =  'Only ' + RTRIM(LTRIM(CONVERT(VARCHAR(9), @vQtdeSingleUsePlanGB))) + ' GB (' + CONVERT(VARCHAR(6), @PercentualSingleUsePlans) + '% of memory) was allocated to "single-use plans". There was no need to clear at this time in spu_ClearSingleUsePlanCache [Config values: ' + RTRIM(LTRIM(CONVERT(VARCHAR(9), @TotalGBParaSingleUse))) + ' GB or ' + CONVERT(VARCHAR(6), @PercentualUtilizado) + '% of memory)].';
               RAISERROR('%s', 10, 1, @vc_InfoMsg);
          
          END 

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
