SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[spu_PurgeBaselineData]
AS 
BEGIN

   /***********************************************************************************************   
   **   
   **  Name         : spu_PurgeBaselineData
   **
   **  Database     : dbaMonitor
   **   
   **  Descrição....: Realiza a exclusão de dados das tabelas de baseline do banco de dados  de
   **                 monitoramento.
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
   **  Data.........: 11/01/2017
   **
   ************************************************************************************************   
   **  Histórico de Alterações   
   ************************************************************************************************   
   **  Data:    Autor:             Descrição:                                                Versão   
   **  -------- ------------------ --------------------------------------------------------- ------   
   **   
   ************************************************************************************************   
   **                                © RafaelOLSR Development  
   ************************************************************************************************/      

     SET NOCOUNT ON 
   
     DECLARE @vn_Error          INT   
           , @vn_RowCount       INT   
           , @vn_TranCount      INT   
           , @vn_ErrorState     INT   
           , @vn_ErrorSeverity  INT   
           , @vc_ErrorProcedure VARCHAR(256)   
           , @vc_ErrorMsg       VARCHAR(MAX);

     DECLARE @vn_DaysToPurge_WaitStats   INT = 90;
     DECLARE @vn_DaysToPurge_SystemStats INT = 150;
     DECLARE @vn_DaysToPurge_SQLStats    INT = 30;

     -- Controle de Transações 
     SET @vn_TranCount = @@TRANCOUNT;
   
     BEGIN TRY;
   
          IF ( @vn_TranCount = 0 )
               BEGIN TRANSACTION;

          -- Purge Wait Statistics
          /*
          DELETE FROM baseline.WaitStatistics
          WHERE CollectionDate < DATEADD(DAY, (-1 * @vn_DaysToPurge_WaitStats), CURRENT_TIMESTAMP);
          */
          -- Purge System Statistics
          DELETE FROM baseline.SystemStatistics
          WHERE CollectionDate < DATEADD(DAY, (-1 * @vn_DaysToPurge_SystemStats), CURRENT_TIMESTAMP);
          /*
          -- Purge SQL Statistics
          DELETE FROM baseline.SQLStatistics
          WHERE collection_time < DATEADD(DAY, (-1 * @vn_DaysToPurge_SQLStats), CURRENT_TIMESTAMP);
          */
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
