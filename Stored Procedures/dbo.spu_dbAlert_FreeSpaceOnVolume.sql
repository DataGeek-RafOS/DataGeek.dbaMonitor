SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

/*


DECLARE @Email VARCHAR(200) = 'rafael.rodrigues@oab.org.br'

INSERT INTO dbaMonitor.dbo.Parameter (
                      prmName
                    , prmProcedure
                    , prmSolvedAlert
                    , prmValue
                    , prmMetric
                    , prmFileLocation
                    , prmOperator
                      )
VALUES ( 'Espaço livre no disco'   -- prmName - varchar(100)
       , 'spu_dbAlert_FreeSpaceOnVolume'   -- prmProcedure - varchar(100)
       , 1 -- prmSolvedAlert - bit
       , 10    -- prmValue - int
       , 'Percentual'   -- prmMetric - varchar(50)
       , NULL   -- prmFileLocation - varchar(1000)
       , @Email   -- prmOperator - varchar(200)
     )

*/


CREATE PROCEDURE [dbo].[spu_dbAlert_FreeSpaceOnVolume]
AS
BEGIN
	SET NOCOUNT ON

	-- Arquivo de Log Full
	DECLARE @ParameterId INT = (SELECT prmId FROM dbaMonitor.dbo.Parameter (NOLOCK) WHERE prmProcedure = 'spu_dbAlert_FreeSpaceOnVolume');

	-- Declara as variaveis
     DECLARE @MailProfile              VARCHAR(250)
           , @HtmlScript               VARCHAR(MAX)
           , @HtmlTableStyle           VARCHAR(MAX)
           , @HtmlThStyle              VARCHAR(MAX)
           , @HtmlTdStyle              VARCHAR(MAX)
           , @EmailDestination         VARCHAR(200)
           , @AlertType                CHAR(1)
           , @AlertId                  INT
           , @Importance               VARCHAR(6)
           , @EmailBody                VARCHAR(MAX)
           , @Subject                  VARCHAR(500)
           , @FreeSpaceThreshold       TINYINT
           , @LastAlert                DATETIME2(0)
           , @ErrorMessage             NVARCHAR(MAX);
	
	-- Recupera os parametros do Alerta
	SELECT @FreeSpaceThreshold = prmValue
		, @EmailDestination   = prmOperator
	FROM dbaMonitor.dbo.Parameter
	WHERE prmId = @ParameterId;

     -- Recupera o profile de e-mail caso o mesmo n�o tenha sido informado 
     IF @MailProfile IS NULL
     BEGIN
        SELECT TOP 1 @MailProfile = name
        FROM msdb.dbo.sysmail_profile WITH (NOLOCK);
     END;      	

     -- Estrutura do CSS - Formatação HTML
     SELECT @HtmlTableStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'tableHTML';
     
     SELECT @HtmlThStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'thHTML';
     
     SELECT @HtmlTdStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'tdHTML';

	-- Verifica o último Tipo do Alerta registrado | R: Resolvido / A: Alerta
	SELECT @AlertId   = alcId
          , @AlertType = alcType
          , @LastAlert = alcCreationDate
	FROM dbaMonitor.dbo.AlertControl 
	WHERE alcId = (SELECT MAX(alcId) FROM dbaMonitor.dbo.AlertControl  WHERE prmId = @ParameterId)
	
     -- Se existe alerta aberto na última dia, aborta 
     IF @AlertType = 'A' AND @LastAlert <= DATEADD(HOUR, -2, CURRENT_TIMESTAMP) -- Último alerta foi a mais de 2 horas
     BEGIN
         RETURN;
     END

	-- Cria a tabela que ira armazenar os dados dos processos
	IF ( OBJECT_ID('TempDB..#WhoIsActiveOutput') IS NOT NULL )
     BEGIN
		DROP TABLE #WhoIsActiveOutput
	END;
     	
	CREATE TABLE #WhoIsActiveOutput 
     (		
	  [dd hh:mm:ss.mss]		VARCHAR(20)
	, [database_name]		NVARCHAR(128)		
	, [login_name]			NVARCHAR(128)
	, [host_name]			NVARCHAR(128)
	, [start_time]			DATETIME
	, [status]			VARCHAR(30)
	, [session_id]			INT
	, [blocking_session_id]	INT
	, [wait_info]			VARCHAR(MAX)
	, [open_tran_count]		INT
	, [CPU]				VARCHAR(MAX)
	, [reads]				VARCHAR(MAX)
	, [writes]			VARCHAR(MAX)		
	, [sql_command]		XML		
	)   

	/*******************************************************************************************************************************
	-- Verifica se existe algum disco abaixo do limite configurado
	*******************************************************************************************************************************/
	IF EXISTS (
               SELECT DISTINCT
                      [Drive]       = s.volume_mount_point
                    , [Label]       = s.logical_volume_name
                    , [Capacity]    = CONVERT(VARCHAR(10), CONVERT(NUMERIC(13,2), s.total_bytes / 1073741824.0))
                    , [FreeSpace]   = CONVERT(VARCHAR(10), CONVERT(NUMERIC(13,2), s.available_bytes / 1073741824.0))
                    , [PercentFree] = CONVERT(NUMERIC(13,1), ((s.available_bytes * 1.0 / s.total_bytes) * 100.0))
               FROM sys.master_files f
                    CROSS APPLY 
                    sys.dm_os_volume_stats(f.database_id, f.[file_id]) s
               WHERE CONVERT(NUMERIC(13,1), ((s.available_bytes * 1.0 / s.total_bytes) * 100.0)) <= @FreeSpaceThreshold
               )     
	BEGIN	
		
		IF ISNULL(@AlertType, 'R') = 'R'	
		BEGIN

	          -- Recupera todos os comandos que est�o sendo executados
	          EXEC dbaMonitor.dbo.sp_WhoIsActive
			          @get_outer_command = 1,
			          @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
			          @destination_table = '#WhoIsActiveOutput',
                         @not_filter_type = 'login',
                         @not_filter = 'CFOAB\OabSQLServer';
				    
	          -- Altera a coluna que possui o comando SQL
	          ALTER TABLE #WhoIsActiveOutput ALTER COLUMN [sql_command] VARCHAR(MAX);
	
               -- Limpeza e tratamento dos dados do comando SQL
	          UPDATE #WhoIsActiveOutput
	          SET [sql_command] = REPLACE( REPLACE( REPLACE( REPLACE( CAST([sql_command] AS VARCHAR(1000)), '<?query --', ''), '--?>', ''), '&gt;', '>'), '&lt;', '');
			
			-- Verifica se não existe nenhum processo em Execução
			IF NOT EXISTS ( SELECT TOP 1 * FROM #WhoIsActiveOutput )
			BEGIN
				INSERT INTO #WhoIsActiveOutput
				SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			END		

               BEGIN TRY

			     -- Criaçao do Html do E-mail						
                    SELECT @HtmlScript = + '<font face="Verdana" size="4">Discos com pouco espaço livre</font>' +
                                              '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                              <tr align="center">
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Disco</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Descrição</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Capaciade</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Espaço livre</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">%</font></b></th>
                                              </tr>'

                    SELECT @HtmlScript = @HtmlScript +
                          '<tr>' +
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([Drive], '') +'</font></td>' +  
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([Label], '') +'</font></td>' +  
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([Capacity], '') +'</font></td>' +  
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([FreeSpace], '') +'</font></td>' +  
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([PercentFree], '') +'</font></td>'  
			     FROM (  
				     -- Dados da Tabela do EMAIL
                         SELECT DISTINCT
                                [Drive]       = s.volume_mount_point
                              , [Label]       = s.logical_volume_name
                              , [Capacity]    = CONVERT(VARCHAR(10), CONVERT(NUMERIC(13,2), s.total_bytes / 1073741824.0))
                              , [FreeSpace]   = CONVERT(VARCHAR(10), CONVERT(NUMERIC(13,2), s.available_bytes / 1073741824.0))
                              , [PercentFree] = CONVERT(VARCHAR(10), CONVERT(NUMERIC(13,1), ((s.available_bytes * 1.0 / s.total_bytes) * 100.0)))
                         FROM sys.master_files f
                              CROSS APPLY 
                              sys.dm_os_volume_stats(f.database_id, f.[file_id]) s
                         WHERE CONVERT(NUMERIC(13,1), ((s.available_bytes * 1.0 / s.total_bytes) * 100.0)) <= @FreeSpaceThreshold
				     ) AS x 
			     ORDER BY [Drive] DESC;  

               END TRY
               BEGIN CATCH
                    SET @ErrorMessage = ERROR_MESSAGE();
                    RAISERROR('Falha ao montar mensagem de erro de falta de espaço em disco. [Erro: %s]', 16, 1 , @ErrorMessage) WITH NOWAIT
                    RETURN(-1);
               END CATCH
			
			-- Corrige a Formata��o da Tabela
			SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<' ), '&gt;', '>' ), '<td>', '<td align=center>')

               SELECT @HtmlScript = @HtmlScript + '</table><br>'
			
			-- Corrige a Formatação da Tabela
			SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<' ), '&gt;', '>' ), '<td>', '<td align=center>')

               SELECT @HtmlScript = @HtmlScript + '</table><br>'

               -- Criação do Html do E-mail	: Processos em execução no momento					
               SELECT @HtmlScript += '<font face="Verdana" size="4">Processos em execução no momento</font>' +
                                         '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                         <tr align="center">
                                               <th ' + @HtmlThStyle + 'width="20%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Duração</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Banco de dados</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Login</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Host</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Hora de in�cio</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Status</font></b></th>       
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Sess�o</font></b></th> 
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Origem</font></b></th>                                                      
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Waits</font></b></th>                                                 <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">transaçães abertas</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">CPU</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Leituras</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Escritas</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Comando</font></b></th>       
                                         </tr>'

               SELECT @HtmlScript = @HtmlScript +
                     '<tr>
                      <td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([Duration], 0) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([database_name], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([login_name], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([host_name], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([start_time], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([status], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([session_id], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([blocking_session_id], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([wait], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([open_tran_count], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([CPU], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([reads], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([writes], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([sql_command], '' ) +'</font></td>'  
               FROM (  
				SELECT TOP 50
					  ISNULL([dd hh:mm:ss.mss], '-')					AS [Duration]
					, ISNULL([database_name], '-')					AS [database_name]
					, ISNULL([login_name], '-')						AS [login_name]
					, ISNULL([host_name], '-')						AS [host_name]
					, ISNULL(CONVERT(VARCHAR(20), [start_time], 120), '-')	AS [start_time]
					, ISNULL([status], '-')							AS [status]
					, ISNULL(CAST([session_id] AS VARCHAR), '-')			AS [session_id]
					, ISNULL(CAST([blocking_session_id] AS VARCHAR), '-')	AS [blocking_session_id]
					, ISNULL([wait_info], '-')						AS [Wait]
					, ISNULL(CAST([open_tran_count] AS VARCHAR), '-')		AS [open_tran_count]
					, ISNULL([CPU], '-')							AS [CPU]
					, ISNULL([reads], '-')							AS [reads]
					, ISNULL([writes], '-')							AS [writes]
					, ISNULL(SUBSTRING([sql_command], 1, 300), '-')		AS [sql_command]
				FROM #WhoIsActiveOutput
				ORDER BY [start_time]
				) AS D ORDER BY [start_time] 

               SELECT @HtmlScript = @HtmlScript + '</table><br>'
			      
			-- Corrige a Formatação da Tabela
			SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

			-- Vari�veis para envio do e-mail
			SET @Importance =	'High';

               SET @Subject = 'Alerta: Espaço em disco(s) abaixo de ' +  CAST((@FreeSpaceThreshold) AS VARCHAR) + '% livre no Servidor: ' + @@SERVERNAME;

			SET @EmailBody = @HtmlScript;

			-- Dispara o e-mail com as informaçães coletadas
			EXEC msdb.dbo.sp_send_dbmail
                              @profile_name  = @MailProfile
                            , @recipients    = @EmailDestination
                            , @subject       = @Subject
                            , @body          = @EmailBody
                            , @body_format   = 'HTML'
                            , @importance    = @Importance;
			
			-- Insere um Registro na Tabela de Controle dos Alertas informando que o alerta foi enviado
               INSERT INTO dbaMonitor.dbo.AlertControl
                         ( prmId
                         , alcMessage
                         , alcType
                         , alcCreationDate
                         )
               SELECT @ParameterId, @Subject, 'A', SYSDATETIME();

		               		
		END
	END		
	ELSE 
	BEGIN	
		IF @AlertType = 'A'
		BEGIN
	          -- Recupera todos os comandos que est�o sendo executados
	          EXEC dbaMonitor.dbo.sp_WhoIsActive
			          @get_outer_command = 1,
			          @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
			          @destination_table = '#WhoIsActiveOutput',
                         @not_filter_type = 'login',
                         @not_filter = 'CFOAB\OabSQLServer';
				    
	          -- Altera a coluna que possui o comando SQL
	          ALTER TABLE #WhoIsActiveOutput ALTER COLUMN [sql_command] VARCHAR(MAX);
	
               -- Limpeza e tratamento dos dados do comando SQL
	          UPDATE #WhoIsActiveOutput
	          SET [sql_command] = REPLACE( REPLACE( REPLACE( REPLACE( CAST([sql_command] AS VARCHAR(1000)), '<?query --', ''), '--?>', ''), '&gt;', '>'), '&lt;', '');
			
			-- Verifica se não existe nenhum processo em Execução
			IF NOT EXISTS ( SELECT TOP 1 * FROM #WhoIsActiveOutput )
			BEGIN
				INSERT INTO #WhoIsActiveOutput
				SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
			END		

               -- Criação do Html do E-mail	: Processos em execução no momento					
               SELECT @HtmlScript += '<font face="Verdana" size="4">Processos em execução no momento</font>' +
                                         '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                         <tr align="center">
                                               <th ' + @HtmlThStyle + 'width="20%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Duração</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Banco de dados</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Login</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Host</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Hora de in�cio</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Status</font></b></th>       
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Sess�o</font></b></th> 
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Origem</font></b></th>                                                      
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Waits</font></b></th>                                                 <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">transaçães abertas</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">CPU</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Leituras</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Escritas</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Comando</font></b></th>       
                                         </tr>'

               SELECT @HtmlScript = @HtmlScript +
                     '<tr>
                      <td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([Duration], 0) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([database_name], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([login_name], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([host_name], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([start_time], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([status], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([session_id], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([blocking_session_id], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([wait], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([open_tran_count], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([CPU], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([reads], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([writes], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([sql_command], '' ) +'</font></td>'  
               FROM (  
				SELECT TOP 50
					  ISNULL([dd hh:mm:ss.mss], '-')					AS [Duration]
					, ISNULL([database_name], '-')					AS [database_name]
					, ISNULL([login_name], '-')						AS [login_name]
					, ISNULL([host_name], '-')						AS [host_name]
					, ISNULL(CONVERT(VARCHAR(20), [start_time], 120), '-')	AS [start_time]
					, ISNULL([status], '-')							AS [status]
					, ISNULL(CAST([session_id] AS VARCHAR), '-')			AS [session_id]
					, ISNULL(CAST([blocking_session_id] AS VARCHAR), '-')	AS [blocking_session_id]
					, ISNULL([wait_info], '-')						AS [Wait]
					, ISNULL(CAST([open_tran_count] AS VARCHAR), '-')		AS [open_tran_count]
					, ISNULL([CPU], '-')							AS [CPU]
					, ISNULL([reads], '-')							AS [reads]
					, ISNULL([writes], '-')							AS [writes]
					, ISNULL(SUBSTRING([sql_command], 1, 300), '-')		AS [sql_command]
				FROM #WhoIsActiveOutput
				ORDER BY [start_time]
				) AS D ORDER BY [start_time] 

               SELECT @HtmlScript = @HtmlScript + '</table><br>'
			      
			-- Corrige a Formatação da Tabela
			SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

			-- Vari�veis para envio do e-mail
			SET @Importance =	'High';

               SET @Subject = 'Resolvido: Disco(s) com menos de ' +  CAST((@FreeSpaceThreshold) AS VARCHAR) + '% de espaço livre no Servidor: ' + @@SERVERNAME;

			SET @EmailBody = @HtmlScript;

			-- Dispara o e-mail com as informaçães coletadas
			EXEC msdb.dbo.sp_send_dbmail
                              @profile_name  = @MailProfile
                            , @recipients    = @EmailDestination
                            , @subject       = @Subject
                            , @body          = @EmailBody
                            , @body_format   = 'HTML'
                            , @importance    = @Importance;
						

			-- Insere um Registro na Tabela de Controle dos Alertas informando que o alerta foi enviado
               UPDATE dbaMonitor.dbo.AlertControl
                  SET alcType = 'R'
                    , alcResolutionDate = SYSDATETIME()
               WHERE alcId = @AlertId;
						
		END
	END		-- FIM - CLEAR
END
GO
