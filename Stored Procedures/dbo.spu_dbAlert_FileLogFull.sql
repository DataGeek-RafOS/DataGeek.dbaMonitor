SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO


CREATE PROCEDURE [dbo].[spu_dbAlert_FileLogFull]
AS
BEGIN
	SET NOCOUNT ON

	-- Arquivo de Log Full
	DECLARE @ParameterId INT = (SELECT prmId FROM dbaMonitor.dbo.Parameter (NOLOCK) WHERE prmName = 'Arquivo com log cheio');
	
	-- Declara as variaveis
     DECLARE @LowestSizeForAlertLog    INT
           , @MailProfile              VARCHAR(250)
           , @HtmlScript               VARCHAR(MAX)
           , @HtmlTableStyle           VARCHAR(MAX)
           , @HtmlThStyle              VARCHAR(MAX)
           , @HtmlTdStyle              VARCHAR(MAX)
           , @EmailDestination         VARCHAR(200)
           , @AlertType                CHAR(1)
           , @AlertId                  INT
           , @AlertCreationDate        DATETIME2(0)
           , @Importance               VARCHAR(6)
           , @EmailBody                VARCHAR(MAX)
           , @Subject                  VARCHAR(500)
           , @LogFullThreshold         TINYINT;
	
	-- Recupera os parametros do Alerta
	SELECT @LogFullThreshold = prmValue
		, @EmailDestination = prmOperator
	FROM dbaMonitor.dbo.Parameter
	WHERE prmId = @ParameterId;

	-- Seta as variaveis
	SET @LowestSizeForAlertLog = 100000;		-- 100 MB

     -- Recupera o profile de e-mail caso o mesmo não tenha sido informado 
     IF @MailProfile IS NULL
     BEGIN
        SELECT TOP 1 @MailProfile = name
        FROM msdb.dbo.sysmail_profile WITH (NOLOCK);
     END;      	

     -- Estrutura do CSS - Formatação HTML
     SELECT @HtmlTableStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'tableHTML';
     
     SELECT @HtmlThStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'thHTML';
     
     SELECT @HtmlTdStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'tdHTML';

	-- Verifica o último Tipo do Alerta registrado | S: Solucionado / A: Alerta
	SELECT @AlertId           = alcId
          , @AlertType         = alcType
          , @AlertCreationDate = alcCreationDate
	FROM dbaMonitor.dbo.AlertControl 
	WHERE alcId = (SELECT MAX(alcId) FROM dbaMonitor.dbo.AlertControl WHERE prmId = @ParameterId)

     -- Se existe alerta aberto na última hora, aborta (envio de hora em hora)
     IF @AlertType = 'A' AND DATEDIFF(MINUTE, @AlertCreationDate, CURRENT_TIMESTAMP) <= 60
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
	-- Verifica se existe algum LOG com muita utilização
	*******************************************************************************************************************************/
	IF EXISTS (
               SELECT db.[name] AS [Database Name]
                    , db.[recovery_model_desc] AS [Recovery Model]
                    , db.[log_reuse_wait_desc] AS [Log Reuse Wait DESCription]
                    , ls.[cntr_value] AS [Log Size (KB)]
                    , lu.[cntr_value] AS [Log Used (KB)]
                    , CAST(CAST(lu.[cntr_value] AS FLOAT) / CASE
                                                                 WHEN CAST(ls.[cntr_value] AS FLOAT) = 0
                                                                 THEN 1
                                                                 ELSE
                                                                 CAST(ls.[cntr_value] AS FLOAT)
                                                            END AS DECIMAL(18, 2)) * 100 AS [Percente_Log_Used]
                    , db.[compatibility_level] AS [DB Compatibility Level]
                    , db.[page_verify_option_desc] AS [Page Verify Option]
               FROM [sys].[databases] AS db
                    INNER JOIN
                    [sys].[dm_os_performance_counters] AS lu
                         ON db.[name] = lu.[instance_name]
                    INNER JOIN
                    [sys].[dm_os_performance_counters] AS ls
                         ON db.[name] = ls.[instance_name]
               WHERE lu.[counter_name] LIKE 'Log File(s) Used Size (KB)%'
               AND   ls.[counter_name] LIKE 'Log File(s) Size (KB)%'
               AND   ls.[cntr_value] > @LowestSizeForAlertLog -- Maior que 100 MB
               AND   ( CAST(CAST(lu.[cntr_value] AS FLOAT) / CASE WHEN CAST(ls.[cntr_value] AS FLOAT) = 0
                                                                  THEN 1
                                                                  ELSE CAST(ls.[cntr_value] AS FLOAT)
                                                             END AS DECIMAL(18, 2)) * 100
                     ) > @LogFullThreshold
			 )
	BEGIN	
     
		IF ISNULL(@AlertType, 'R') = 'R'		
		BEGIN

	          -- Recupera todos os comandos que estão sendo executados
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


			-- Criação do Html do E-mail						
               SELECT @HtmlScript = + '<font face="Verdana" size="4">Arquivo(s) de transaction log quase cheio(s)</font>' +
                                         '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                         <tr align="center">
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Banco de dados</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Tamanho do Log (MB)</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Percentual Log Utilizado (%)</font></b></th>
                                         </tr>'

               SELECT @HtmlScript = @HtmlScript +
                     '<tr>
                      <td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([DatabaseName], '') +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + CAST([cntr_value] AS VARCHAR)	 +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + CAST([Percente_Log_Used] AS VARCHAR)  +'</font></td>'  
			FROM (  
				-- Dados da Tabela do email
				SELECT [DatabaseName] = db.[name]
					, [cntr_value] = CAST(ls.[cntr_value] / 1024.00 AS DECIMAL(18,2))
					, [Percente_Log_Used] = CAST(	CAST(lu.[cntr_value] AS FLOAT) / CASE WHEN CAST(ls.[cntr_value] AS FLOAT) = 0 
												                                   THEN 1 
												                                   ELSE CAST(ls.[cntr_value] AS FLOAT) 
										                                        END AS DECIMAL(18,2)) * 100					 
				FROM [sys].[databases] AS db
					INNER JOIN 
                         [sys].[dm_os_performance_counters] AS lu 
                              ON db.[name] = lu.[instance_name]
					INNER JOIN [sys].[dm_os_performance_counters] AS ls  
                              ON db.[name] = ls.[instance_name]
				WHERE lu.[counter_name] LIKE 'Log File(s) Used Size (KB)%'
				AND  ls.[counter_name] LIKE 'Log File(s) Size (KB)%' 
				AND  ls.[cntr_value] > @LowestSizeForAlertLog 
				AND (CAST(CAST(lu.[cntr_value] AS FLOAT) / CASE WHEN CAST(ls.[cntr_value] AS FLOAT) = 0 
														THEN 1 
														ELSE CAST(ls.[cntr_value] AS FLOAT) 
												     END AS DECIMAL(18,2)) * 100) > @LogFullThreshold

				) AS x 
			ORDER BY [Percente_Log_Used] DESC;  
			
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
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Hora de início</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Status</font></b></th>       
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Sessão</font></b></th> 
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Origem</font></b></th>                                                      
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Waits</font></b></th>                                                 
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">transaçães abertas</font></b></th>
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

			-- Variáveis para envio do e-mail
			SET @Importance =	'High';

               SET @Subject = 'Alerta: Arquivo(s) de Transaction Log com mais de ' +  CAST((@LogFullThreshold) AS VARCHAR) + '% de utilização no Servidor: ' + @@SERVERNAME;

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
	          -- Recupera todos os comandos que estão sendo executados
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


			-- Criação do Html do E-mail						
               SELECT @HtmlScript = + '<font face="Verdana" size="4">Arquivo(s) de transaction log quase cheio(s)</font>' +
                                         '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                         <tr align="center">
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Banco de dados</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Tamanho do Log (MB)</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Percentual Log Utilizado (%)</font></b></th>
                                         </tr>'

               SELECT @HtmlScript = @HtmlScript +
                     '<tr>
                      <td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([DatabaseName], '') +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + CAST([cntr_value] AS VARCHAR)	 +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + CAST([Percente_Log_Used] AS VARCHAR)  +'</font></td>'  
			FROM (  
				-- Dados da Tabela do EMAIL
				SELECT [DatabaseName] = db.[name]
					, [cntr_value] = CAST(ls.[cntr_value] / 1024.00 AS DECIMAL(18,2))
					, [Percente_Log_Used] = CAST(	CAST(lu.[cntr_value] AS FLOAT) / CASE WHEN CAST(ls.[cntr_value] AS FLOAT) = 0 
												                                   THEN 1 
												                                   ELSE CAST(ls.[cntr_value] AS FLOAT) 
										                                        END AS DECIMAL(18,2)) * 100					 
				FROM [sys].[databases] AS db
					INNER JOIN 
                         [sys].[dm_os_performance_counters] AS lu 
                              ON db.[name] = lu.[instance_name]
					INNER JOIN [sys].[dm_os_performance_counters] AS ls  
                              ON db.[name] = ls.[instance_name]
				WHERE lu.[counter_name] LIKE 'Log File(s) Used Size (KB)%'
				AND  ls.[counter_name] LIKE 'Log File(s) Size (KB)%' 
				AND  ls.[cntr_value] > @LowestSizeForAlertLog 
				AND (CAST(CAST(lu.[cntr_value] AS FLOAT) / CASE WHEN CAST(ls.[cntr_value] AS FLOAT) = 0 
														THEN 1 
														ELSE CAST(ls.[cntr_value] AS FLOAT) 
												     END AS DECIMAL(18,2)) * 100) > @LogFullThreshold

				) AS x 
			ORDER BY [Percente_Log_Used] DESC;  
			
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
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Hora de início</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Status</font></b></th>       
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Sessão</font></b></th> 
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Origem</font></b></th>                                                      
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Waits</font></b></th>                                                 
                                               <th ' + @HtmlThStyle + 'width="7%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">transações abertas</font></b></th>
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

			-- Variáveis para envio do e-mail
			SET @Importance =	'High';

               SET @Subject = 'Resolvido: Arquivo(s) de Transaction Log com mais de ' +  CAST((@LogFullThreshold) AS VARCHAR) + '% de utilização no Servidor: ' + @@SERVERNAME;

			SET @EmailBody = @HtmlScript;

			-- Dispara o e-mail com as informações coletadas
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
