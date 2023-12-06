SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

-- Alerta: PROCESSO BLOQUEADO

CREATE PROCEDURE [alert].[spu_BlockedProcessWarning]
AS
BEGIN
	SET NOCOUNT ON
     SET CONCAT_NULL_YIELDS_NULL OFF;

	-- Processo Bloqueado
	DECLARE @ParameterId INT = (SELECT prmId FROM dbaMonitor.alert.Parameter (NOLOCK) WHERE prmName = 'Processos bloqueados')
	
	-- Declara as variaveis
	DECLARE @Subject                  VARCHAR(500)
           , @AlertId                  INT
           , @AlertType                CHAR(1)
           , @Importance               VARCHAR(6)
           , @MailProfile              VARCHAR(250)
           , @HtmlScript               VARCHAR(MAX)
           , @HtmlTableStyle           VARCHAR(MAX)
           , @HtmlThStyle              VARCHAR(MAX)
           , @HtmlTdStyle              VARCHAR(MAX)
           , @CurrentTime              DATETIME
           , @EmailBody                VARCHAR(MAX)
           , @BlockedProcessValue      INT
           , @MinutesSinceRootBlocking INT
           , @EmailDestination         VARCHAR(200)

	-- Recupera os parametros do Alerta
	SELECT @BlockedProcessValue = prmValue
		, @EmailDestination = prmOperator
	FROM dbaMonitor.alert.Parameter
	WHERE prmId = @ParameterId;

	-- Query que esta gerando o lock (rodando a mais de 1 minuto)
	SELECT @MinutesSinceRootBlocking = 1	
     
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
      
	-- Seta a hora atual
	SELECT @CurrentTime = GETDATE();

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

	-- Verifica o último Tipo do Alerta registrado | S: Solucionado / A: Alerta
	SELECT @AlertId   = alcId
          , @AlertType = alcType
	FROM dbaMonitor.alert.ControlMessage
	WHERE alcId = (SELECT MAX(alcId) FROM dbaMonitor.alert.ControlMessage WHERE prmId = @ParameterId AND alcResolutionDate IS NULL )

	--	Verifica se existe algum Processo Bloqueado
	IF EXISTS	(
			SELECT NULL 
			FROM #WhoIsActiveOutput A
			     INNER JOIN 
                    #WhoIsActiveOutput B 
                         ON A.[blocking_session_id] = B.[session_id]
			WHERE DATEDIFF(SECOND,A.[start_time], @CurrentTime) > @BlockedProcessValue * 60		-- A query que está sendo bloqueada está rodando a mais 2 minutos
			AND   DATEDIFF(SECOND,B.[start_time], @CurrentTime) > @MinutesSinceRootBlocking * 60			-- A query que está bloqueando está rodando a mais de 1 minuto
			)
	BEGIN	
		IF ISNULL(@AlertType, 'R') = 'R'	-- Se não houver pendências
		BEGIN

			-- Declara a variavel e retorna a quantidade de processos bloqueados
			DECLARE @QtyBlockedProcesses INT = (
										   SELECT COUNT(*)
										   FROM #WhoIsActiveOutput A
										        INNER JOIN 
                                                          #WhoIsActiveOutput B 
                                                                ON A.[blocking_session_id] = B.[session_id]
										   WHERE DATEDIFF(SECOND,A.[start_time], @CurrentTime) > @BlockedProcessValue	* 60
										   AND   DATEDIFF(SECOND,B.[start_time], @CurrentTime) > @MinutesSinceRootBlocking * 60
									);

			DECLARE @QtyBlockedProcessesLocks INT = (
                                                          SELECT COUNT(*)
                                                          FROM #WhoIsActiveOutput A
                                                          WHERE [blocking_session_id] IS NOT NULL
                                                          );

			--------------------------------------------------------------------------------------------------------------------------------
			--	Verifica o Nivel dos Locks
			--------------------------------------------------------------------------------------------------------------------------------
			ALTER TABLE #WhoIsActiveOutput ADD LockLevelNumber TINYINT; 

			-- Nivel 0
			UPDATE Who
			   SET LockLevelNumber = 0
			FROM #WhoIsActiveOutput Who
			WHERE blocking_session_id IS NULL 
               AND   session_id IN ( SELECT DISTINCT blocking_session_id 
						       FROM #WhoIsActiveOutput 
                                     WHERE blocking_session_id IS NOT NULL
                                   );

			UPDATE Who
			   SET LockLevelNumber = 1
			FROM #WhoIsActiveOutput Who
			WHERE LockLevelNumber IS NULL
			AND   blocking_session_id IN ( SELECT DISTINCT session_id FROM #WhoIsActiveOutput WHERE LockLevelNumber = 0)

			UPDATE Who
			   SET LockLevelNumber = 2
			FROM #WhoIsActiveOutput Who
			WHERE LockLevelNumber IS NULL
			AND   blocking_session_id IN ( SELECT DISTINCT session_id FROM #WhoIsActiveOutput WHERE LockLevelNumber = 1)

			UPDATE Who
			SET LockLevelNumber = 3
			FROM #WhoIsActiveOutput Who
			WHERE LockLevelNumber IS NULL
			AND   blocking_session_id IN ( SELECT DISTINCT session_id FROM #WhoIsActiveOutput WHERE LockLevelNumber = 2)

			-- Tratamento quando não tem um Lock Raiz
			IF NOT EXISTS( SELECT 1 
                              FROM #WhoIsActiveOutput 
                              WHERE LockLevelNumber IS NOT NULL
                            )
			BEGIN
				UPDATE Who
				SET LockLevelNumber = 0
				FROM #WhoIsActiveOutput Who
				WHERE session_id IN ( SELECT 
                                          DISTINCT blocking_session_id 
				                      FROM #WhoIsActiveOutput 
                                          WHERE blocking_session_id IS NOT NULL
                                        );
          
				UPDATE Who
				SET LockLevelNumber = 1
				FROM #WhoIsActiveOutput Who
				WHERE LockLevelNumber IS NULL
				AND   blocking_session_id IN ( SELECT DISTINCT session_id FROM #WhoIsActiveOutput WHERE LockLevelNumber = 0 );

				UPDATE Who
				SET LockLevelNumber = 2
				FROM #WhoIsActiveOutput Who
				WHERE LockLevelNumber IS NULL
				AND  blocking_session_id IN ( SELECT DISTINCT session_id FROM #WhoIsActiveOutput WHERE LockLevelNumber = 1 );

				UPDATE Who
				SET LockLevelNumber = 3
				FROM #WhoIsActiveOutput Who
				WHERE LockLevelNumber IS NULL
				AND  blocking_session_id IN ( SELECT DISTINCT session_id FROM #WhoIsActiveOutput WHERE LockLevelNumber = 2 );
			END

               -- Criação do Html do E-mail						
               SELECT @HtmlScript = + '<font face="Verdana" size="4">Processos em situação de bloqueio</font>' +
                                         '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                         <tr align="center">
                                               <th ' + @HtmlThStyle + 'width="15%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Nível</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Duração</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Banco de dados</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Login</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Host</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Hora de início</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="40%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Status</font></b></th>       
                                               <th ' + @HtmlThStyle + 'width="40%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Sessão</font></b></th> 
                                               <th ' + @HtmlThStyle + 'width="40%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Origem</font></b></th>                                                      
                                               <th ' + @HtmlThStyle + 'width="40%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">transações abertas</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="40%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">CPU</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="40%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Leituras</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="40%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Escritas</font></b></th>
                                               <th ' + @HtmlThStyle + 'width="40%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Comando</font></b></th>       
                                         </tr>'

               SELECT @HtmlScript = @HtmlScript +
                     '<tr>
                      <td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([LockLevelNumber], 0) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([Duration], '') +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([database_name], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([login_name], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([host_name], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([start_time], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([status], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([session_id], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([blocking_session_id], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([open_tran_count], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([CPU], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([reads], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([writes], '' ) +'</font></td>' +  
                     '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + ISNULL([sql_command], '' ) +'</font></td>'  
			FROM (  
				SELECT TOP 30
                           CAST(LockLevelNumber AS VARCHAR)					AS [LockLevelNumber]
                         , ISNULL([dd hh:mm:ss.mss], '-')					AS [Duration]
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
				WHERE LockLevelNumber IS NOT NULL
				ORDER BY [LockLevelNumber], [start_time] 
				) AS x 
               ORDER BY [LockLevelNumber], [start_time] 
			      
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

               SET @Subject = 'Alerta: ' + CAST(@QtyBlockedProcesses AS VARCHAR) + 
						' processo(s) bloqueado(s) a mais de ' +  CAST((@BlockedProcessValue) AS VARCHAR) + ' minuto(s)' +
						' e um total de ' + CAST(@QtyBlockedProcessesLocks AS VARCHAR) +  ' Lock(s) no Servidor: ' + @@SERVERNAME;

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
               INSERT INTO dbaMonitor.dbo.AlertControl
                         ( prmId
                         , alcMessage
                         , alcType
                         , alcCreationDate
                         )
               SELECT @ParameterId, @Subject, 'A', SYSDATETIME();
			
		END
	END	-- Fim. Criação do alerta
	ELSE 
	BEGIN -- Inicio. Solução do alerta
		IF @AlertType = 'A'
		BEGIN
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
			              			

			SET @Importance =	'High';

			SET  @Subject = 'Resolvido: Não existem mais processos bloqueados no Servidor: ' + @@SERVERNAME

			SET 	@EmailBody = @HtmlScript;
				
               -- Envio do e-mail de solução			
			EXEC msdb.dbo.sp_send_dbmail
                           @profile_name = @MailProfile
                         , @recipients   = @EmailDestination
                         , @subject      = @Subject
                         , @body         = @EmailBody
                         , @body_format  = 'HTML'
                         , @importance   = @Importance;

			-- Atualiza o Registro na Tabela de Controle dos Alertas informando que o alerta foi solucionado
               UPDATE dbaMonitor.dbo.AlertControl
                  SET alcType = 'R'
                    , alcResolutionDate = SYSDATETIME()
               WHERE alcId = @AlertId;
				
		END		
	END	-- Fim. Solução
END
GO
