SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

/*******************************************************************************************************************************
--	Alerta: Consumo de CPU
*******************************************************************************************************************************/

CREATE PROCEDURE [dbo].[spu_dbAlert_HighCPU]
AS
BEGIN
	SET NOCOUNT ON

	-- Consumo CPU
	DECLARE @ParameterId INT = (
                                SELECT prmId FROM 
                                dbaMonitor.dbo.Parameter (NOLOCK) 
                                WHERE prmName = 'Consumo de CPU'
                                );

     -- Declara as variaveis
     DECLARE @MailProfile            VARCHAR(250)
           , @HtmlScript             VARCHAR(MAX)
           , @HtmlTableStyle         VARCHAR(MAX)
           , @HtmlThStyle            VARCHAR(MAX)
           , @HtmlTdStyle            VARCHAR(MAX)
           , @EmailDestination       VARCHAR(200)
           , @AlertType              CHAR(1)
           , @AlertId                INT
           , @Importance             VARCHAR(6)
           , @EmailBody              VARCHAR(MAX)
           , @Subject                VARCHAR(500)
           , @ThresholdCPU           INT 
           , @ProcessorUsage         INT
           , @SQLServerCpuUsage      INT
           , @OtherProcessesCpuUsage INT

	-- Recupera os parametros do Alerta
	SELECT @ThresholdCPU     = prmValue
		, @EmailDestination = prmOperator
	FROM dbaMonitor.dbo.Parameter
	WHERE prmId = @ParameterId;

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
	SELECT @AlertId   = alcId
          , @AlertType = alcType
	FROM dbaMonitor.dbo.AlertControl 
	WHERE alcId = (
                   SELECT MAX(alcId) 
                   FROM dbaMonitor.dbo.AlertControl  
                   WHERE prmId = @ParameterId 
                   AND  alcResolutionDate IS NULL
                   );

	-- Verifica a utilização da CPU
	IF ( OBJECT_ID('tempdb..#CPU_Utilization') IS NOT NULL )
		DROP TABLE #CPU_Utilization
	
	SELECT TOP(2)
		   record_id
		 , SQLProcessUtilization
		 , 100 - SystemIdle - SQLProcessUtilization as OtherProcessUtilization,
		[SystemIdle],
		100 - SystemIdle AS CPU_Utilization
	INTO #CPU_Utilization
	FROM	( 
				SELECT	CONVERT(INT, record.value('(./Record/@id)[1]', 'int'))											    		AS [record_id], 
						CONVERT(INT, record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int'))			AS [SystemIdle],
						CONVERT(INT, record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int'))	AS [SQLProcessUtilization], 
						[timestamp] 
				FROM ( 
						SELECT [timestamp], CONVERT(XML, [record]) AS [record] 
						FROM [sys].[dm_os_ring_buffers] 
						WHERE	[ring_buffer_type] = N'RING_BUFFER_SCHEDULER_MONITOR' 
								AND [record] LIKE '%<SystemHealth>%'
					) AS X					   
			) AS Y
	ORDER BY record_id DESC

	--	Verifica se o Consumo de CPU está maior do que o parametro
     SELECT @ProcessorUsage         = CPU_Utilization 
          , @SQLServerCpuUsage      = SQLProcessUtilization
          , @OtherProcessesCpuUsage = OtherProcessUtilization
     FROM #CPU_Utilization
	WHERE record_id = ( SELECT MAX(record_id) FROM #CPU_Utilization )

	IF  @ProcessorUsage    >= @ThresholdCPU
     AND @SQLServerCpuUsage >= @OtherProcessesCpuUsage
	BEGIN	

		IF ( SELECT CPU_Utilization 
               FROM #CPU_Utilization
			WHERE record_id = (SELECT MIN(record_id) FROM #CPU_Utilization )
		   ) > @ThresholdCPU
		BEGIN
			
               IF ISNULL(@AlertType, 'R') = 'R' /* Alerta náo existe ou não está pendente */
			BEGIN

			     -- Criação do Html do E-mail						
                    SELECT @HtmlScript = + '<font face="Verdana" size="4">Verificação de Consumo de CPU</font>' +
                                              '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                              <tr align="center">
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">SQL Server (%)</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Outros Processos (%)</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Livre (%)</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Total (%)</font></b></th>
                                              </tr>'

                    SELECT @HtmlScript = @HtmlScript +
                          '<tr>
                           <td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [SQLProcessUtilization] +'</font></td>' +  
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [OtherProcessUtilization]	 +'</font></td>' +  
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [SystemIdle] +'</font></td>' +
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [CPU_Utilization] +'</font></td>'  
			     FROM ( 
					SELECT TOP 1
						  [SQLProcessUtilization]   = CAST([SQLProcessUtilization] AS VARCHAR) 
						, [OtherProcessUtilization] = CAST((100 - SystemIdle - SQLProcessUtilization) AS VARCHAR)  
						, [SystemIdle]              = CAST([SystemIdle] AS VARCHAR)
						, [CPU_Utilization]         = CAST(100 - SystemIdle AS VARCHAR)
					FROM #CPU_Utilization
			          ORDER BY record_id DESC		
                         ) _
	      
				-- Corrige a Formatação da Tabela
				SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

                    SELECT @HtmlScript = @HtmlScript + '</table><br>'
			      
			     -- Corrige a Formatação da Tabela
			     SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

			     -- Variáveis para envio do e-mail
			     SET @Importance =	'High';

                    SET @Subject = 'Alerta: Consumo de CPU está acima de ' +  CAST((@ThresholdCPU) AS VARCHAR) + '% no Servidor: ' + @@SERVERNAME

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
		END
	END		
	ELSE 
	BEGIN	
		IF @AlertType = 'A'
		BEGIN
			     -- Criação do Html do E-mail						
                    SELECT @HtmlScript = + '<font face="Verdana" size="4">Verificação de Consumo de CPU</font>' +
                                              '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                              <tr align="center">
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">SQL Server (%)</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Outros Processos (%)</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Livre (%)</font></b></th>
                                                    <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Total (%)</font></b></th>
                                              </tr>'

                    SELECT @HtmlScript = @HtmlScript +
                          '<tr>
                           <td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [SQLProcessUtilization] +'</font></td>' +  
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [OtherProcessUtilization]	 +'</font></td>' +  
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [SystemIdle] +'</font></td>' +
                          '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [CPU_Utilization] +'</font></td>'  
			     FROM ( 
					SELECT TOP 1
						  [SQLProcessUtilization]   = CAST([SQLProcessUtilization] AS VARCHAR) 
						, [OtherProcessUtilization] = CAST((100 - SystemIdle - SQLProcessUtilization) AS VARCHAR)  
						, [SystemIdle]              = CAST([SystemIdle] AS VARCHAR)
						, [CPU_Utilization]         = CAST(100 - SystemIdle AS VARCHAR)
					FROM #CPU_Utilization
			          ORDER BY record_id DESC			      
                         ) _

				-- Corrige a Formatação da Tabela
				SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

                    SELECT @HtmlScript = @HtmlScript + '</table><br>'
						      
			     -- Corrige a Formatação da Tabela
			     SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

			     -- Variáveis para envio do e-mail
			     SET @Importance =	'High';

                    SET @Subject = 'Resolvido: Consumo de CPU está acima de ' +  CAST((@ThresholdCPU) AS VARCHAR) + '% no Servidor: ' + @@SERVERNAME

			     SET @EmailBody = @HtmlScript;

			     -- Dispara o e-mail com as informações coletadas
			     EXEC msdb.dbo.sp_send_dbmail
                                   @profile_name  = @MailProfile
                                 , @recipients    = @EmailDestination
                                 , @subject       = @Subject
                                 , @body          = @EmailBody
                                 , @body_format   = 'HTML'
                                 , @importance    = @Importance;
			
			-- Atualiza o Registro na Tabela de Controle dos Alertas informando que o alerta foi solucionado
               UPDATE dbaMonitor.dbo.AlertControl
                  SET alcType = 'R'
                    , alcResolutionDate = SYSDATETIME()
               WHERE alcId = @AlertId;	

		END
	END		-- FIM - CLEAR
END

GO
