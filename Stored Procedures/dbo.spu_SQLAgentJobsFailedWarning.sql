SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[spu_SQLAgentJobsFailedWarning]
AS
BEGIN
	SET NOCOUNT ON

     -- Declara as variaveis
     DECLARE @MailProfile       VARCHAR(250)
           , @HtmlScript        VARCHAR(MAX)
           , @HtmlTableStyle    VARCHAR(MAX)
           , @HtmlThStyle       VARCHAR(MAX)
           , @HtmlTdStyle       VARCHAR(MAX)
           , @EmailDestination  VARCHAR(200)
           , @AlertType         CHAR(1)
           , @AlertId           INT
           , @Importance        VARCHAR(6)
           , @EmailBody         VARCHAR(MAX)
           , @Subject           VARCHAR(500)
           , @JobFailedInterval INT 
           , @StartDate         DATETIME2(0)
           , @ReferenceDate     DATETIME2(0);

	-- Consumo CPU
	DECLARE @ParameterId INT = (
                                SELECT prmId FROM 
                                dbaMonitor.dbo.Parameter (NOLOCK) 
                                WHERE prmName = 'Falha em Job (SQL Agent)'
                                );

     IF ISNULL(@ParameterId, 0) = 0
     BEGIN
         RAISERROR('Falha de configuração para o parâmetro [Falha em Job (SQL Agent)] em dbaMonitor.dbo.Parameter.', 16, 1);
     END

	-- Recupera os parametros do Alerta
	SELECT @JobFailedInterval = prmValue
		, @EmailDestination  = prmOperator
	FROM dbaMonitor.dbo.Parameter
	WHERE prmId = @ParameterId;

     -- Parametrização
     SET @ReferenceDate = SYSDATETIME();
     SET @StartDate = CONVERT(CHAR(8), (DATEADD(HOUR, -1*@JobFailedInterval, @ReferenceDate)));

     -- Recupera o profile de e-mail caso o mesmo n�o tenha sido informado 
     IF @MailProfile IS NULL
     BEGIN
        SELECT TOP 1 @MailProfile = name
        FROM msdb.dbo.sysmail_profile WITH (NOLOCK);
     END;    

     -- Estrutura do CSS - Formata��o HTML
     SELECT @HtmlTableStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'tableHTML';
     
     SELECT @HtmlThStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'thHTML';
     
     SELECT @HtmlTdStyle = cnfValue FROM dbaMonitor.dbo.[Configurations] WHERE cnfAttribute = 'tdHTML';

	IF (OBJECT_ID('TempDB..#FailedJobs') IS NOT NULL)
     BEGIN
		DROP TABLE #FailedJobs;
     END; 

	IF (OBJECT_ID('TempDB..#JobHistory') IS NOT NULL)
     BEGIN
		DROP TABLE #JobHistory;
     END; 

	CREATE TABLE #JobHistory 
     (
       history_code		INT IDENTITY(1,1)
     , instance_id		INT
     , job_id			VARCHAR(255)
     , job_name		VARCHAR(255)
     , step_id			INT
     , step_name		VARCHAR(255)
     , sql_message_id	INT
     , sql_severity		INT
     , sql_message		VARCHAR(4490)
     , run_status		INT
     , run_date		VARCHAR(20)
     , run_time		VARCHAR(20)
     , run_duration		INT
     , operator_emailed	VARCHAR(100)
     , operator_netsent	VARCHAR(100)
     , operator_paged	VARCHAR(100)
     , retries_attempted	INT
     , nm_server		VARCHAR(100)  
	);

     -- Realiza a carga de histórico de jobs do SQL Agent
	INSERT INTO #JobHistory
	EXEC msdb.dbo.sp_help_jobhistory @mode = 'FULL', @start_run_date = @StartDate;

     -- Realiza a carga de jobs com falha no período
	SELECT TOP 50
            [Server] = [nm_server]  
          , [JobName] = [job_name] 
          , [JobStatus] = CASE [run_status]
                               WHEN 0 THEN 'Failed'
                               WHEN 1 THEN 'Succeeded'
                               WHEN 2 THEN 'Retry (step only)'
                               WHEN 3 THEN 'Cancelled'
                               WHEN 4 THEN 'In-progress message'
                               WHEN 5 THEN 'Unknown' 
                         END
          , [ExecutionDate] = ExecTime.ExecutionDate
          , [RunDuration] = ExecTime.RunDuration
          , [SQLMessage] = CAST([SQL_Message] AS VARCHAR(3990))  
     INTO #FailedJobs
     FROM #JobHistory 
          CROSS APPLY 
          (
          SELECT [ExecutionDate] = CAST([run_date] + ' ' +
                                   RIGHT('00' + SUBSTRING([run_time], (LEN([run_time])-5), 2), 2) + ':' +
                                   RIGHT('00' + SUBSTRING([run_time], (LEN([run_time])-3), 2), 2) + ':' +
                                   RIGHT('00' + SUBSTRING([run_time], (LEN([run_time])-1), 2), 2) AS VARCHAR) 
               , [RunDuration] = RIGHT('00' + SUBSTRING(CAST([run_duration] AS VARCHAR), (LEN([run_duration])-5), 2), 2) + ':' +
                                 RIGHT('00' + SUBSTRING(CAST([run_duration] AS VARCHAR), (LEN([run_duration])-3), 2), 2) + ':' +
                                 RIGHT('00' + SUBSTRING(CAST([run_duration] AS VARCHAR), (LEN([run_duration])-1), 2), 2) 
          ) ExecTime 
     WHERE [step_id] = 0 
     AND   [run_status] != 1 
     AND   ExecTime.ExecutionDate >= DATEADD(HOUR, -1*@JobFailedInterval, @ReferenceDate) 
     AND   ExecTime.ExecutionDate < @ReferenceDate
     ORDER BY ExecTime.ExecutionDate DESC;

	-- Verifica o Último Tipo do Alerta registrado | S: Solucionado / A: Alerta
	SELECT @AlertId   = alcId
          , @AlertType = alcType
	FROM dbaMonitor.dbo.AlertControl 
	WHERE alcId = (
                   SELECT MAX(alcId) 
                   FROM dbaMonitor.dbo.AlertControl  
                   WHERE prmId = @ParameterId 
                   AND  alcResolutionDate IS NULL
                   );

	--	Verifica se o Consumo de CPU est� maior do que o parametro
	IF EXISTS ( SELECT 1
                 FROM #FailedJobs
               )
	BEGIN	

          IF ISNULL(@AlertType, 'R') = 'R' /* Alerta não existe ou não está pendente */
		BEGIN

			-- Cria��o do Html do E-mail						
               SELECT @HtmlScript = + '<font face="Verdana" size="4">SQL Agent Jobs com Falha</font>' +
                                             '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                             <tr align="center">
                                                  <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Nome</font></b></th>
                                                  <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Status</font></b></th>
                                                  <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Hora da execução</font></b></th>
                                                  <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Duração</font></b></th>
                                                  <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Mensagem</font></b></th>
                                             </tr>'

               SELECT @HtmlScript = @HtmlScript +
                         '<tr>
                          <td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + _.JobName +'</font></td>' +  
                         '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + _.JobStatus	 +'</font></td>' +  
                         '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + _.ExecutionDate +'</font></td>' +
                         '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + _.RunDuration +'</font></td>' +
                         '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + _.SQLMessage +'</font></td>'  
			FROM ( 
				SELECT JobName
                         , JobStatus
                         , ExecutionDate
                         , RunDuration
                         , SQLMessage
                    FROM #FailedJobs	
                    ) _
	      
			-- Corrige a Formatação da Tabela
			SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

               SELECT @HtmlScript = @HtmlScript + '</table><br>'
			
			-- Vari�veis para envio do e-mail
			SET @Importance = 'High';

               SET @Subject = 'Alerta: Falha no(s) Job(s) do SQL Agent Job(s) no Servidor: ' + @@SERVERNAME;

			SET @EmailBody = @HtmlScript;

			-- Dispara o e-mail com as informa��es coletadas
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

			     -- Vari�veis para envio do e-mail
			     SET @Importance =	'High';

                    SET @Subject = 'Resolvido: Falha no(s) Job(s) do SQL Agent Job(s) no Servidor: ' + @@SERVERNAME;

			     SET @EmailBody = @HtmlScript;

			     -- Dispara o e-mail com as informa��es coletadas
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
	END		
END

GO
