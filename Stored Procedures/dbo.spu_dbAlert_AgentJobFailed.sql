SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

/*******************************************************************************************************************************
--	ALERTA: CONSUMO CPU
*******************************************************************************************************************************/

CREATE PROCEDURE [dbo].[spu_dbAlert_AgentJobFailed]
AS
BEGIN
	SET NOCOUNT ON

	-- Consumo CPU
	DECLARE @ParameterId INT = (
                                SELECT prmId 
                                FROM dbaMonitor.dbo.Parameter (NOLOCK) 
                                WHERE prmName = 'SQL Agent - Jobs com Falha'
                                );

     -- Declara as variaveis
     DECLARE @MailProfile      VARCHAR(250)
           , @HtmlScript       VARCHAR(MAX)
           , @HtmlTableStyle   VARCHAR(MAX)
           , @HtmlThStyle      VARCHAR(MAX)
           , @HtmlTdStyle      VARCHAR(MAX)
           , @EmailDestination VARCHAR(200)
           , @AlertType        CHAR(1)
           , @AlertId          INT
           , @AlertExpired     BIT = CONVERT(BIT, 0)
           , @Importance       VARCHAR(6)
           , @EmailBody        VARCHAR(MAX)
           , @Subject          VARCHAR(500)
           , @TimeThreshold    INT 

	-- Recupera os parametros do Alerta
	SELECT @TimeThreshold    = prmValue
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
     -- Força a expiração do alerta a cada 24 horas para envio de uma nova mensagem
	SELECT @AlertId      = alcId
         , @AlertType    = alcType
         , @AlertExpired = IIF(DATEDIFF(HOUR, alcCreationDate, CURRENT_TIMESTAMP) > 24, CONVERT(BIT, 1), CONVERT(BIT, 0))
	FROM dbaMonitor.dbo.AlertControl 
	WHERE alcId = (
                   SELECT MAX(alcId) 
                   FROM dbaMonitor.dbo.AlertControl  
                   WHERE prmId = @ParameterId 
                   AND  alcResolutionDate IS NULL
                   );

	-- Cria a tabela que ira armazenar os dados dos processos
	IF ( OBJECT_ID('TempDB..#FailedJobs') IS NOT NULL )
     BEGIN
		DROP TABLE #FailedJobs
	END;
     	
	CREATE TABLE #FailedJobs 
     (		
	  JobName      VARCHAR(256)   NOT NULL 
     , StepName     VARCHAR(256) NULL 
     , ErrorMessage NVARCHAR(4000) NULL 
     , RunDate      DATETIME2(0) NOT NULL 
	);
     
     INSERT INTO #FailedJobs ( JobName, StepName, ErrorMessage, RunDate )
          SELECT Jobs.name
               , Hist.step_name
               , Hist.message
               , msdb.dbo.agent_datetime(Hist.run_date , Hist.run_time) As [RunDate]
          FROM msdb.dbo.sysjobs AS Jobs
               INNER JOIN 
               msdb.dbo.sysjobhistory AS Hist
                    ON Hist.job_id = Jobs.job_id     
          WHERE Jobs.enabled = CONVERT(BIT, 1)
          AND   Hist.run_status = 0
          AND   Hist.step_name <> '(Job outcome)'
          AND   NOT EXISTS ( SELECT 1 
                             FROM msdb.dbo.sysjobhistory inHist
                             WHERE inHist.job_id = Jobs.job_id
                             AND   inHist.run_status = 1
                             AND   msdb.dbo.agent_datetime(inHist.run_date , inHist.run_time) > msdb.dbo.agent_datetime(Hist.run_date , Hist.run_time)
                           );

     IF  ISNULL(@AlertType, 'R') = 'R' /* Se não existe alerta pendente */
     AND EXISTS ( SELECT 1 FROM #FailedJobs ) /* Existem jobs com falha */
     BEGIN 

	     -- Criação do Html do E-mail						
          SELECT @HtmlScript = + '<font face="Verdana" size="4">SQL Agent - Falha de execução de Jobs</font>' +
                                        '<table ' + @HtmlTableStyle + ' height="50" cellSpacing="0" cellPadding="0" width="1300" >
                                        <tr align="center">
                                             <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Nome do Job</font></b></th>
                                             <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Nome do Step</font></b></th>
                                             <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Mensagem de erro</font></b></th>
                                             <th ' + @HtmlThStyle + 'width="9%" height="15"><b><font face="Verdana" size="1.5" color="#FFFFFF">Data de execução</font></b></th>
                                        </tr>'

          SELECT @HtmlScript = @HtmlScript +
                    '<tr>
                     <td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [JobName] +'</font></td>' +  
                    '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [StepName]	 +'</font></td>' +  
                    '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [ErrorMessage] +'</font></td>' +
                    '<td ' + @HtmlTdStyle + 'align="center" height="21"><font face="Verdana" size="1">' + [RunDate] +'</font></td>'  
		FROM ( 
			SELECT TOP 1
				  JobName
				, StepName
				, ErrorMessage
				, CONVERT(CHAR(10), RunDate, 103) + ' ' + CONVERT(CHAR(10), RunDate, 108) AS RunDate
			FROM #FailedJobs
			ORDER BY RunDate ASC	
               ) _
	      
		-- Corrige a Formatação da Tabela
		SET @HtmlScript = REPLACE( REPLACE( REPLACE( @HtmlScript, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

          SELECT @HtmlScript = @HtmlScript + '</table><br>'
                   
		-- Variáveis para envio do e-mail
		SET @Importance = 'High';

          SET @Subject = 'Alerta: Falha em execução - SQL Agent Jobs no Servidor: ' + @@SERVERNAME

		SET @EmailBody = @HtmlScript;

		-- Dispara o e-mail com as informações coletadas
		EXEC msdb.dbo.sp_send_dbmail
                         @profile_name    = @MailProfile
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
	
	END;

	IF @AlertType = 'A' /* Pendência está aberta */ 
	BEGIN	
		
          IF NOT EXISTS ( SELECT 1 FROM #FailedJobs ) 
		BEGIN

			     -- Variáveis para envio do e-mail
			     SET @Importance = 'High';

                    SET @Subject = 'Resolvido: Falha em execução - SQL Agent Jobs no Servidor: ' + @@SERVERNAME

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

		IF @AlertExpired = CONVERT(BIT, 1)
          BEGIN 

			-- Atualiza o Registro na Tabela de Controle dos Alertas informando que o alerta foi solucionado para forçar uma nova notificação
               UPDATE dbaMonitor.dbo.AlertControl
                  SET alcType = 'R'
                    , alcResolutionDate = SYSDATETIME()
               WHERE alcId = @AlertId;	

          END 

	END		
END

GO
