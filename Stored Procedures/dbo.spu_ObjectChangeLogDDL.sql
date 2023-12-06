SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[spu_ObjectChangeLogDDL]
WITH EXECUTE AS OWNER
AS
BEGIN
   DECLARE @MessageBody XML


   WHILE ( 1 = 1 )
   BEGIN


      BEGIN TRANSACTION
      -- Recebe a próxima mensagem disponível existente na fila
         WAITFOR ( RECEIVE TOP ( 1 ) -- trata somente 1 mensagem por vez
                          @MessageBody =  CONVERT ( XML , CONVERT (NVARCHAR (MAX), message_body))
                   FROM dbo. ObjectChangeLogDDL_BrokerQueue ), TIMEOUT 1000  -- se a fila estiver vazia, atualiza e finaliza


      -- Finaliza se nada foi recebido ou alteração realizada pelo Agent
      IF ( @@ROWCOUNT = 0 )
      BEGIN
         ROLLBACK TRANSACTION
         BREAK
      END


      IF (SELECT @MessageBody.value ('(/EVENT_INSTANCE/LoginName)[1]', 'varchar(128)')) != 'CFOAB\OabSQLAgent'
      BEGIN


           -- Processa dados da fila
           INSERT INTO dbo.ObjectChangeLog
                     ( EventType
                     , PostTime
                     , ServerName
                     , LoginName
                     , UserName
                     , DatabaseName
                     , SchemaName
                     , ObjectName
                     , ObjectType
                     , TSQLCommand
                     )
              SELECT @MessageBody .value( '(/EVENT_INSTANCE/EventType)[1]', 'varchar(128)') AS EventType
                   , CONVERT (DATETIME , @MessageBody.value ('(/EVENT_INSTANCE/PostTime)[1]', 'varchar(128)')) AS PostTime
                   , @MessageBody .value( '(/EVENT_INSTANCE/ServerName)[1]', 'varchar(128)') AS ServerName
                   , @MessageBody .value( '(/EVENT_INSTANCE/LoginName)[1]', 'varchar(128)') AS LoginName
                   , @MessageBody .value( '(/EVENT_INSTANCE/UserName)[1]', 'varchar(128)') AS UserName
                   , @MessageBody .value( '(/EVENT_INSTANCE/DatabaseName)[1]' , 'varchar(128)') AS DatabaseName
                   , @MessageBody .value( '(/EVENT_INSTANCE/SchemaName)[1]', 'varchar(128)') AS SchemaName
                   , @MessageBody .value( '(/EVENT_INSTANCE/ObjectName)[1]', 'varchar(128)') AS ObjectName
                   , @MessageBody .value( '(/EVENT_INSTANCE/ObjectType)[1]', 'varchar(128)') AS ObjectType
                   , @MessageBody .value( '(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]' , 'nvarchar(max)') AS TSQLCommand
         
     END


      COMMIT TRANSACTION
   END
END
  

GO
