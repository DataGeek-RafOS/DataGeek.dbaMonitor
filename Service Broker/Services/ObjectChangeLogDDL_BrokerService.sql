CREATE SERVICE [ObjectChangeLogDDL_BrokerService]
AUTHORIZATION [dbo]
ON QUEUE [dbo].[ObjectChangeLogDDL_BrokerQueue]
(
[http://schemas.microsoft.com/SQL/Notifications/PostEventNotification]
)
GO
