SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE VIEW [dbo].[vw_UserAccess]
 
AS
SELECT srv.srvName  
     , usr.usrLogin
     , usr.usrPasswd
FROM dbo.Servers srv
     INNER JOIN dbo.Users usr 
            ON  srv.srvId = usr.srvId 
GO
