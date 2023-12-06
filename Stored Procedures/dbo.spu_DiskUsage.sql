SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[spu_DiskUsage]
AS
BEGIN 

SET NOCOUNT ON;

     -- Declaração de variáveis
     --------------------------
     DECLARE @vc_UnitAloc  CHAR(2)
           , @vn_ObjFSO    INT
           , @vn_ObjDrive  INT
           , @vc_DriveSize VARCHAR(256)
           , @vn_Return    INT;

     DECLARE @vc_DriveLetter CHAR(3)
           , @vc_DriveLabel  VARCHAR(256)
           , @vn_FreeSpace   INT
           , @vn_Capacity    BIGINT;

     DECLARE @vt_DiskUsage TABLE
     (
       Drive       CHAR(3)
     , Label       VARCHAR(256)
     , Capacity    NUMERIC(9, 1)
     , FreeSpace   NUMERIC(9, 2)
     , PercentFree NUMERIC(5, 2)
     );

     SET @vc_UnitAloc = 'GB';

     INSERT INTO @vt_DiskUsage( Drive , FreeSpace)
          EXEC master.dbo .xp_FixedDrives ;

     -- Chamado do OACreate - OLE Object
     -----------------------------------
     EXEC @vn_Return = master.dbo.sp_OACreate 'scripting.FileSystemObject' , @vn_ObjFSO OUTPUT;

     -- Ciclo para verificação de volumes
     ------------------------------------
     WHILE EXISTS (
                  SELECT 1
                  FROM @vt_DiskUsage
                  WHERE Capacity IS NULL
                  )
     BEGIN

          SELECT TOP 1
                 @vc_DriveLetter = RTRIM(Drive) + ':\'
               , @vn_FreeSpace = FreeSpace
          FROM @vt_DiskUsage
          WHERE Capacity IS NULL;

          EXEC @vn_Return = master.dbo.sp_OAMethod @vn_ObjFSO , 'GetDrive' , @vn_ObjDrive OUTPUT, @vc_DriveLetter ;
          EXEC Master.dbo.sp_OAMethod @vn_ObjDrive, 'TotalSize', @vc_DriveSize OUTPUT ;
          EXEC Master.dbo.sp_OAMethod @vn_ObjDrive, 'VolumeName', @vc_DriveLabel OUTPUT ;

          UPDATE @vt_DiskUsage
             SET Capacity = ((CAST(@vc_DriveSize AS NUMERIC( 18))/1024.0 )/1024.0)
              , Label = @vc_DriveLabel
          WHERE LOWER(RTRIM (Drive )) + ':\' = LOWER(@vc_DriveLetter);
     
          EXEC master.dbo.sp_OADestroy @vn_ObjDrive;

     END

     EXEC master.dbo.sp_OADestroy @vn_ObjFSO ;

     -- Atualização de registros para Output
     ---------------------------------------
     UPDATE @vt_DiskUsage
        SET PercentFree = (( FreeSpace/Capacity ) * 100)
          , Capacity = CASE WHEN @vc_UnitAloc = 'GB'
                            THEN Capacity / 1024.0
                            ELSE Capacity
                       END
          , FreeSpace = CASE WHEN @vc_UnitAloc = 'GB'
                             THEN FreeSpace / 1024.0
                             ELSE FreeSpace
                        END;

     -- Retorno da informações
     -------------------------
     SELECT RTRIM(Drive) + ':\' AS Drive
          , Label
          , Capacity
          , FreeSpace
          , PercentFree
     FROM @vt_DiskUsage
     WHERE Label NOT IN ('Quorum', 'SnapInfo')
     ORDER BY Drive ASC;
      
END 
 
  
GO
