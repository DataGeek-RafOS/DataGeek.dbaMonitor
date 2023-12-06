CREATE TABLE [dbo].[Users]
(
[srvId] [tinyint] NOT NULL,
[usrLogin] [varchar] (128) COLLATE Latin1_General_CI_AI NOT NULL,
[usrPasswd] [varchar] (128) COLLATE Latin1_General_CI_AI NOT NULL,
[usrOn] [bit] NOT NULL CONSTRAINT [DF_Users_usrOn] DEFAULT ((1))
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Users] ADD CONSTRAINT [PK_Security] PRIMARY KEY CLUSTERED ([srvId], [usrLogin]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Users] ADD CONSTRAINT [FK_Usuario_Instance] FOREIGN KEY ([srvId]) REFERENCES [dbo].[Servers] ([srvId])
GO
