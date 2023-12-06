CREATE TABLE [dbo].[DimensaoData]
(
[ChaveData] [int] NOT NULL,
[Data] [date] NULL,
[DataPTBR] [char] (10) COLLATE Latin1_General_CI_AI NULL,
[NumeroDiaDaSemana] [tinyint] NULL,
[NomeDiaDaSemana] [varchar] (13) COLLATE Latin1_General_CI_AI NULL,
[DiaAbrevDaSemana] [varchar] (5) COLLATE Latin1_General_CI_AI NULL,
[NumeroDiaDoMes] [tinyint] NULL,
[NumeroDiaDoAno] [smallint] NULL,
[NumeroSemanaDoAno] [tinyint] NULL,
[NomeDoMes] [varchar] (10) COLLATE Latin1_General_CI_AI NULL,
[NumeroMesDoAno] [tinyint] NULL,
[Trimestre] [tinyint] NULL,
[AnoCalendario] [smallint] NULL,
[DiaUtil] [bit] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[DimensaoData] ADD CONSTRAINT [PK_DimensaoData] PRIMARY KEY CLUSTERED ([ChaveData]) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [ixNCL_Data] ON [dbo].[DimensaoData] ([Data], [ChaveData]) ON [PRIMARY]
GO
GRANT SELECT ON  [dbo].[DimensaoData] TO [usrReports]
GO
