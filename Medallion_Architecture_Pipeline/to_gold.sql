-- Query para a camada gold

CREATE OR REFRESH MATERIALIZED VIEW broadcast_gold AS
WITH log_identifier_1 AS (
  SELECT rli.logServiceID, rli.LogIdentifierID
    FROM referencetables.log_identifier rli
  WHERE rli.PrimaryFG = 1
)
 SELECT 
  dbs.BroadcastLogID,
  dbs.LogServiceID, 
  dbs.ClosedCaptionID,
  dbs.ProgramClassID,
  dbs.ProgramTitle,
  dbs.LogEntryDate,
  dbs.year,
  dbs.month,
  dbs.day,
  dbs.Quarter,
  dbs.Duration_seconds,
  dbs.EndTime,
  dbs.StartTime,
  rcpc.EnglishDescription, 
  li.LogIdentifierID, 
  dbkg.prediction
    FROM default.broadcast_silver dbs
      LEFT JOIN referencetables.cd_program_class rcpc
        ON dbs.ProgramClassID = rcpc.ProgramClassID
      LEFT JOIN log_identifier_1 li
        ON dbs.LogServiceID = li.LogServiceID
      LEFT JOIN default.broadcast_kmeans_gold dbkg
        ON dbs.BroadcastLogID = dbkg.broadcastLogID
