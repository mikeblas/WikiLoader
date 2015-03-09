SET STATISTICS IO ON;

-- Find the average fragmentation percentage of all indexes
-- in all tables of a database
SELECT a.index_id, name, avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats (DB_ID(N'Wikipedia'), NULL, NULL, NULL, NULL) AS a
    JOIN sys.indexes AS b ON a.object_id = b.object_id AND a.index_id = b.index_id; 
GO

-- Find the average fragmentation percentage of all indexes
-- of one table in a database
SELECT a.index_id, name, avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats (DB_ID(N'Wikipedia'), OBJECT_ID(N'PageRevision'), NULL, NULL, NULL) AS a
    JOIN sys.indexes AS b ON a.object_id = b.object_id AND a.index_id = b.index_id; 
GO

-- fast total rows
select ST.name, SI.name, SI.rowcnt
  from sysindexes AS SI 
  JOIN sys.tables AS ST ON ST.object_id = SI.ID;



ALTER INDEX ALL ON [User]
REBUILD WITH (FILLFACTOR=80);

ALTER INDEX PageRevision_AK ON [PageRevision] REBUILD;


-- sp_help PageRevision