USE Wikipedia;
SET STATISTICS IO ON;

-- Find the average fragmentation percentage of all indexes
-- in all tables of this database
SELECT OBJECT_SCHEMA_NAME(ips.object_id) AS schema_name,
       OBJECT_NAME(ips.object_id) AS object_name,
       i.name AS index_name,
       i.type_desc AS index_type,
       ips.avg_fragmentation_in_percent,
       ips.avg_page_space_used_in_percent,
	   ips.record_count,
       ips.page_count,
       ips.alloc_unit_type_desc
FROM sys.dm_db_index_physical_stats(DB_ID(), default, default, default, 'SAMPLED') AS ips
INNER JOIN sys.indexes AS i 
ON ips.object_id = i.object_id
   AND
   ips.index_id = i.index_id
ORDER BY page_count DESC;


-- Find the average fragmentation percentage of all indexes
-- of one table in a database
SELECT OBJECT_SCHEMA_NAME(ips.object_id) AS schema_name,
       OBJECT_NAME(ips.object_id) AS object_name,
       i.name AS index_name,
       i.type_desc AS index_type,
       ips.avg_fragmentation_in_percent,
       ips.avg_page_space_used_in_percent,
	   ips.record_count,
       ips.page_count,
       ips.alloc_unit_type_desc
FROM sys.dm_db_index_physical_stats (DB_ID(N'Wikipedia'), OBJECT_ID(N'PageRevision'), default, default, 'SAMPLED') AS ips
    JOIN sys.indexes AS i ON ips.object_id = i.object_id AND ips.index_id = i.index_id; 
GO


-- fast total rows of all objects, including size
select ST.name, SI.name, SI.rowcnt, SI.dpages * 8 'Data KB', SI.reserved * 8 'Reserved KB', SI.used * 8 'Used KB'
  from sysindexes AS SI 
  JOIN sys.tables AS ST ON ST.object_id = SI.ID;



-- rebuilds for Page table
ALTER INDEX [Page_Space_Title] ON [Page] REBUILD WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = ON);

-- rebuilds for PageRevision table
ALTER INDEX [PK__PageRevi__9E1CEB3EC170874F] ON PageRevision REBUILD WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = ON);
ALTER INDEX [PageRevision_Contributor_When] ON PageRevision REBUILD WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = ON);
ALTER INDEX [PageRevision_PageID_When] ON PageRevision REBUILD WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = ON);
ALTER INDEX [PageRevision_When] ON PageRevision REBUILD WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = ON);


-- rebuilds for User table
ALTER INDEX [User_PK] ON [User] REBUILD WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = ON);
ALTER INDEX [User_AK] ON [User] REBUILD WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = ON);


