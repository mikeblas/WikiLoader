-- sELECT * FROM sys.dm_exec_requests;

     SELECT sqltext.TEXT,
			req.session_id,
			req.blocking_session_id,
			req.status,
			req.command,
			req.cpu_time,
			req.total_elapsed_time, 
			req.reads,
			req.logical_reads,
			req.writes,
			req.wait_Type,
			req.wait_time,
			req.last_wait_type,
--			req.logical_writes,
  			req.wait_resource
       FROM sys.dm_exec_requests req
CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS sqltext;

 select rowcnt, object_name(id)
  from sysindexes
 WHERE INDID = 1;

 -- KEY: 21:72057594041860096 (ce9d92697c46)
 SELECT o.name, i.name, p.index_id, i.index_id
FROM sys.partitions p
JOIN sys.objects o ON p.object_id = o.object_id
JOIN sys.indexes i ON p.object_id = i.object_id
AND p.index_id = i.index_id
WHERE p.hobt_id = 72057594041860096;

select * from sys.partitions p where p.hobt_id = 72057594041860096;

select object_name(277576027);
select * from sys.indexes where index_id = 1 and object_id = 277576027

sp_help pagerevision
_pok

sp_help page;

sp_help pagerevision;

select count(*) FROM [Revisions_ZINC_12];

select * FROM [Revisions_ZINC_13];



DECLARE @FirstPages AS BIGINT;
DECLARE @LastPages AS BIGINT;
DECLARE @FirstBatches AS BIGINT;
DECLARE @LastBatches AS BIGINT;

SELECT @FirstPages = cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Page splits/sec';
SELECT @FirstBatches = cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Batch Requests/sec';

WAITFOR DELAY '00:00:01';

SELECT @LastPages = cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Page splits/sec';
SELECT @LastBatches = cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Batch Requests/sec';

SELECT 'Page Splits', @LastPages - @FirstPages
UNION ALL
SELECT 'Batches', @LastBatches - @FirstBatches;


SELECT * FROM sys.dm_os_performance_counters WHERE counter_name = 'Batch Requests/sec';
select * FROM sys.dm_os_performance_counters WHERE counter_name = 'Page splits/sec';

-- sp_help PageRevision



DBCC traceon (3604)
GO
23:4:10105296
DBCC page (23, 4, 10105296) --Database_id,file_id,page_id 

select object_name(277576027)
153
256


sp_help pageRevision


SET STATISTICS IO ON;
select PageRevisionID FROM PageRevision WHERE PageID = 82131;

SELECT top 100 * FROM [User]
LEFT JOIN 
(
select ContributorID, COUNT(1) AS ContributorCount FROM PageRevision WHERE PageID = 82131
GROUP BY ContributorID
) AS X 
ON X.ContributorID = [User].UserID

sp_help [user]

ORDER BY 2 DESC;

select top 1000 * from page ORDER BY PageID DESC



select top 10 * FROM PageRevision
WHERE ContributorID IS NOT NULL
AND ContributorID NOT IN (SELECT ContributorID FROM [User])



select count(*) from activity

sp_help activity