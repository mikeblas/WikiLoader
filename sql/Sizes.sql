use Wikipedia;

with fs
as
(
    select database_id, type, size * 8.0 / 1024 size
    from sys.master_files
)
select 
    name,
    (select sum(size) from fs where type = 0 and fs.database_id = db.database_id) DataFileSizeMB,
    (select sum(size) from fs where type = 1 and fs.database_id = db.database_id) LogFileSizeMB
from sys.databases db
order by DataFileSizeMB DESC


-- 1593344.000000
-- 1593600.000000
-- 1593856.000000
-- 1595904.000000
-- 1600512.000000
-- 1619968.000000
-- 1630720.000000
-- 1662976.000000

select * from run order by runid desc

select SourceFileName, DATEDIFF(mi, StartTime, EndTime) AS DurationMinutes, StartTime, EndTime
from run
WHERE RunID >= 213 -- and EndTime IS NOT NULL
ORDER BY STartTime DESC


SELECT a FROM (VALUES (3),(NULL),(NULL)) as x(a) GROUP BY a;

SELECT CASE WHEN a IS NULL THEN 5 ELSE a END FROM (VALUES (3),(NULL),(NULL)) as x(a) GROUP BY a;



SELECT CAST(CAST('2024-08-01' AS DATE) AS INTEGER)