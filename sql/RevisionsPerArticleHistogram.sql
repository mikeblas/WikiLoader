use wikipedia;

set statistics io on;
set statistics time on;


WITH Intervals AS 
(
   SELECT LowBound, HighBound
FROM
   (VALUES 
        (1, 10),
        (11, 100),
        (101, 500),
        (501, 1000),
        (1001, 2500),
        (2501, 5000),
        (5001, 10000),
        (10001, 15000),
        (15001, 20000),
        (20000, 30000),
        (30001, 50000),
        (50001, 100000),
        (100001, NULL)
   ) AS I (LowBound, HighBound)
),
RevisionCounts AS (
SELECT PageID, COUNT(PageRevisionID) RevisionCount
FROM PageRevision
WHERE NamespaceID = 0
GROUP BY PageID
)
SELECT LowBound, HighBound, COUNT(PageID) AS PagesInBucket
 FROM RevisionCounts
 JOIN Intervals ON RevisionCount > Lowbound AND (HighBound IS NULL OR RevisionCount <HighBound)
GROUP BY LowBound, HighBound

