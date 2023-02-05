
use Wikipedia;
SET STATISTICS IO ON;

-- all the namespaces
select * from Namespace;

select * from msdb..suspect_pages
-- number of users, pages, revisions
select count(*) AS CountUsers from [user] WITH(NOLOCK) ;
SELECT COUNT(*) AS CountRevisions FROM PageRevision;
SELECT COUNT(*) AS CountPages FROM Page;

select * from run ORDER By RunID DESC;

select *, DATEDIFF(millisecond, StartTime, EndTime) from run ORDER By RunID DESC;
select *, DATEDIFF(millisecond, StartTime, EndTime) from run ORDER By DATEDIFF(millisecond, StartTime, EndTime)  DESC;

select * from Activity WHERE RunID = 50 ORDER BY DurationMillis DESC;
select * from Activity WHERE RunID = 50 ORDER BY TargetPageID;



select RunID, SUM(WorkCount)
 FROM Activity
 WHERE Activity = 'Merge PageRevisions'
GROUP BY RunID


-- counts of page states
-- runs in about 5 minutes 
select NamespaceID,
   SUM(CASE WHEN TextAvailable = 1 THEN 1 ELSE 0 END) AS TextAvailable,
   SUM(CASE WHEN TextAvailable = 0 THEN 1 ELSE 0 END) AS TextNotAvailable,
   SUM(CASE WHEN TextAvailable IS NULL THEN 1 ELSE 0 END) AS TextAvailableNull
FROM  PageRevision
GROUP BY NamespaceID
ORDER BY NamespaceID


-- ID ranges and page count per Namespace
  SELECT NamespaceID, MIN(PageID) AS Min, MAX(PageID) AS Max, COUNT(PageRevisionID) AS Count
    FROM PageRevision
GROUP BY NamespaceID;

  SELECT NamespaceID, MIN(PageID) AS Min, MAX(PageID) AS Max, COUNT(PageRevisionID) AS Count,
         (SELECT COUNT(*) FROM PageRevisionText WHERE PageRevisionText.NamespaceID = PageRevision.NamespaceID) AS Texts
    FROM PageRevision
GROUP BY NamespaceID
ORDER BY NamespaceID;


-- what pages are marked available, but not actually found?
SELECT PageRevision.NamespaceID, PageRevision.PageID, PageRevision.PageRevisionID
  FROM PageRevision
 WHERE TextAvailable = 1
   AND PageRevision.ArticleText IS NULL
   AND NOT EXISTS (
			SELECT 1
			  FROM PageRevisionText
		     WHERE PageRevisionText.PageID = PageRevision.PageID
			   AND PageRevisionText.NamespaceID = PageRevision.NamespaceID
			   AND PageRevisionText.PageRevisionID = PageRevision.PageRevisionID
			   )
ORDER BY 1, 2, 3;


-- number of revisions per page
SELECT Total.PageID, PageName, PageRevisionCount
 FROM 
( SELECT PageID, Count(PageRevisionID) AS PageRevisionCount
  FROM PageRevision
  GROUP BY PageID) AS Total
JOIN Page ON Page.PageID = Total.PageID
  ORDER BY 3 DESC;


-- top 100 revisions per page
SELECT NS.NamespaceName, Page.PageName, RevisionCount
FROM
( select TOP 100 NamespaceID, PageID, COUNT(PagerevisionID) AS RevisionCount
FROM PageRevision
 GROUP BY NamespaceID, PageID
 ORDER BY 3 DESC
 ) AS RevCounts
 JOIN [Page] ON [Page].PageID = RevCounts.PageID
 JOIN Namespace AS NS ON NS.NamespaceID = RevCounts.NamespaceID
ORDER BY 3 DESC;


-- Total pages, revisions, by Namespace
SELECT NS.NamespaceName, TOTALS.DistinctPages, TOTALS.PageRevisions
 FROM (
	  SELECT NamespaceID, COUNT(DISTINCT PageID) AS DistinctPages, COUNT(PageRevisionID) AS PageRevisions
	    FROM PageRevision
	GROUP BY NamespaceID
	) AS Totals
JOIN Namespace AS NS ON NS.NamespaceID = TOTALS.NamespaceID
ORDER BY NS.NamespaceName;


-- Find PageRevisions that don't correspond to
-- a known page
SELECT * FROM PageRevision
 WHERE PageID NOT IN (SELECT PageID FROM [PAGE]);


-- What contributor IDs are credited with revisions,
-- but don't appear in the User table?
-- ContributorID == 0 is ignored.
SELECT * FROM PageRevision WHERE ContributorID NOT IN (SELECT UserID FROM [User]) AND ContributorID != 0;


-- number of pages per namespace
SELECT Namespace.NamespaceID, NamespaceName, COUNT(Namespace.NamespaceID)
  FROM [Page]
  JOIN Namespace ON Namespace.NamespaceID = [Page].NamespaceID
GROUP BY Namespace.NamespaceID, NamespaceName
ORDER BY 3 DESC;


-- Contributions by non-anonymous contributor
SELECT TOP 1000 UserID, UserName, Contributions
FROM
(
	  SELECT ContributorID, COUNT(ContributorID) AS Contributions
		FROM PageRevision
	   WHERE ContributorID IS NOT NULL
	GROUP BY ContributorID
) AS Tally
JOIN [User] AS U on U.UserID = Tally.ContributorID
ORDER BY 3 DESC;


-- Contributions by anonymous contributors
   SELECT IPAddress, COUNT(IPAddress) AS Contributions
	 FROM PageRevision
    WHERE ContributorID IS NULL
 GROUP BY IPAddress
 ORDER BY 2 DESC;


 -- most recent page reads
select *
 from Activity WHERE Activity = 'Read Page'
 ORDER BY ActivityID DESC;

-- logest page reads (From XML)
select *
 from Activity WHERE Activity = 'Read Page'
 ORDER BY DurationMillis DESC;

-- longest revision merges
select *
    from Activity WHERE Activity = 'Merge PageRevisions'
ORDER BY DurationMillis DESC;


-- contributors with the most versions
  SELECT TOP 10 ContributorID, [USer].UserName, COUNT(ContributorID) ContribCount
    FROM PageRevision
    JOIN [User] ON PageRevision.ContributorID = [User].UserID
   WHERE ContributorID != 0
GROUP BY ContributorID, [User].UserName
ORDER BY 3 DESC;



-- pages with the most revisions
SELECT NamespaceName, PageName, RevisionCount
FROM
(
SELECT TOP 100 NamespaceID, PageID, COUNT(PageRevisionID) RevisionCount
FROM PageRevision
GROUP BY NamespaceID, PageID
ORDER BY 3 DESC
) AS X
JOIN Namespace ON Namespace.NamespaceID = X.NamespaceID
JOIN Page ON Page.PageID = X.PageID AND Page.NamespaceID = X.NamespaceID
ORDER BY 3 DESC;




-- it's me! UserID = 327592
select * from [user] where username = 'Mikeblas';
select * from PageRevision WITH(NOLOCK) WHERE ContributorID = 327592 ORDER BY RevisionWhen DESC;

select ContributorID, NamespaceID, PageID, PageRevisionID from PageRevision WITH(NOLOCK) WHERE ContributorID = 327592 ORDER BY RevisionWhen DESC;
select NamespaceID, PageID, PageRevisionID from PageRevision WITH(NOLOCK) WHERE ContributorID = 327592 ORDER BY RevisionWhen DESC;

-- text copies per article
SELECT PageID, COUNT(PageID) AS TotalRevisions, SUM(CASE WHEN ArticleText IS NOT NULL THEN 1 ELSE 0 END) AS RevisionsWithText
    FROM PageRevision
GROUP BY PageID
ORDER BY TotalRevisions DESC;

 select * FROM PageRevision WHERE ArticleText IS NOT NULL

 SELECT TOP 10 *
    FROM PageRevision
   WHERE ArticleText IS NOT NULL
ORDER BY ArticleTextLength DESC;


select * from PageRevision WHERE UserDeleted = 1;

select * from PageRevision WHERE Comment IS NULL;


