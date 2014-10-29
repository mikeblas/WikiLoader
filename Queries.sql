

-- all the namespaces
select * from Namespace;


select * from [User]

select * from PageRevision

SET STATISTICS IO ON;
-- number of users, pages, revisions
select count(*) AS CountUsers from [user];
SELECT COUNT(*) AS CountRevisions FROM PageRevision;
SELECT COUNT(*) AS CountPages FROM Page;


-- number of revisions per page
SELECT Total.PageID, PageName, PageRevisionCount
 FROM 
( SELECT PageID, Count(PageRevisionID) AS PageRevisionCount
  FROM PageRevision
  GROUP BY PageID) AS Total
JOIN Page ON Page.PageID = Total.PageID
  ORDER BY 3 DESC;


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

-- Contributions by contributor
SELECT ContributorID, COUNT(1) AS Contributions
  FROM PageRevision
GROUP BY ContributorID
ORDER BY 2 DESC;


select * from PageRevision WHERE ContributorID IS NULL AND IPAddress IS NULL;

select * from PageRevision WHERE ContributorID = 0 AND IPAddress IS null;

-- it's me! UserID = 327592
select * from [user] where username = 'Mikeblas';
select * from PageRevision WHERE ContributorID = 327592;

 select * FROM PageRevision WHERE ArticleText IS NOT NULL

 SELECT TOP 10 *
    FROM PageRevision
   WHERE ArticleText IS NOT NULL
ORDER BY ArticleTextLength DESC;

SELECT * FROM PageRevision WHERE ContributorID = 0 AND IPAddress = 0;

select * from PageRevision WHERE UserDeleted = 1;

select * from PageRevision WHERE Comment IS NULL;
