use wikipedia;

select top 10 * from [User] where username IN ('MalnadachBot', 'mikeblas', 'AnomieBOT', 'WOSLinkerBot', 'WOSLinker') ;

-- looks like AFD
select pr.*, p.PageName
from pagerevision pr
join page p on p.namespaceID = pr.namespaceID AND p.PageID = pr.pageID
where contributorid = 327592
and comment like '%afd%'
AND p.namespaceID = 4;

-- select them with some formatting
select FORMAT(MinRevWhen, 'yyyy-MM-ddThh:mm:ss'),
'|| [[' + SUBSTRING(p.pageName, CHARINDEX('/', p.pageName) + 1, 99) + ']] || [[' + p.PageName + '| link]] || ||', 
FORMAT(MaxRevWhen, 'yyyy-MM-ddThh:mm:ss')
from pagerevision pr
join page p on p.namespaceID = pr.namespaceID AND p.PageID = pr.pageID
CROSS APPLY 
(
SELECT MIN(RevisionWhen) MinRevWhen, MAX(RevisionWhen) MaxRevWhen
  FROM PageRevision
 WHERE PageID = p.PageID
   AND ContributorID NOT IN (41830889, 7611264, 3138265, 34993826) -- skip robots
) AS Xss
where contributorid = 327592 -- it's me!
and comment like '%afd%'
AND p.namespaceID = 4
ORDER BY 1;



-- another way
SELECT '| ' + CONVERT(VARCHAR(20), RevLow, 126)  + ' || [' + PageName + '] || || || '+ CONVERT(VARCHAR(20), RevHigh, 126)
  FROM Page
  JOIN 
(
select PageID, MIN(RevisionWhen) AS RevLow, MAX(RevisionWhen) AS RevHigh
  from [PageRevision]
 where ContributorID = 327592
   and comment like '%afd%'
   and revisionwhen >= '2006-09-10' -- T08:08:53'
   and revisionwhen <= '2015-08-10' -- T04:20:00'
   and NamespaceID = 0
   GROUP BY PageID
) X 
ON Page.PageID = X.PageID
ORDER BY RevLow
;


-- trry to subselect discussion article
SELECT RevLow, PageName, CONVERT(VARCHAR(20), RevHigh, 126),
(
select TOP 1 P.PageName
from [Page] AS P
where P.PageName LIKE 'Wikipedia:Articles for deletion/' + Page.PageName + '%'
and P.namespaceID = 4
)

  FROM Page
  JOIN 
(
select PageID, MIN(RevisionWhen) AS RevLow, MAX(RevisionWhen) AS RevHigh
  from [PageRevision]
 where ContributorID = 327592
   and comment like '%afd%'
   and revisionwhen >= '2006-09-10' -- T08:08:53'
   and revisionwhen <= '2015-08-10' -- T04:20:00'
   and NamespaceID = 0
   GROUP BY PageID
) X 
ON Page.PageID = X.PageID
ORDER BY RevLow
;

