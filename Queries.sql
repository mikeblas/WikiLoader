

select * from Namespace


select * from [User]

select * from PageRevision

select count(*) from [user];

select PageID, Count(PageRevisionID)
  FROM PageRevision
  GROUP BY PageID
  ORDER BY 2 DESC;
