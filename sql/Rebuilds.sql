
SET STATISTICS IO ON;

ALTER INDEX ALL ON [User] REBUILD WITH (ONLINE=ON); 
ALTER INDEX ALL ON [Page] REBUILD WITH (ONLINE = ON);

-- can't rebuild online because of error:
/*
Msg 2725, Level 16, State 2, Line 6
An online operation cannot be performed for index 'PageRevision_PK' because the index contains column 'ArticleText' of data type text, ntext, image or FILESTREAM.
For a non-clustered index, the column could be an include column of the index.
For a clustered index, the column could be any column of the table.
If DROP_EXISTING is used, the column could be part of a new or old index.
The operation must be performed offline.
*/
-- ALTER INDEX ALL ON [PageRevision] REBUILD WITH (ONLINE = ON);
ALTER INDEX ALL ON [PageRevision] REBUILD;

