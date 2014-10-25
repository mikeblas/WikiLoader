
CREATE TABLE [User]
(
	UserID BIGINT NOT NULL,
	UserName VARCHAR(80) NOT NULL,
);

CREATE UNIQUE CLUSTERED INDEX User_PK ON [User] (UserID);
CREATE UNIQUE INDEX User_AK ON [User] (UserName);

CREATE TABLE [Page]
(
	NamespaceID INT NOT NULL,
	PageID BIGINT NOT NULL,
	PageName VARCHAR(80) NOT NULL,	
);

CREATE UNIQUE CLUSTERED INDEX Page_PK ON [Page] (NamespaceID, PageID);
CREATE UNIQUE INDEX Page_AK ON [Page] (NamespaceID, PageName);

CREATE TABLE [PageRevision]
(
	PageID BIGINT NOT NULL,
	PageRevisionID BIGINT NOT NULL,
	RevisionWhen DATETIME NOT NULL,
	ContributorID BIGINT NOT NULL,
	Comment VARCHAR(80) NOT NULL,
	ArticleText TEXT,
	IsMinor BIT NOT NULL,
	ArticleTextLength INT NOT NULL,
);

CREATE UNIQUE CLUSTERED INDEX PageRevision_PK ON [PageRevision] (PageID, PageRevisionID);
CREATE INDEX PageRevision_AK ON [PageRevision] (RevisionWhen);
CREATE INDEX PageRevision_ByContributor ON [PageRevision] (ContributorID);


CREATE TABLE [Namespace]
(
	NamespaceID BIGINT NOT NULL,
	NamespaceName VARCHAR(80),
);

CREATE UNIQUE CLUSTERED INDEX Namespace_PK ON [Namespace] (NamespaceID);
CREATE UNIQUE INDEX Namespace_AK ON [Namespace] (NamespaceName);
