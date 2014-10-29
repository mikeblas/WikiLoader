use Wikipedia;

CREATE TABLE [User]
(
	UserID BIGINT NOT NULL,
	UserName NVARCHAR(80) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL,
);

-- SQL_Latin1_General_CP1_CI_AS
-- SQL_Latin1_General_CP1_CS_AS

CREATE UNIQUE CLUSTERED INDEX User_PK ON [User] (UserID);
CREATE /* UNIQUE */ INDEX User_AK ON [User] (UserName);

CREATE TABLE [Page]
(
	NamespaceID INT NOT NULL,
	PageID BIGINT NOT NULL,
	PageName NVARCHAR(255) COLLATE SQL_Latin1_General_CP1_CS_AS  NOT NULL,	
);

CREATE UNIQUE CLUSTERED INDEX Page_PK ON [Page] (NamespaceID, PageID);
CREATE /* UNIQUE */ INDEX Page_AK ON [Page] (NamespaceID, PageName);

CREATE TABLE [PageRevision]
(
	NamespaceID INT NOT NULL,
	PageID BIGINT NOT NULL,
	PageRevisionID BIGINT NOT NULL,
	ParentPageRevisionID BIGINT NOT NULL,
	RevisionWhen DATETIME NOT NULL,
	ContributorID BIGINT,
	IPAddress VARCHAR(39),
	Comment NVARCHAR(255),
	ArticleText TEXT,
	IsMinor BIT NOT NULL,
	ArticleTextLength INT NOT NULL,
	TextDeleted BIT NOT NULL,
	UserDeleted BIT NOT NULL,
);

CREATE UNIQUE CLUSTERED INDEX PageRevision_PK ON [PageRevision] (NamespaceID, PageID, PageRevisionID);
CREATE INDEX PageRevision_AK ON [PageRevision] (RevisionWhen);
CREATE INDEX PageRevision_ByContributor ON [PageRevision] (ContributorID);


CREATE TABLE [Namespace]
(
	NamespaceID BIGINT NOT NULL,
	NamespaceName NVARCHAR(80),
);

CREATE UNIQUE CLUSTERED INDEX Namespace_PK ON [Namespace] (NamespaceID);
CREATE UNIQUE INDEX Namespace_AK ON [Namespace] (NamespaceName);
