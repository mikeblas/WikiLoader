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

CREATE UNIQUE CLUSTERED INDEX PageRevision_PK ON PageRevision(PageID, PageRevisionID);
CREATE INDEX PageRevision_AK2 ON PageRevision(NamespaceID, PageID);
CREATE INDEX PageRevision_AK ON [PageRevision] (RevisionWhen);
CREATE INDEX PageRevision_ByContributor ON [PageRevision] (ContributorID);

CREATE TABLE [Namespace]
(
	NamespaceID BIGINT NOT NULL,
	NamespaceName NVARCHAR(80),
);

CREATE UNIQUE CLUSTERED INDEX Namespace_PK ON [Namespace] (NamespaceID);
CREATE UNIQUE INDEX Namespace_AK ON [Namespace] (NamespaceName);

-- DROP TABLE Run;
CREATE TABLE [Run]
(
	RunID BIGINT IDENTITY(1,1) NOT NULL,
	HostName VARCHAR(256) NOT NULL,
	ProcID BIGINT NOT NULL,
	SourceFileName VARCHAR(256) NOT NULL,
	SourceFileSize BIGINT NOT NULL,
	SourceFileTimestamp DATETIME NOT NULL,
	StartTime DATETIME NOT NULL,
	EndTime DATETIME,
	Result VARCHAR(1024),
);

CREATE UNIQUE CLUSTERED INDEX RunID_PK ON [Run] (RunID);

-- DROP TABLE Activity;
CREATE TABLE [Activity]
(
	RunID BIGINT NOT NULL,
	ActivityID BIGINT IDENTITY(1,1) NOT NULL,
	ThreadID BIGINT NOT NULL,
	Activity VARCHAR(40) NOT NULL,
	StartTime DATETIME NOT NULL,
	TargetNamespace INT,
	TargetPageID BIGINT,
	WorkCount BIGINT,
	CompletedCount BIGINT,
	EndTime DATETIME,
	DurationMillis BIGINT,
	Result VARCHAR(1024)
);

CREATE UNIQUE CLUSTERED INDEX Activity_PK ON [Activity] (RunID, ActivityID);
CREATE UNIQUE INDEX ActivitY_AK ON [Activity] (ActivityID);

