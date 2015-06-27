use Wikipedia;
SET STATISTICS IO ON;

CREATE TABLE [User]
(
	UserID BIGINT NOT NULL,
	UserName NVARCHAR(80) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL,
);

-- SQL_Latin1_General_CP1_CI_AS
-- SQL_Latin1_General_CP1_CS_AS

CREATE UNIQUE CLUSTERED INDEX User_PK ON [User] (UserID) ON PRIMARY;
CREATE /* UNIQUE */ INDEX User_AK ON [User] (UserName) ON PRIMARY;

CREATE TABLE [Page]
(
	NamespaceID INT NOT NULL,
	PageID BIGINT NOT NULL,
	PageName NVARCHAR(265) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL,	
	RedirectTitle NVARCHAR(265) COLLATE SQL_Latin1_General_CP1_CS_AS NULL,
);

CREATE UNIQUE CLUSTERED INDEX Page_PK ON [Page] (NamespaceID, PageID) ON PRIMARY;
CREATE UNIQUE INDEX Page_AK ON [Page] (NamespaceID, PageName) ON PRIMARY;
CREATE /* UNIQUE */ INDEX Page_AK2 ON [Page] (NamespaceID, PageName) ON PRIMARY;

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
	-- ArticleText TEXT,
	TextAvailable BIT NOT NULL,
	IsMinor BIT NOT NULL,
	ArticleTextLength INT NOT NULL,
	TextDeleted BIT NOT NULL,
	UserDeleted BIT NOT NULL,
);

CREATE UNIQUE CLUSTERED INDEX PageRevision_PK ON PageRevision(PageID, PageRevisionID) ON PRIMARY;
CREATE INDEX PageRevision_Namespace ON [PageRevision] (NamespaceID, PageID) ON SECONDARY;
CREATE INDEX PageRevision_Contributor ON [PageRevision] (ContributorID) ON SECONDARY;
CREATE INDEX PageRevision_When2 ON [PageRevision] (RevisionWhen) ON SPAN;

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


CREATE TABLE PageRevisionText (
	NamespaceID INT NOT NULL,
	PageID BIGINT NOT NULL,
	PageRevisionID BIGINT NOT NULL,
	ArticleText TEXT NOT NULL
) ON SECONDARY;
CREATE UNIQUE CLUSTERED INDEX PageRevisionText_PK ON PageRevisionText(NamespaceID, PageID, PageRevisionID) ON SECONDARY;

