
-- the User table tracks registered users.
CREATE TABLE [dbo].[User](
	[UserID] [bigint] NOT NULL,
	[UserName] [nvarchar](80) NOT NULL,
 CONSTRAINT [User_PK] PRIMARY KEY CLUSTERED 
(
	[UserID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE /* UNIQUE */ INDEX User_AK ON [User] (UserName) ON [PRIMARY];

-- SQL_Latin1_General_CP1_CI_AS
-- SQL_Latin1_General_CP1_CS_AS


-- a page is a particualr topic in a namespace. It has an ID ane a name,
-- and it might have a redirect. Note that names are case-sensitive.
CREATE TABLE [dbo].[Page](
	[NamespaceID] [int] NOT NULL,
	[PageID] [bigint] NOT NULL,
	[PageName] [nvarchar](265) NOT NULL,
	[RedirectTitle] [nvarchar](265) NULL,
 CONSTRAINT [Page_PK] PRIMARY KEY CLUSTERED 
(
	[NamespaceID] ASC,
	[PageID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Page]  WITH CHECK ADD  CONSTRAINT [FK_Page_Namespace] FOREIGN KEY([NamespaceID])
REFERENCES [dbo].[Namespace] ([NamespaceID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[Page] CHECK CONSTRAINT [FK_Page_Namespace]
GO

CREATE INDEX Page_AK2 ON [Page] (NamespaceID, PageName) ON [PRIMARY];


-- A PageRevision is a revision of a particular page, written by some certain user
-- and made available at a certain time. Users can anonymously edit, in which case
-- the IP address of the editor is recorded and ContributorID is NULL.
--
-- An edit can be "minor" or not, an artibrary flag set by the contributor at the
-- time their edits are saved.
--
-- A particular revision might be permanently deleted (for copyvio or harrasment,
-- for example) and might not have visible text. Users might be flagged as deleted
-- here, too.
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
	TextAvailable BIT NOT NULL,
	IsMinor BIT NOT NULL,
	ArticleTextLength INT NOT NULL,
	TextDeleted BIT NOT NULL,
	UserDeleted BIT NOT NULL,
);

CREATE UNIQUE CLUSTERED INDEX PageRevision_PK ON PageRevision(PageID, PageRevisionID) ON [PRIMARY];
CREATE INDEX PageRevision_Namespace ON [PageRevision] (NamespaceID, PageID) ON [SECONDARY];
CREATE INDEX PageRevision_Contributor ON [PageRevision] (ContributorID) ON [SECONDARY];
CREATE INDEX PageRevision_When2 ON [PageRevision] (RevisionWhen) ON [SPAN];

-- A namcepace is a grouping of pages. Normal pages are in the main namespace, and other
-- namespaces include Talk for conversations about a page, User for user vanity pages, and 
-- Template for a system of templates that can be invoked when rendering any page.
CREATE TABLE [dbo].[Namespace](
	[NamespaceID] [int] NOT NULL,
	[NamespaceName] [nvarchar](80) NULL,
 CONSTRAINT [Namespace_PK] PRIMARY KEY CLUSTERED 
(
	[NamespaceID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE UNIQUE INDEX Namespace_AK ON [Namespace] (NamespaceName);

-- A Run is an instance of any read-write application using this database. An entry in
-- Run must be made to identify application instances so they can log their work in the
-- Activity table.
CREATE TABLE [dbo].[Run](
	[RunID] [bigint] IDENTITY(1,1) NOT NULL,
	[HostName] [varchar](256) NOT NULL,
	[ProcID] [bigint] NOT NULL,
	[SourceFileName] [varchar](256) NOT NULL,
	[SourceFileSize] [bigint] NOT NULL,
	[SourceFileTimestamp] [datetime] NOT NULL,
	[StartTime] [datetime] NOT NULL,
	[EndTime] [datetime] NULL,
	[Result] [varchar](1024) NULL,
 CONSTRAINT [Run_PK] PRIMARY KEY CLUSTERED 
(
	[RunID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


-- The Activity table logs write actions taken by an application that works this database.
-- Actions are related to Runs.
CREATE TABLE [dbo].[Activity](
	[RunID] [bigint] NOT NULL,
	[ActivityID] [bigint] IDENTITY(1,1) NOT NULL,
	[ThreadID] [bigint] NOT NULL,
	[Activity] [varchar](40) NOT NULL,
	[StartTime] [datetime] NOT NULL,
	[TargetNamespace] [int] NULL,
	[TargetPageID] [bigint] NULL,
	[WorkCount] [bigint] NULL,
	[CompletedCount] [bigint] NULL,
	[EndTime] [datetime] NULL,
	[DurationMillis] [bigint] NULL,
	[Result] [varchar](1024) NULL,
 CONSTRAINT [Activity_PK] PRIMARY KEY CLUSTERED 
(
	[RunID] ASC,
	[ActivityID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE UNIQUE INDEX Activity_AK ON [Activity] (ActivityID);

ALTER TABLE [dbo].[Activity]  WITH CHECK ADD  CONSTRAINT [FK_Activity_Run] FOREIGN KEY([RunID])
REFERENCES [dbo].[Run] ([RunID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[Activity]  WITH CHECK ADD  CONSTRAINT [FK_Activity_Namespace] FOREIGN KEY([TargetNamespace])
REFERENCES [dbo].[Namespace] ([NamespaceID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

-- PageRevisionText holds actual texts of some articles. A row is expected
-- here if the TextAvailable column in the PageRevision table is not zero.
CREATE TABLE PageRevisionText (
	NamespaceID INT NOT NULL,
	PageID BIGINT NOT NULL,
	PageRevisionID BIGINT NOT NULL,
	ArticleText TEXT NOT NULL
) ON SECONDARY;
CREATE UNIQUE CLUSTERED INDEX PageRevisionText_PK ON PageRevisionText(NamespaceID, PageID, PageRevisionID) ON [SECONDARY];

