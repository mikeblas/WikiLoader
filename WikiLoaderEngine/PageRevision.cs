namespace WikiLoaderEngine
{
    using System;

    /// <summary>
    /// Represents a revision to a Page.
    ///
    /// https://www.mediawiki.org/wiki/Manual:Revision_table
    ///
    /// </summary>
    internal class PageRevision
    {
        private readonly long parentRevisionId = 0;
        private readonly long revisionId = 0;
        private readonly DateTime timestamp = DateTime.MinValue;
        private readonly User? contributor = null;
        private readonly string? comment = null;
        private readonly bool minor = false;
        private readonly bool commentDeleted = false;
        private readonly bool textDeleted = false;

        private int textLength = 0;
        private string? text = null;

        public PageRevision(long parentRevisionId, long revisionId, DateTime timestamp, User? contributor, string? comment, string? text, bool minor)
        {
            this.parentRevisionId = parentRevisionId;
            this.revisionId = revisionId;
            this.timestamp = timestamp;
            this.contributor = contributor;
            this.comment = comment;
            this.text = text;
            if (this.text == null)
            {
                this.textLength = 0;
                this.textDeleted = true;
            }
            else
            {
                this.textLength = this.text.Length;
                this.textDeleted = false;
            }

            this.minor = minor;
        }

        public long RevisionId
        {
            get { return this.revisionId; }
        }

        public long ParentRevisionId
        {
            get { return this.parentRevisionId; }
        }

        public User? Contributor
        {
            get { return this.contributor; }
        }

        public DateTime TimeStamp
        {
            get { return this.timestamp; }
        }

        public string? Comment
        {
            get { return this.comment; }
            // set { comment = value; }
        }

        public bool CommentDeleted
        {
            get { return this.commentDeleted; }
        }

        public bool TextDeleted
        {
            get { return this.textDeleted; }
        }

        public string? Text
        {
            get
            {
                return this.text;
            }

            set
            {
                this.text = value;
                if (this.text != null)
                    this.textLength = this.text.Length;
                else
                    this.textLength = 0;
            }
        }

        public int TextLength
        {
            get { return this.textLength; }
        }

        public bool IsMinor
        {
            get { return this.minor; }
        }
    }
}
