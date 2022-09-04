namespace WikiReader
{
    using System;

    /// <summary>
    /// Represents a revision to a Page.
    /// 
    /// https://www.mediawiki.org/wiki/Manual:Revision_table
    /// 
    /// </summary>
    class PageRevision
    {
        readonly long _parentRevisionId = 0;
        readonly long _revisionId = 0;
        readonly DateTime _timestamp = DateTime.MinValue;
        readonly User? _contributor = null;
        readonly string? _comment = null;
        readonly bool _minor = false;
        readonly bool _commentDeleted = false;
        readonly bool _textDeleted = false;

        int _textLength = 0;
        string? _text = null;

        public PageRevision(long parentRevisionId, long revisionId, DateTime timestamp, User? contributor, string? comment, string? text, bool minor)
        {
            _parentRevisionId = parentRevisionId;
            _revisionId = revisionId;
            _timestamp = timestamp;
            _contributor = contributor;
            _comment = comment;
            _text = text;
            if (null == _text)
            {
                _textLength = 0;
                _textDeleted = true;
            }
            else
            {
                _textLength = _text.Length;
                _textDeleted = false;
            }
            _minor = minor;
        }

        public Int64 RevisionId
        {
            get { return _revisionId; }
        }

        public Int64 ParentRevisionId
        {
            get { return _parentRevisionId; }
        }

        public User? Contributor
        {
            get { return _contributor; }
        }

        public DateTime TimeStamp
        {
            get { return _timestamp; }
        }

        public string? Comment
        {
            get { return _comment; }
            // set { _comment = value; }
        }

        public bool CommentDeleted
        {
            get { return _commentDeleted; }
        }

        public bool TextDeleted
        {
            get { return _textDeleted; }
        }

        public string? Text
        {
            get { return _text; }
            set
            {
                _text = value;
                if (_text != null)
                    _textLength = _text.Length;
                else
                    _textLength = 0;
            }
        }

        public int TextLength
        {
            get { return _textLength; }
        }

        public bool IsMinor
        {
            get { return _minor; }
        }
 
    }

}
