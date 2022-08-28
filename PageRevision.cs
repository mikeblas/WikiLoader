using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiReader
{
    /// <summary>
    /// Represents a revision to a Page.
    /// 
    /// https://www.mediawiki.org/wiki/Manual:Revision_table
    /// 
    /// </summary>
    class PageRevision
    {
        readonly Int64 _parentRevisionId = 0;
        readonly Int64 _revisionId = 0;
        readonly DateTime _timestamp = DateTime.MinValue;
        readonly User? _contributor = null;
        string? _comment = null;
        string? _text = null;
        int _textLength = 0;
        readonly bool _minor = false;
        readonly bool _commentDeleted = false;
        readonly bool _textDeleted = false;

        public PageRevision(Int64 parentRevisionId, Int64 revisionId, DateTime timestamp, User? contributor, string? comment, string? text, bool minor)
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
            set { _comment = value; }
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
                if (_text!= null) _textLength = _text.Length;
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
