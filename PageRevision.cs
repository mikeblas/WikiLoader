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
        Int64 _parentRevisionId = 0;
        Int64 _revisionId = 0;
        DateTime _timestamp = DateTime.MinValue;
        User _contributor = null;
        String _comment = null;
        String _text = null;
        int _textLength = 0;
        bool _minor = false;
        bool _commentDeleted = false;
        bool _textDeleted = false;

        public PageRevision(Int64 parentRevisionId, Int64 revisionId, DateTime timestamp, User contributor, String comment, String text, bool minor)
        {
            _parentRevisionId = parentRevisionId;
            _revisionId = revisionId;
            _timestamp = timestamp;
            _contributor = contributor;
            _comment = comment;
            _text = text;
            _textLength = _text.Length;
            _minor = minor;
        }

        public Int64 revisionId
        {
            get { return _revisionId; }
        }

        public Int64 parentRevisionId
        {
            get { return _parentRevisionId; }
        }

        public User contributor
        {
            get { return _contributor; }
        }

        public DateTime timestamp
        {
            get { return _timestamp; }
        }

        public String comment
        {
            get { return _comment; }
            set { _comment = comment; }
        }

        public String text
        {
            get { return _text; }
            set
            {
                _text = value;
                if (value != null) _textLength = _text.Length;
            }
        }
    }

}
