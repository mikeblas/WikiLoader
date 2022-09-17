namespace WikiLoader
{
    using System;
    using System.Collections.Generic;
    using System.Data;

    internal class PageRevisionTextDataReader : IDataReader
    {
        // list of PageRevisions this reader will supply
        readonly List<PageRevision> _revisions = new ();

        /// <summary>
        /// What revision would be next read?
        /// </summary>
        int _currentRevision = -1;
        readonly int _namespaceID;
        readonly long _pageID;

        public PageRevisionTextDataReader(int namespaceID, long pageID, IList<PageRevision> pages)
        {
            _namespaceID = namespaceID;
            _pageID = pageID;
            foreach (PageRevision pr in pages)
            {
                // only consider pages that have text
                if (pr.Text != null)
                    _revisions.Add(pr);
            }

        }

        public int Count
        {
            get { return _revisions.Count; }
        }

        void IDataReader.Close()
        {
        }

        int IDataReader.Depth
        {
            get { throw new NotImplementedException(); }
        }

        DataTable IDataReader.GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        bool IDataReader.IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        bool IDataReader.NextResult()
        {
            throw new NotImplementedException();
        }

        bool IDataReader.Read()
        {
            if (_currentRevision + 1 >= _revisions.Count)
                return false;
            _currentRevision += 1;
            return true;
        }

        int IDataReader.RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        void IDisposable.Dispose()
        {
            throw new NotImplementedException();
        }

        int IDataRecord.FieldCount
        {
            get { return 4; }
        }

        bool IDataRecord.GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        byte IDataRecord.GetByte(int i)
        {
            throw new NotImplementedException();
        }

        long IDataRecord.GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        char IDataRecord.GetChar(int i)
        {
            throw new NotImplementedException();
        }

        long IDataRecord.GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        IDataReader IDataRecord.GetData(int i)
        {
            throw new NotImplementedException();
        }

        string IDataRecord.GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        DateTime IDataRecord.GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        decimal IDataRecord.GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        double IDataRecord.GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        Type IDataRecord.GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        float IDataRecord.GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        Guid IDataRecord.GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        short IDataRecord.GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        int IDataRecord.GetInt32(int i)
        {
            if (i == 0)
                return _namespaceID;
            throw new NotImplementedException();
        }

        long IDataRecord.GetInt64(int i)
        {
            if (i == 2)
                return _revisions[_currentRevision].RevisionId;
            if (i == 1)
                return _pageID;
            if (i == 0)
                return _namespaceID;
            throw new NotImplementedException();
        }

        string IDataRecord.GetName(int i)
        {
            throw new NotImplementedException();
        }

        int IDataRecord.GetOrdinal(string name)
        {
            return name switch
            {
                "NamespaceID" => 0,
                "PageID" => 1,
                "PageRevisionID" => 2,
                "ArticleText" => 3,
                _ => throw new NotImplementedException(),
            };
        }

        string IDataRecord.GetString(int i)
        {
            if (i == 3)
            {
                string? s = _revisions[_currentRevision].Text;
                if (s == null)
                    throw new InvalidOperationException("null GetString");
                return s;
            }

            throw new NotImplementedException();
        }

        object IDataRecord.GetValue(int i)
        {
            if (i == 3)
            {
                string? s = _revisions[_currentRevision].Text;
                if (s == null)
                    throw new InvalidOperationException("null Getvalue");
                return s;
            }

            if (i == 2)
                return _revisions[_currentRevision].RevisionId;
            if (i == 1)
                return _pageID;
            if (i == 0)
                return _namespaceID;
            throw new NotImplementedException();
        }

        int IDataRecord.GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        bool IDataRecord.IsDBNull(int i)
        {
            return false;
        }

        object IDataRecord.this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        object IDataRecord.this[int i]
        {
            get { throw new NotImplementedException(); }
        }
    }
}
