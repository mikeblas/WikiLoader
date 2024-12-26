namespace WikiLoaderEngine
{
    using System;
    using System.Collections.Generic;
    using System.Data;


    /// <summary>
    /// Implements IDataReader for PageRevisionText records.
    /// </summary>
    internal class PageRevisionTextDataReader : IDataReader
    {
        // list of PageRevisions this reader will supply
        private readonly List<PageRevision> revisions = new ();

        private readonly int namespaceID;
        private readonly long pageID;

        /// <summary>
        /// revision ID that we are currently reading.
        /// </summary>
        private int currentRevision = -1;

        /// <summary>
        /// Initializes a new instance of the <see cref="PageRevisionTextDataReader"/> class for the given
        /// revisions.
        /// </summary>
        /// <param name="namespaceID">integer with the namespaceID of this page.</param>
        /// <param name="pageID">integer representing the pageID for this page.</param>
        /// <param name="pages">Collection of PageRevision objects to insert. Only those with a non-null Text property will be inserted.</param>
        public PageRevisionTextDataReader(int namespaceID, long pageID, IEnumerable<PageRevision> pages)
        {
            this.namespaceID = namespaceID;
            this.pageID = pageID;
            foreach (PageRevision pr in pages)
            {
                // only consider pages that have text
                if (pr.Text != null)
                    revisions.Add(pr);
            }

        }

        /// <summary>
        /// Gets the number of revisions we contain.
        /// </summary>
        public int Count
        {
            get { return revisions.Count; }
        }

        int IDataReader.Depth
        {
            get { throw new NotImplementedException(); }
        }

        bool IDataReader.IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        int IDataReader.RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        int IDataRecord.FieldCount
        {
            get { return 4; }
        }


        object IDataRecord.this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        object IDataRecord.this[int i]
        {
            get { throw new NotImplementedException(); }
        }

        void IDataReader.Close()
        {
        }

        DataTable IDataReader.GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        bool IDataReader.NextResult()
        {
            throw new NotImplementedException();
        }

        bool IDataReader.Read()
        {
            if (currentRevision + 1 >= revisions.Count)
                return false;
            currentRevision += 1;
            return true;
        }

        void IDisposable.Dispose()
        {
            throw new NotImplementedException();
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
                return namespaceID;
            throw new NotImplementedException();
        }

        long IDataRecord.GetInt64(int i)
        {
            if (i == 2)
                return revisions[currentRevision].RevisionId;
            if (i == 1)
                return pageID;
            if (i == 0)
                return namespaceID;
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
                string? s = revisions[currentRevision].Text;
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
                string? s = revisions[currentRevision].Text;
                if (s == null)
                    throw new InvalidOperationException("null Getvalue");
                return s;
            }

            if (i == 2)
                return revisions[currentRevision].RevisionId;
            if (i == 1)
                return pageID;
            if (i == 0)
                return namespaceID;
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
    }
}
