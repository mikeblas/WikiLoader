namespace WikiLoaderEngine
{
    using System;
    using System.Collections.Generic;
    using System.Data;

    internal class PageRevisionDataReader : IDataReader
    {
        // list of PageRevisions this reader will supply
        private readonly List<PageRevision> revisions = new ();

        /// <summary>
        /// What revision would be next read?
        /// </summary>
        private readonly int namespaceID;
        private readonly long pageID;

        private readonly Dictionary<string, int> columnMap = new ();
        private readonly Dictionary<int, string> indexMap = new ();

        private int _currentRevision = -1;

        public PageRevisionDataReader(int namespaceID, long pageID, IList<PageRevision> pages)
        {
            this.namespaceID = namespaceID;
            this.pageID = pageID;
            foreach (PageRevision pr in pages)
            {
                this.revisions.Add(pr);
            }

            this.AddColumn("NamespaceID");
            this.AddColumn("PageID");
            this.AddColumn("PageRevisionID");
            this.AddColumn("ParentPageRevisionID");
            this.AddColumn("RevisionWhen");
            this.AddColumn("ContributorID");
            this.AddColumn("Comment");
            this.AddColumn("TextAvailable");
            this.AddColumn("IsMinor");
            this.AddColumn("ArticleTextLength");
            this.AddColumn("UserDeleted");
            this.AddColumn("TextDeleted");
            this.AddColumn("IPAddress");
        }

        private void AddColumn(string columnName)
        {
            int index = this.columnMap.Count;
            this.columnMap.Add(columnName, index);
            this.indexMap.Add(index, columnName);
        }

        public int Count
        {
            get { return this.revisions.Count; }
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
            if (this._currentRevision + 1 >= this.revisions.Count)
                return false;
            this._currentRevision += 1;
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
            get { return this.columnMap.Count; }
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
            throw new NotImplementedException();
        }

        long IDataRecord.GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        string IDataRecord.GetName(int i)
        {
            if (this.indexMap.TryGetValue(i, out string? name))
            {
                return name;
            }

            throw new NotImplementedException();
        }

        int IDataRecord.GetOrdinal(string name)
        {
            if (this.columnMap.TryGetValue(name, out int index))
                return index;

            Console.WriteLine($"Couldn't find {name}");
            throw new NotImplementedException();
        }

        string IDataRecord.GetString(int i)
        {
            throw new NotImplementedException();
        }

        object IDataRecord.GetValue(int i)
        {
            if (!indexMap.TryGetValue(i, out string? columnName))
                throw new NotImplementedException();

            string columnNameLower = columnName.ToLower();

            PageRevision record = this.revisions[this._currentRevision];

            switch (columnNameLower)
            {
                case "namespaceid":
                    return this.namespaceID;

                case "pageid":
                    return this.pageID;

                case "isminor":
                    return record.IsMinor;

                case "articletextlength":
                    return record.TextLength;

                case "textavailable":
                    return record.Text != null;

                case "comment":
                    if (record.Comment == null)
                        return DBNull.Value;
                    else
                        return record.Comment;

                case "pagerevisionid":
                    return record.RevisionId;

                case "parentpagerevisionid":
                    return record.ParentRevisionId;

                case "revisionwhen":
                    return record.TimeStamp;

                case "textdeleted":
                    return record.TextDeleted;

                case "contributorid":
                    {
                        if (record.Contributor == null)
                        {
                            return DBNull.Value;
                        }
                        else
                        {
                            if (record.Contributor.IsAnonymous)
                                return DBNull.Value;
                            else
                                return record.Contributor.ID;
                        }
                    }

                case "userdeleted":
                    if (record.Contributor != null)
                        return false;
                    else
                        return true;

                case "ipaddress":
                    if (record.Contributor == null)
                        return DBNull.Value;
                    else
                    {
                        if (record.Contributor.IsAnonymous)
                        {
                            if (record.Contributor.IPAddress == null)
                                throw new InvalidOperationException("IPAddress must be set if IsAnonymous");
                            return record.Contributor.IPAddress;
                        }
                        else
                            return DBNull.Value;
                    }

                default:
                    Console.WriteLine("Need more cases! {i}, {columnName}");
                    break;
            }

            throw new InvalidOperationException($"unknown name {columnName}");
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
