using System;
using System.Collections.Generic;
using System.Data;

namespace WikiReader
{
    class PageRevisionDataReader : IDataReader
    {
        // list of PageRevisions this reader will supply
        readonly List<PageRevision> _revisions = new();

        /// <summary>
        /// What revision would be next read?
        /// </summary>
        int _currentRevision = -1;
        readonly int _namespaceID;
        readonly long _pageID;

        readonly Dictionary<string, int> _columnMap = new();
        readonly Dictionary<int, string> _indexMap = new();

        public PageRevisionDataReader(int namespaceID, long pageID, IList<PageRevision> pages)
        {
            _namespaceID = namespaceID;
            _pageID = pageID;
            foreach (PageRevision pr in pages)
            {
                _revisions.Add(pr);
            }

            AddColumn("NamespaceID");
            AddColumn("PageID");
            AddColumn("PageRevisionID");
            AddColumn("ParentPageRevisionID");
            AddColumn("RevisionWhen");
            AddColumn("ContributorID");
            AddColumn("Comment");
            AddColumn("TextAvailable");
            AddColumn("IsMinor");
            AddColumn("ArticleTextLength");
            AddColumn("UserDeleted");
            AddColumn("TextDeleted");
            AddColumn("IPAddress");
        }

        void AddColumn(string columnName)
        {
            int index = _columnMap.Count;
            _columnMap.Add(columnName, index);
            _indexMap.Add(index, columnName);
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
            get { return _columnMap.Count; }
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
            if (_indexMap.TryGetValue(i, out string? name))
            {
                return name;
            }
            throw new NotImplementedException();
        }

        int IDataRecord.GetOrdinal(string name)
        {
            if (_columnMap.TryGetValue(name, out int index))
                return index;

            System.Console.WriteLine($"Couldn't find {name}");
            throw new NotImplementedException();
        }

        string IDataRecord.GetString(int i)
        {
            throw new NotImplementedException();
        }

        object IDataRecord.GetValue(int i)
        {
            if (!_indexMap.TryGetValue(i, out string? columnName))
                throw new NotImplementedException();

            string columnNameLower = columnName.ToLower();

            PageRevision record = _revisions[_currentRevision]; 

            switch (columnNameLower)
            {
                case "namespaceid":
                    return _namespaceID;

                case "pageid":
                    return _pageID;

                case "isminor":
                    return record.IsMinor;

                case "articletextlength":
                    return record.TextLength;

                case "textavailable":
                    return (record.Text != null);

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
                    System.Console.WriteLine("Need more cases! {i}, {columnName}");
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
