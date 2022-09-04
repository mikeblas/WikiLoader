namespace WikiReader
{
    using System;
    using System.Collections.Generic;
    using System.Data;

    internal class UserDataReader : IDataReader
    {
        // list of users this reader will supply
        readonly List<User> _contributors = new();

        /// <summary>
        /// What user would be next read?
        /// </summary>
        int _currentUser = -1;

        public UserDataReader(HashSet<long> insertedUserSet, IList<PageRevision> pages)
        {
            long lastUserID = -1;
            foreach (PageRevision pr in pages)
            {
                lock (insertedUserSet)
                {
                    if (lastUserID != -1)
                        insertedUserSet.Add(lastUserID);

                    lastUserID = -1;

                    // if the contributor was deleted, skip it
                    if (null == pr.Contributor)
                        continue;

                    // if we're not anonymous and we've already seen this ID, then skip
                    if (false == pr.Contributor.IsAnonymous && insertedUserSet.Contains(pr.Contributor.ID))
                        continue;
                }

                // if we're anonymous, then skip
                if (true == pr.Contributor.IsAnonymous)
                    continue;

                // if we're not anonymous, add this user to our list
                if (false == pr.Contributor.IsAnonymous)
                {
                    _contributors.Add(pr.Contributor);
                    lastUserID = pr.Contributor.ID;
                }
            }

            if (lastUserID != -1)
            {
                lock (insertedUserSet)
                {
                    insertedUserSet.Add(lastUserID);
                }
            }
        }

        public int Count
        {
            get { return _contributors.Count; }
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
            if (_currentUser + 1 >= _contributors.Count)
                return false;
            _currentUser += 1;
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
            get { return 2; }
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
            if (i == 0)
                return _contributors[_currentUser].ID;
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
                "UserID" => 0,
                "UserName" => 1,
                _ => throw new NotImplementedException(),
            };
        }

        string IDataRecord.GetString(int i)
        {
            string? s = _contributors[_currentUser].Name;
            if (s == null)
                throw new InvalidOperationException("null GetString");
            return s;
        }

        object IDataRecord.GetValue(int i)
        {
            if (i == 1)
            {
                string? s = _contributors[_currentUser].Name;
                if (s == null)
                    throw new InvalidOperationException("Null name");
                return s;
            }
            if (i == 0)
                return _contributors[_currentUser].ID;
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

