namespace WikiLoaderEngine
{
    using System;
    using System.Collections.Generic;
    using System.Data;

    internal class UserDataReader : IDataReader
    {
        // list of users this reader will supply
        private readonly List<User> contributors = new ();

        /// <summary>
        /// Index of the user to be next read from the IDataReader interface.
        /// </summary>
        private int currentUser = -1;


        /// <summary>
        /// Initializes a new instance of the <see cref="UserDataReader"/> class.
        ///
        /// This instance will reference the InsertedUserSet to track which users have already been
        /// inserted. It will build a set of users not yet inserted for itself from the passed list,
        /// and set itself up to supply those users from its IDataReader interface.
        /// </summary>
        /// <param name="insertedUserSet">Global set of users that have already been inserted. Accessed with locks.</param>
        /// <param name="pages">PageRevision objects which will provide the users to be inserted.</param>
        public UserDataReader(IDictionary<long, bool> insertedUserSet, IEnumerable<PageRevision> pages)
        {
            foreach (PageRevision pr in pages)
            {
                if (pr.Contributor == null)
                    continue;

                if (!pr.Contributor.IsAnonymous)
                {
                    if (insertedUserSet.TryAdd(pr.Contributor.ID, true))
                    {
                        this.contributors.Add(pr.Contributor);
                    }
                }
            }
        }

        /*
        public UserDataReader(HashSet<long> insertedUserSet, IEnumerable<PageRevision> pages)
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
                    if (pr.Contributor == null)
                        continue;

                    // if we're not anonymous and we've already seen this ID, then skip
                    if (!pr.Contributor.IsAnonymous && insertedUserSet.Contains(pr.Contributor.ID))
                        continue;
                }

                // if we're not anonymous, add this user to our list
                if (!pr.Contributor.IsAnonymous)
                {
                    this.contributors.Add(pr.Contributor);
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
        */

            /// <summary>
            /// Gets the count of users to actually be inserted by this object.
            /// </summary>
        public int Count
        {
            get { return this.contributors.Count; }
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
            if (this.currentUser + 1 >= this.contributors.Count)
                return false;
            this.currentUser += 1;
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
                return this.contributors[this.currentUser].ID;
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
            string? s = this.contributors[this.currentUser].Name;
            if (s == null)
                throw new InvalidOperationException("null GetString");
            return s;
        }

        object IDataRecord.GetValue(int i)
        {
            if (i == 1)
            {
                string? s = this.contributors[this.currentUser].Name;
                if (s == null)
                    throw new InvalidOperationException("Null name");
                return s;
            }

            if (i == 0)
                return this.contributors[this.currentUser].ID;

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
