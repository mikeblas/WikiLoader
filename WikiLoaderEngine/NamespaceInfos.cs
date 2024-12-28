namespace WikiLoaderEngine
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading;

    /// <summary>
    /// NamespaceInfos contains a collection of Namespace objects,
    /// and implements the Insertable interface so the list can be pushed
    /// to the database.
    ///
    /// Note that the class has a few members (counts, particularly) that
    /// aren't persisted; these are used to show statistics for a run of
    /// the loader.
    /// </summary>
    public class NamespaceInfos : IInsertable
    {
        /// <summary>
        /// map of namespace IDs (as integers) to NamespaceInfo objects.
        /// </summary>
        private readonly Dictionary<int, NamespaceInfo> namespaceMap;

        private string state = "idle";

        /// <summary>
        /// Initializes a new instance of the <see cref="NamespaceInfos"/> class.
        /// </summary>
        public NamespaceInfos()
        {
            this.namespaceMap = new Dictionary<int, NamespaceInfo>();
        }

        /// <summary>
        /// Gets the number of items in this collection.
        /// </summary>
        public int Count
        {
            get { return this.namespaceMap.Count; }
        }

        /// <summary>
        /// Gets a collection containing our values (only).
        /// </summary>
        public Dictionary<int, NamespaceInfo>.ValueCollection Values
        {
            get { return this.namespaceMap.Values; }
        }


        /// <summary>
        /// Find a NamespaceInfo object given the ID key.
        /// Returns null if we don't have it.
        /// </summary>
        /// <param name="key">NamespaceID to find.</param>
        /// <returns>NamespaceID, null if not found.</returns>
        public NamespaceInfo this[int key]
        {
            get { return this.namespaceMap[key]; }
        }

        /// <summary>
        /// Add a new NamespaceInfo to this collection.
        /// </summary>
        /// <param name="nsi">NamespaceInfo to be added to the collection.</param>
        public void Add(NamespaceInfo nsi)
        {
            this.namespaceMap.Add(nsi.ID, nsi);
        }

        /// <summary>
        /// Insertable implementation: Insert method.
        /// Takes a SqlConnection and inserts the collection into it.
        /// </summary>
        /// <param name="pump">DatabasePump that will receive objects to be inserted.</param>
        /// <param name="conn">SqlConnection to write into.</param>
        /// <param name="progress">IInsertableProgress that will receive notifications of parsing progress.</param>
        /// <param name="parserProgress">IXmlDumpParserProgress which will receive notifications of progress.</param>
        public void Insert(DatabasePump pump, SqlConnection conn, IInsertableProgress progress, IXmlDumpParserProgress parserProgress)
        {
            // count of rows actually inserted
            int inserts = 0;
            // count of rows we didn't insert because they were dupes
            int already = 0;

            this.state = "Inserting";

            long activityID = pump.StartActivity("Insert Namespaces", null, null, this.namespaceMap.Count);

            // command to push a new row into the Namespace table
            using var cmd = new SqlCommand("INSERT INTO [Namespace] (NamespaceID, NamespaceName) VALUES ( @ID, @Name );", conn);

            // go through the whole collection
            foreach (KeyValuePair<int, NamespaceInfo> kvp in this.namespaceMap)
            {
                // reset the parameters collection; bind the current item from the collection
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@ID", kvp.Key);
                cmd.Parameters.AddWithValue("@Name", kvp.Value.Name);

                // try to do the insert; tally if we're successful
                try
                {
                    cmd.ExecuteNonQuery();
                    inserts += 1;
                }
                catch (SqlException sex)
                {
                    // if the exception is a duplicate key, just tally that
                    if (sex.Number == 2601 || sex.Number == 2627)
                    {
                        // already know that namespace ID
                        already += 1;
                    }
                    else
                    {
                        // otherwise, something else went wrong
                        throw;
                    }
                }
            }

            // show our results
            Console.WriteLine($"Inserted {inserts} new namespaces; {already} already known");
            pump.CompleteActivity(activityID, inserts, null);

            // signal the next in the chain of waiters
            this.state = "completed";
        }

        string IInsertable.ObjectName
        {
            get { return "Namespaces Inserter"; }
        }

        string IInsertable.ObjectTarget
        {
            get { return "Namespaces list";  }
        }

        string IInsertable.ObjectState
        {
            get { return state; }
        }

        int IInsertable.RevisionCount
        {
            get { return this.namespaceMap.Count; }
        }

        int IInsertable.RemainingRevisionCount
        {
            get { return 0; }
        }
    }
}
