namespace WikiLoaderEngine
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Represense a namespace.
    ///
    /// Includes the name and ID of the namespace. Also manages a count
    /// which is used to tally the number of articles read per namespace.
    /// </summary>
    public class NamespaceInfo
    {
        private readonly int namespaceID;
        private readonly string name;
        private int pageCount;

        /// <summary>
        /// Create a new namespace instance.
        /// pageCount is initalized to one.
        ///
        /// </summary>
        /// <param name="name">Name of this namespace.</param>
        /// <param name="namespaceId">ID for this namespace.</param>
        public NamespaceInfo(string name, int namespaceId)
        {
            this.name = name;
            this.pageCount = 1;
            this.namespaceID = namespaceId;
        }

        /// <summary>
        /// Gets the ID of this namespace.
        /// </summary>
        public int ID
        {
            get { return this.namespaceID; }
        }

        /// <summary>
        /// Gets the naem of this Namespace.
        /// </summary>
        public string Name
        {
            get { return this.name; }
        }

        /// <summary>
        /// Gets the number of pages we have in this namespace.
        /// </summary>
        public int PageCount
        {
            get { return this.pageCount; }
        }

        /// <summary>
        /// Increments the count of pages we have in this namespace by one.
        /// </summary>
        public void IncrementCount()
        {
            this.pageCount += 1;
        }
    }
}
