namespace WikiReader
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a user; contains their name (as a string) and an ID integer.
    /// </summary>
    internal class User
    {
        private readonly string? userName = null;
        private readonly string? ipAddress = null;
        private readonly long userId = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="User"/> class for a given ID and name.
        /// </summary>
        /// <param name="userId">user ID integer.</param>
        /// <param name="userName">user name as a string.</param>
        public User(long userId, string userName)
        {
            this.userId = userId;
            this.userName = userName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="User"/> class for an anonymous user,
        /// known only by IP address.
        /// </summary>
        /// <param name="ipAddress">IP address, as a string, where the edit was logged.</param>
        public User(string ipAddress)
        {
            this.ipAddress = ipAddress;
        }

        /// <summary>
        /// Gets a value indicating whether this object represents an anonymous user or not.
        /// </summary>
        public bool IsAnonymous
        {
            get { return (this.userId == 0) && (this.userName == null); }
        }

        /// <summary>
        /// Gets the ID of this user; 0 if the user is anonymous.
        /// </summary>
        public long ID
        {
            get { return this.userId; }
        }

        /// <summary>
        /// Gets the name of this user, null if anonymous.
        /// </summary>
        public string? Name
        {
            get { return this.userName; }
        }

        /// <summary>
        /// Gets the IPAddress of this user, null if anonymous.
        /// </summary>
        public string? IPAddress
        {
            get { return this.ipAddress; }
        }
    }
}
